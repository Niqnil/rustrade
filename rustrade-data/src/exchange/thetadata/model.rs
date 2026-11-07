//! Request selectors and the records shared across ThetaData's asset classes.

use super::{error::ThetaDataError, time};
use crate::{
    books::Level,
    subscription::{book::OrderBookL1, candle::Candle},
};
use chrono::{DateTime, NaiveDate, NaiveTime, TimeDelta, Utc};
use rust_decimal::Decimal;
use rustrade_instrument::instrument::{
    kind::option::{OptionExercise, OptionKind},
    market_data::kind::MarketDataOptionContract,
};
use serde_json::value::RawValue;
use smol_str::SmolStr;
use std::{borrow::Cow, collections::HashMap, str::FromStr};

/// The sampling of a history request: every event (`Tick`) or one row per interval.
///
/// For quotes, an interval row is the last quote at the interval's timestamp; for OHLC, a bar
/// opening at it. Intervals below one minute are accepted only for a single day of data, and the
/// client splits a longer range into one-day requests for them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Interval {
    Tick,
    Ms10,
    Ms100,
    Ms500,
    S1,
    S5,
    S10,
    S15,
    S30,
    M1,
    M5,
    M10,
    M15,
    M30,
    H1,
}

impl Interval {
    /// The request parameter.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Tick => "tick",
            Self::Ms10 => "10ms",
            Self::Ms100 => "100ms",
            Self::Ms500 => "500ms",
            Self::S1 => "1s",
            Self::S5 => "5s",
            Self::S10 => "10s",
            Self::S15 => "15s",
            Self::S30 => "30s",
            Self::M1 => "1m",
            Self::M5 => "5m",
            Self::M10 => "10m",
            Self::M15 => "15m",
            Self::M30 => "30m",
            Self::H1 => "1h",
        }
    }

    /// The interval's length, or `None` for `Tick`.
    pub fn duration(self) -> Option<TimeDelta> {
        let ms = match self {
            Self::Tick => return None,
            Self::Ms10 => 10,
            Self::Ms100 => 100,
            Self::Ms500 => 500,
            Self::S1 => 1_000,
            Self::S5 => 5_000,
            Self::S10 => 10_000,
            Self::S15 => 15_000,
            Self::S30 => 30_000,
            Self::M1 => 60_000,
            Self::M5 => 300_000,
            Self::M10 => 600_000,
            Self::M15 => 900_000,
            Self::M30 => 1_800_000,
            Self::H1 => 3_600_000,
        };
        Some(TimeDelta::milliseconds(ms))
    }

    /// Whether ThetaData serves this interval only one day per request: `Tick` and every interval
    /// below one minute.
    pub(crate) fn is_single_day(self) -> bool {
        self.duration()
            .is_none_or(|duration| duration < TimeDelta::minutes(1))
    }
}

/// Which expirations an option request covers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Expiration {
    /// Every expiration (`*`).
    All,
    /// One expiration date.
    On(NaiveDate),
}

impl Expiration {
    pub(crate) fn param(self) -> String {
        match self {
            Self::All => "*".to_owned(),
            Self::On(date) => time::format_date(date),
        }
    }
}

/// Which strikes an option request covers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Strike {
    /// Every strike (`*`).
    All,
    /// One strike, in dollars.
    At(Decimal),
}

impl Strike {
    pub(crate) fn param(self) -> String {
        match self {
            Self::All => "*".to_owned(),
            Self::At(strike) => strike.normalize().to_string(),
        }
    }
}

/// The contracts an option request covers: an underlying's root, and which expirations, strikes
/// and rights.
///
/// `max_dte` keeps only contracts at most that many full calendar days from expiry, and
/// `strike_range` only the `n` strikes either side of the underlying's price; ThetaData applies
/// both.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OptionSelection {
    pub root: SmolStr,
    pub expiration: Expiration,
    pub strike: Strike,
    /// `None` for both calls and puts.
    pub kind: Option<OptionKind>,
    pub max_dte: Option<u32>,
    pub strike_range: Option<u32>,
}

impl OptionSelection {
    /// Every contract on `root` with the given expirations: all strikes, calls and puts.
    pub fn new(root: impl Into<SmolStr>, expiration: Expiration) -> Self {
        Self {
            root: root.into(),
            expiration,
            strike: Strike::All,
            kind: None,
            max_dte: None,
            strike_range: None,
        }
    }

    /// One contract.
    pub fn contract(contract: &OptionContract) -> Self {
        Self {
            strike: Strike::At(contract.strike),
            kind: Some(contract.kind),
            ..Self::new(contract.root.clone(), Expiration::On(contract.expiration))
        }
    }

    pub fn with_strike(self, strike: Strike) -> Self {
        Self { strike, ..self }
    }

    pub fn with_kind(self, kind: OptionKind) -> Self {
        Self {
            kind: Some(kind),
            ..self
        }
    }

    pub fn with_max_dte(self, max_dte: u32) -> Self {
        Self {
            max_dte: Some(max_dte),
            ..self
        }
    }

    pub fn with_strike_range(self, strike_range: u32) -> Self {
        Self {
            strike_range: Some(strike_range),
            ..self
        }
    }

    pub(crate) fn params(&self) -> Vec<(&'static str, String)> {
        let mut params = vec![
            ("symbol", self.root.to_string()),
            ("expiration", self.expiration.param()),
            ("strike", self.strike.param()),
            (
                "right",
                match self.kind {
                    None => "both",
                    Some(OptionKind::Call) => "call",
                    Some(OptionKind::Put) => "put",
                }
                .to_owned(),
            ),
        ];
        if let Some(max_dte) = self.max_dte {
            params.push(("max_dte", max_dte.to_string()));
        }
        if let Some(strike_range) = self.strike_range {
            params.push(("strike_range", strike_range.to_string()));
        }
        params
    }
}

/// An option contract as ThetaData identifies it.
///
/// `root` is the option root ThetaData reports, which can differ from the underlying's own symbol
/// (`SPXW` for the weekly SPX options). The strike is exact, in dollars.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct OptionContract {
    pub root: SmolStr,
    pub expiration: NaiveDate,
    pub strike: Decimal,
    pub kind: OptionKind,
}

impl OptionContract {
    /// The contract as this crate's market-data option kind.
    ///
    /// ThetaData does not publish the exercise style, so the caller supplies it: US equity and ETF
    /// options are American, cash-settled index options such as SPX are European.
    ///
    /// The expiry is midnight UTC of the expiration date, the convention the crate's other option
    /// connectors follow, so the instant's UTC date is the expiration date.
    pub fn to_market_data_contract(&self, exercise: OptionExercise) -> MarketDataOptionContract {
        MarketDataOptionContract {
            kind: self.kind,
            exercise,
            expiry: self.expiration.and_time(NaiveTime::MIN).and_utc(),
            strike: self.strike,
        }
    }
}

/// The national best bid and offer, as ThetaData reports it.
///
/// Prices are exact. Exchange and condition codes are ThetaData's numeric codes, kept as reported
/// (exchange 0 is the composite; quote condition 0 is regular for stocks). A side with nothing
/// quoted on it is reported as a zero price and size, and kept here as such; [`Self::top_of_book`]
/// maps it to an absent level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Nbbo {
    pub bid: Decimal,
    pub bid_size: u64,
    pub bid_exchange: i32,
    pub bid_condition: i32,
    pub ask: Decimal,
    pub ask_size: u64,
    pub ask_exchange: i32,
    pub ask_condition: i32,
}

impl Nbbo {
    /// The quote as a top of book stamped `time`.
    ///
    /// A zero price maps to an absent level: a zero-priced level would assert someone trades at
    /// zero, and a mid-price averaged against it is wrong in a way nothing downstream can detect.
    /// A zero size at a real price is kept: the price is still a quote.
    pub fn top_of_book(&self, time: DateTime<Utc>) -> OrderBookL1 {
        let side = |price: Decimal, size: u64| {
            (!price.is_zero()).then(|| Level::new(price, Decimal::from(size)))
        };
        OrderBookL1 {
            last_update_time: time,
            best_bid: side(self.bid, self.bid_size),
            best_ask: side(self.ask, self.ask_size),
        }
    }
}

/// An NBBO quote at a time: one row of a stock quote history.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Quote {
    pub time: DateTime<Utc>,
    pub nbbo: Nbbo,
}

impl From<&Quote> for OrderBookL1 {
    fn from(quote: &Quote) -> Self {
        quote.nbbo.top_of_book(quote.time)
    }
}

/// An NBBO quote for one symbol: one row of a stock quote snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SymbolQuote {
    pub symbol: SmolStr,
    pub time: DateTime<Utc>,
    pub nbbo: Nbbo,
}

impl From<&SymbolQuote> for OrderBookL1 {
    fn from(quote: &SymbolQuote) -> Self {
        quote.nbbo.top_of_book(quote.time)
    }
}

/// ThetaData's end-of-day report for one day.
///
/// ThetaData generates it at 17:15 New York time from the day's trades and the last NBBO, since
/// the feeds publish no national end-of-day report of their own. `created` is when the report was
/// generated and `last_trade` the time of the day's last trade.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EndOfDay {
    pub created: DateTime<Utc>,
    pub last_trade: DateTime<Utc>,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: u64,
    pub trade_count: u64,
    /// The last NBBO of the day.
    pub closing_nbbo: Nbbo,
}

/// An OHLC report for one symbol: one row of a snapshot of the current day's OHLC.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OhlcSnapshot {
    pub symbol: SmolStr,
    pub time: DateTime<Utc>,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: u64,
    pub trade_count: u64,
}

/// One OHLC bar of an intraday history.
///
/// ThetaData stamps a bar with its opening time and builds it from the trades in
/// `open_time <= trade time < open_time + interval`, by the feeds' rules for which trades count.
/// [`Candle::close_time`] is `open_time + interval`. An interval with no trades is reported as a
/// bar of zero prices and kept as such.
#[derive(Debug, Clone, PartialEq)]
pub struct Bar {
    pub open_time: DateTime<Utc>,
    pub candle: Candle,
    /// The volume-weighted average price ThetaData reports with the bar.
    pub vwap: Option<Decimal>,
}

// ---------------------------------------------------------------------------------------------
// Row decoding
// ---------------------------------------------------------------------------------------------

/// One NDJSON row, its fields borrowed from the line: each value is kept as its raw JSON text
/// until a decoder asks for it as a type.
///
/// Numbers are read from that text, so a price parses into a `Decimal` exactly; read through `f64`
/// (serde_json's default), `0.1` and many prices would come back inexact. Rows of different
/// endpoints share groups of fields (the contract, the NBBO), which the methods below decode once
/// for all of them.
pub(crate) struct Row<'a> {
    line: &'a str,
    fields: HashMap<&'a str, &'a RawValue>,
}

impl<'a> Row<'a> {
    pub(crate) fn parse(line: &'a str) -> Result<Self, ThetaDataError> {
        let fields = serde_json::from_str(line).map_err(|e| ThetaDataError::decode(e, line))?;
        Ok(Self { line, fields })
    }

    pub(crate) fn error(&self, message: impl std::fmt::Display) -> ThetaDataError {
        ThetaDataError::decode(message, self.line)
    }

    fn raw(&self, name: &str) -> Result<&'a RawValue, ThetaDataError> {
        self.fields
            .get(name)
            .copied()
            .ok_or_else(|| self.error(format_args!("missing field `{name}`")))
    }

    fn is_null(&self, name: &str) -> bool {
        self.fields.get(name).is_none_or(|raw| raw.get() == "null")
    }

    pub(crate) fn str(&self, name: &str) -> Result<Cow<'a, str>, ThetaDataError> {
        serde_json::from_str(self.raw(name)?.get())
            .map_err(|e| self.error(format_args!("field `{name}`: {e}")))
    }

    pub(crate) fn symbol(&self, name: &str) -> Result<SmolStr, ThetaDataError> {
        self.str(name).map(SmolStr::new)
    }

    pub(crate) fn decimal(&self, name: &str) -> Result<Decimal, ThetaDataError> {
        let text = self.raw(name)?.get();
        Decimal::from_str(text)
            .or_else(|_| Decimal::from_scientific(text))
            .map_err(|e| self.error(format_args!("field `{name}` = {text}: {e}")))
    }

    pub(crate) fn decimal_opt(&self, name: &str) -> Result<Option<Decimal>, ThetaDataError> {
        if self.is_null(name) {
            Ok(None)
        } else {
            self.decimal(name).map(Some)
        }
    }

    pub(crate) fn u64(&self, name: &str) -> Result<u64, ThetaDataError> {
        let text = self.raw(name)?.get();
        text.parse()
            .map_err(|e| self.error(format_args!("field `{name}` = {text}: {e}")))
    }

    pub(crate) fn i32(&self, name: &str) -> Result<i32, ThetaDataError> {
        let text = self.raw(name)?.get();
        text.parse()
            .map_err(|e| self.error(format_args!("field `{name}` = {text}: {e}")))
    }

    pub(crate) fn timestamp(&self, name: &str) -> Result<DateTime<Utc>, ThetaDataError> {
        time::parse_timestamp(&self.str(name)?).map_err(|e| self.error(e))
    }

    pub(crate) fn date(&self, name: &str) -> Result<NaiveDate, ThetaDataError> {
        time::parse_date(&self.str(name)?).map_err(|e| self.error(e))
    }

    pub(crate) fn time_of_day_opt(&self, name: &str) -> Result<Option<NaiveTime>, ThetaDataError> {
        if self.is_null(name) {
            return Ok(None);
        }
        let text = self.str(name)?;
        if text.is_empty() {
            return Ok(None);
        }
        time::parse_time(&text).map(Some).map_err(|e| self.error(e))
    }

    pub(crate) fn contract(&self) -> Result<OptionContract, ThetaDataError> {
        Ok(OptionContract {
            root: self.symbol("symbol")?,
            expiration: self.date("expiration")?,
            strike: self.decimal("strike")?,
            kind: option_kind(&self.str("right")?).map_err(|e| self.error(e))?,
        })
    }

    pub(crate) fn nbbo(&self) -> Result<Nbbo, ThetaDataError> {
        Ok(Nbbo {
            bid: self.decimal("bid")?,
            bid_size: self.u64("bid_size")?,
            bid_exchange: self.i32("bid_exchange")?,
            bid_condition: self.i32("bid_condition")?,
            ask: self.decimal("ask")?,
            ask_size: self.u64("ask_size")?,
            ask_exchange: self.i32("ask_exchange")?,
            ask_condition: self.i32("ask_condition")?,
        })
    }

    pub(crate) fn quote(&self) -> Result<Quote, ThetaDataError> {
        Ok(Quote {
            time: self.timestamp("timestamp")?,
            nbbo: self.nbbo()?,
        })
    }

    pub(crate) fn symbol_quote(&self) -> Result<SymbolQuote, ThetaDataError> {
        Ok(SymbolQuote {
            symbol: self.symbol("symbol")?,
            time: self.timestamp("timestamp")?,
            nbbo: self.nbbo()?,
        })
    }

    pub(crate) fn end_of_day(&self) -> Result<EndOfDay, ThetaDataError> {
        Ok(EndOfDay {
            created: self.timestamp("created")?,
            last_trade: self.timestamp("last_trade")?,
            open: self.decimal("open")?,
            high: self.decimal("high")?,
            low: self.decimal("low")?,
            close: self.decimal("close")?,
            volume: self.u64("volume")?,
            trade_count: self.u64("count")?,
            closing_nbbo: self.nbbo()?,
        })
    }

    pub(crate) fn bar(&self, interval: TimeDelta) -> Result<Bar, ThetaDataError> {
        let open_time = self.timestamp("timestamp")?;
        let close_time = open_time
            .checked_add_signed(interval)
            .ok_or_else(|| self.error("bar close time out of range"))?;
        Ok(Bar {
            open_time,
            candle: Candle {
                close_time,
                open: self.decimal("open")?,
                high: self.decimal("high")?,
                low: self.decimal("low")?,
                close: self.decimal("close")?,
                volume: Some(Decimal::from(self.u64("volume")?)),
                trade_count: Some(self.u64("count")?),
            },
            vwap: self.decimal_opt("vwap")?,
        })
    }

    pub(crate) fn ohlc_snapshot(&self) -> Result<OhlcSnapshot, ThetaDataError> {
        Ok(OhlcSnapshot {
            symbol: self.symbol("symbol")?,
            time: self.timestamp("timestamp")?,
            open: self.decimal("open")?,
            high: self.decimal("high")?,
            low: self.decimal("low")?,
            close: self.decimal("close")?,
            volume: self.u64("volume")?,
            trade_count: self.u64("count")?,
        })
    }
}

pub(crate) fn option_kind(text: &str) -> Result<OptionKind, String> {
    if text.eq_ignore_ascii_case("call") || text.eq_ignore_ascii_case("c") {
        Ok(OptionKind::Call)
    } else if text.eq_ignore_ascii_case("put") || text.eq_ignore_ascii_case("p") {
        Ok(OptionKind::Put)
    } else {
        Err(format!("right {text:?} is neither call nor put"))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    const NBBO: &str = r#""bid_size":598,"bid_exchange":5,"bid":4.55,"bid_condition":50,"ask_size":424,"ask_exchange":9,"ask":4.70,"ask_condition":50"#;

    #[test]
    fn prices_keep_their_exact_decimal_text() {
        let row = Row::parse(
            r#"{"a":187.2984,"b":0.1,"c":1.5e-3,"d":"x","e":null,"f":7,"g":-1,"h":1.5}"#,
        )
        .unwrap();
        assert_eq!(row.decimal("a").unwrap(), dec!(187.2984));
        assert_eq!(row.decimal("b").unwrap(), dec!(0.1));
        assert_eq!(row.decimal("c").unwrap(), dec!(0.0015));
        assert!(row.decimal("d").is_err());
        assert_eq!(row.decimal_opt("e").unwrap(), None);
        assert_eq!(row.decimal_opt("missing").unwrap(), None);
        assert_eq!(row.decimal_opt("a").unwrap(), Some(dec!(187.2984)));
        assert_eq!(row.u64("f").unwrap(), 7);
        assert!(row.u64("g").is_err());
        assert!(row.u64("h").is_err());
        assert_eq!(row.i32("g").unwrap(), -1);
        assert_eq!(row.str("d").unwrap(), "x");
        assert!(row.str("a").is_err());
        assert!(row.decimal("missing").is_err());
        assert!(Row::parse("not json").is_err());
    }

    #[test]
    fn a_stock_quote_row_decodes() {
        let line = format!(r#"{{{NBBO},"timestamp":"2024-01-02T09:31:00"}}"#);
        let quote: Quote = Row::parse(&line).unwrap().quote().unwrap();
        assert_eq!(quote.time.to_rfc3339(), "2024-01-02T14:31:00+00:00");
        assert_eq!(
            quote.nbbo,
            Nbbo {
                bid: dec!(4.55),
                bid_size: 598,
                bid_exchange: 5,
                bid_condition: 50,
                ask: dec!(4.70),
                ask_size: 424,
                ask_exchange: 9,
                ask_condition: 50,
            }
        );
    }

    #[test]
    fn a_zero_side_is_kept_in_the_row_and_absent_from_the_book() {
        let line = r#"{"bid_size":0,"bid_exchange":42,"bid":0.00,"bid_condition":50,"ask_size":0,"ask_exchange":42,"ask":0.00,"ask_condition":50,"timestamp":"2024-11-04T09:30:00"}"#;
        let quote: Quote = Row::parse(line).unwrap().quote().unwrap();
        assert!(quote.nbbo.bid.is_zero() && quote.nbbo.ask.is_zero());
        let book = OrderBookL1::from(&quote);
        assert_eq!(book.last_update_time, quote.time);
        assert_eq!((book.best_bid, book.best_ask), (None, None));

        let one_sided = Nbbo {
            bid: dec!(1.25),
            bid_size: 0,
            ..quote.nbbo
        };
        let book = one_sided.top_of_book(quote.time);
        assert_eq!(book.best_bid, Some(Level::new(dec!(1.25), dec!(0))));
        assert_eq!(book.best_ask, None);
    }

    #[test]
    fn a_malformed_row_names_the_row() {
        let error = Row::parse(r#"{"timestamp":"2024-01-02T09:31:00"}"#)
            .unwrap()
            .quote()
            .unwrap_err();
        let ThetaDataError::Decode { row, .. } = error else {
            panic!("expected Decode, got {error:?}");
        };
        assert!(row.contains("2024-01-02"), "{row}");

        let line = format!(r#"{{{NBBO},"timestamp":"2024-03-10T02:30:00"}}"#);
        assert!(matches!(
            Row::parse(&line).unwrap().quote(),
            Err(ThetaDataError::Decode { .. })
        ));
    }

    #[test]
    fn contract_fields_decode_and_convert() {
        let contract = Row::parse(
            r#"{"symbol":"SPXW","expiration":"2024-11-08","strike":5725.000,"right":"PUT"}"#,
        )
        .unwrap()
        .contract()
        .unwrap();
        assert_eq!(
            contract,
            OptionContract {
                root: "SPXW".into(),
                expiration: NaiveDate::from_ymd_opt(2024, 11, 8).unwrap(),
                strike: dec!(5725),
                kind: OptionKind::Put,
            }
        );
        let market = contract.to_market_data_contract(OptionExercise::European);
        assert_eq!(market.expiry.to_rfc3339(), "2024-11-08T00:00:00+00:00");
        assert_eq!(market.strike, dec!(5725));
        assert_eq!(market.kind, OptionKind::Put);
        assert_eq!(market.exercise, OptionExercise::European);

        assert_eq!(option_kind("call").unwrap(), OptionKind::Call);
        assert_eq!(option_kind("C").unwrap(), OptionKind::Call);
        assert_eq!(option_kind("p").unwrap(), OptionKind::Put);
        assert!(option_kind("both").is_err());
    }

    #[test]
    fn selection_params_spell_every_filter() {
        let contract = OptionContract {
            root: "AAPL".into(),
            expiration: NaiveDate::from_ymd_opt(2024, 11, 8).unwrap(),
            strike: dec!(220.500),
            kind: OptionKind::Call,
        };
        assert_eq!(
            OptionSelection::contract(&contract).params(),
            vec![
                ("symbol", "AAPL".to_owned()),
                ("expiration", "20241108".to_owned()),
                ("strike", "220.5".to_owned()),
                ("right", "call".to_owned()),
            ]
        );
        let chain = OptionSelection::new("SPY", Expiration::All)
            .with_kind(OptionKind::Put)
            .with_max_dte(45)
            .with_strike_range(10);
        assert_eq!(
            chain.params(),
            vec![
                ("symbol", "SPY".to_owned()),
                ("expiration", "*".to_owned()),
                ("strike", "*".to_owned()),
                ("right", "put".to_owned()),
                ("max_dte", "45".to_owned()),
                ("strike_range", "10".to_owned()),
            ]
        );
        assert_eq!(
            OptionSelection::new("SPY", Expiration::All).params()[3].1,
            "both"
        );
    }

    #[test]
    fn intervals_spell_and_measure_themselves() {
        assert_eq!(Interval::M1.as_str(), "1m");
        assert_eq!(Interval::Tick.as_str(), "tick");
        assert_eq!(Interval::H1.duration(), Some(TimeDelta::hours(1)));
        assert_eq!(
            Interval::Ms500.duration(),
            Some(TimeDelta::milliseconds(500))
        );
        assert_eq!(Interval::Tick.duration(), None);
        assert!(Interval::Tick.is_single_day());
        assert!(Interval::S30.is_single_day());
        assert!(!Interval::M1.is_single_day());
        assert!(!Interval::H1.is_single_day());
    }
}
