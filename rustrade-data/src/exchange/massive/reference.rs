//! Reference data endpoints for Massive API.
//!
//! Provides access to ticker information, exchanges, market status, and holidays.

use super::error::MassiveError;
use super::rest::MassiveRestClient;
use async_stream::try_stream;
use chrono::{DateTime, NaiveDate, Utc};
use futures::Stream;
use rust_decimal::Decimal;
use serde::Deserialize;
use std::collections::HashMap;
use tracing::{debug, warn};

// ============================================================================
// Query Builders
// ============================================================================

/// Sort order for paginated results.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SortOrder {
    #[default]
    Asc,
    Desc,
}

impl SortOrder {
    fn as_str(self) -> &'static str {
        match self {
            SortOrder::Asc => "asc",
            SortOrder::Desc => "desc",
        }
    }
}

impl std::fmt::Display for SortOrder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Filter parameters for the `/v3/reference/tickers` endpoint.
///
/// All fields are optional. Construct with [`TickerQuery::new`] and chain
/// setters. The stream handles pagination automatically.
///
/// # Example
///
/// ```ignore
/// use rustrade_data::exchange::massive::{MassiveRestClient, TickerQuery};
///
/// let client = MassiveRestClient::from_env()?;
/// let query = TickerQuery::new()
///     .market("stocks")
///     .active(true)
///     .limit(500);
///
/// let mut stream = client.fetch_tickers(&query);
/// while let Some(ticker) = stream.next().await {
///     println!("{:?}", ticker?);
/// }
/// ```
#[derive(Debug, Default, Clone)]
pub struct TickerQuery {
    /// Filter by ticker symbol (exact match or prefix).
    pub ticker: Option<String>,
    /// Filter by asset type (CS, ETF, ADRC, etc.).
    pub asset_type: Option<String>,
    /// Filter by market (stocks, crypto, fx, otc, indices).
    pub market: Option<String>,
    /// Filter by primary exchange MIC code (XNYS, XNAS, etc.).
    pub exchange: Option<String>,
    /// Filter by active trading status.
    pub active: Option<bool>,
    /// Text search across ticker and company name.
    pub search: Option<String>,
    /// Results per page (max 1000).
    pub limit: Option<u16>,
    /// Sort direction.
    pub order: Option<SortOrder>,
    /// Field to sort by (e.g., "ticker", "name").
    pub sort: Option<String>,
    /// Ticker greater than or equal to.
    pub ticker_gte: Option<String>,
    /// Ticker greater than.
    pub ticker_gt: Option<String>,
    /// Ticker less than or equal to.
    pub ticker_lte: Option<String>,
    /// Ticker less than.
    pub ticker_lt: Option<String>,
}

impl TickerQuery {
    /// Create a new empty query (all tickers, default pagination).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Filter by exact ticker symbol or prefix.
    #[must_use]
    pub fn ticker(mut self, v: impl Into<String>) -> Self {
        self.ticker = Some(v.into());
        self
    }

    /// Filter by asset type (CS, ETF, ADRC, etc.).
    #[must_use]
    pub fn asset_type(mut self, v: impl Into<String>) -> Self {
        self.asset_type = Some(v.into());
        self
    }

    /// Filter by market (stocks, crypto, fx, otc, indices).
    #[must_use]
    pub fn market(mut self, v: impl Into<String>) -> Self {
        self.market = Some(v.into());
        self
    }

    /// Filter by primary exchange MIC code (XNYS, XNAS, etc.).
    #[must_use]
    pub fn exchange(mut self, v: impl Into<String>) -> Self {
        self.exchange = Some(v.into());
        self
    }

    /// Filter by active trading status.
    #[must_use]
    pub fn active(mut self, v: bool) -> Self {
        self.active = Some(v);
        self
    }

    /// Text search across ticker and company name.
    #[must_use]
    pub fn search(mut self, v: impl Into<String>) -> Self {
        self.search = Some(v.into());
        self
    }

    /// Set results per page (clamped to max 1000).
    #[must_use]
    pub fn limit(mut self, v: u16) -> Self {
        self.limit = Some(v.min(1000));
        self
    }

    /// Set sort direction.
    #[must_use]
    pub fn order(mut self, v: SortOrder) -> Self {
        self.order = Some(v);
        self
    }

    /// Set field to sort by.
    #[must_use]
    pub fn sort(mut self, v: impl Into<String>) -> Self {
        self.sort = Some(v.into());
        self
    }

    /// Filter tickers >= value.
    #[must_use]
    pub fn ticker_gte(mut self, v: impl Into<String>) -> Self {
        self.ticker_gte = Some(v.into());
        self
    }

    /// Filter tickers > value.
    #[must_use]
    pub fn ticker_gt(mut self, v: impl Into<String>) -> Self {
        self.ticker_gt = Some(v.into());
        self
    }

    /// Filter tickers <= value.
    #[must_use]
    pub fn ticker_lte(mut self, v: impl Into<String>) -> Self {
        self.ticker_lte = Some(v.into());
        self
    }

    /// Filter tickers < value.
    #[must_use]
    pub fn ticker_lt(mut self, v: impl Into<String>) -> Self {
        self.ticker_lt = Some(v.into());
        self
    }

    /// Build query string parameters.
    fn to_query_string(&self) -> String {
        let mut pairs: Vec<(&str, String)> = Vec::new();

        if let Some(ref v) = self.ticker {
            pairs.push(("ticker", v.clone()));
        }
        if let Some(ref v) = self.asset_type {
            pairs.push(("type", v.clone()));
        }
        if let Some(ref v) = self.market {
            pairs.push(("market", v.clone()));
        }
        if let Some(ref v) = self.exchange {
            pairs.push(("exchange", v.clone()));
        }
        if let Some(v) = self.active {
            pairs.push(("active", v.to_string()));
        }
        if let Some(ref v) = self.search {
            pairs.push(("search", v.clone()));
        }
        if let Some(v) = self.limit {
            pairs.push(("limit", v.to_string()));
        }
        if let Some(v) = self.order {
            pairs.push(("order", v.as_str().to_string()));
        }
        if let Some(ref v) = self.sort {
            pairs.push(("sort", v.clone()));
        }
        if let Some(ref v) = self.ticker_gte {
            pairs.push(("ticker.gte", v.clone()));
        }
        if let Some(ref v) = self.ticker_gt {
            pairs.push(("ticker.gt", v.clone()));
        }
        if let Some(ref v) = self.ticker_lte {
            pairs.push(("ticker.lte", v.clone()));
        }
        if let Some(ref v) = self.ticker_lt {
            pairs.push(("ticker.lt", v.clone()));
        }

        if pairs.is_empty() {
            String::new()
        } else {
            let encoded: Vec<String> = pairs
                .into_iter()
                .map(|(k, v)| format!("{}={}", k, urlencoding::encode(&v)))
                .collect();
            format!("?{}", encoded.join("&"))
        }
    }
}

// ============================================================================
// Response Types
// ============================================================================

/// Ticker summary from the tickers list endpoint.
#[derive(Debug, Clone, PartialEq)]
pub struct Ticker {
    /// Ticker symbol.
    pub ticker: String,
    /// Company or asset name.
    pub name: String,
    /// Market category (stocks, crypto, fx, etc.).
    pub market: String,
    /// Asset type (CS, ETF, CRYPTO, etc.).
    pub asset_type: String,
    /// Whether the ticker is actively traded.
    pub active: bool,
    /// Primary exchange MIC code.
    pub primary_exchange: Option<String>,
    /// Trading currency.
    pub currency_name: Option<String>,
    /// Composite FIGI identifier.
    pub composite_figi: Option<String>,
    /// Locale (us, global).
    pub locale: Option<String>,
    /// Last update timestamp.
    pub last_updated_utc: Option<DateTime<Utc>>,
}

/// Detailed ticker information from the ticker details endpoint.
#[derive(Debug, Clone, PartialEq)]
pub struct TickerDetails {
    /// Basic ticker info.
    pub ticker: Ticker,
    /// Company description.
    pub description: Option<String>,
    /// Company homepage URL.
    pub homepage_url: Option<String>,
    /// Total number of employees.
    pub total_employees: Option<u64>,
    /// Market capitalization.
    pub market_cap: Option<Decimal>,
    /// Phone number.
    pub phone_number: Option<String>,
    /// Company address.
    pub address: Option<Address>,
    /// SIC code.
    pub sic_code: Option<String>,
    /// SIC description.
    pub sic_description: Option<String>,
    /// Ticker root (e.g., AAPL for AAPL).
    pub ticker_root: Option<String>,
    /// IPO/listing date.
    pub list_date: Option<NaiveDate>,
    /// Delisting date (if delisted).
    pub delisted_utc: Option<DateTime<Utc>>,
    /// Shares outstanding.
    pub share_class_shares_outstanding: Option<u64>,
    /// Weighted shares outstanding.
    pub weighted_shares_outstanding: Option<u64>,
    /// Round lot size.
    pub round_lot: Option<u32>,
}

/// Company address.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Address {
    pub address1: Option<String>,
    pub city: Option<String>,
    pub state: Option<String>,
    pub postal_code: Option<String>,
}

/// Exchange information.
#[derive(Debug, Clone, PartialEq)]
pub struct Exchange {
    /// Internal exchange ID.
    pub id: i64,
    /// Exchange name.
    pub name: String,
    /// Exchange acronym (NYSE, NASDAQ, etc.).
    pub acronym: Option<String>,
    /// Market Identifier Code (ISO 10383).
    pub mic: Option<String>,
    /// Operating MIC.
    pub operating_mic: Option<String>,
    /// Asset class (stocks, crypto, fx, etc.).
    pub asset_class: String,
    /// Locale (us, global).
    pub locale: String,
    /// Exchange type.
    pub exchange_type: Option<String>,
    /// Exchange URL.
    pub url: Option<String>,
}

/// Current market status.
#[derive(Debug, Clone, PartialEq)]
pub struct MarketStatus {
    /// Overall market status.
    pub market: String,
    /// Server timestamp.
    pub server_time: DateTime<Utc>,
    /// Whether after-hours trading is active.
    pub after_hours: bool,
    /// Whether early/pre-market trading is active.
    pub early_hours: bool,
    /// Status of currency markets.
    pub currencies: CurrencyStatus,
    /// Status of individual exchanges.
    pub exchanges: HashMap<String, String>,
    /// Status of index groups.
    pub indices_groups: HashMap<String, String>,
}

/// Currency market status.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct CurrencyStatus {
    /// Crypto market status.
    pub crypto: String,
    /// Forex market status.
    pub fx: String,
}

/// Market holiday information.
#[derive(Debug, Clone, PartialEq)]
pub struct MarketHoliday {
    /// Holiday date.
    pub date: NaiveDate,
    /// Exchange code.
    pub exchange: String,
    /// Holiday name.
    pub name: String,
    /// Status: "closed" or "early-close".
    pub status: String,
    /// Trading open time (for early-close days).
    pub open: Option<DateTime<Utc>>,
    /// Trading close time (for early-close days).
    pub close: Option<DateTime<Utc>>,
}

/// Dividend payment frequency.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DividendFrequency {
    /// Once per year.
    Annual,
    /// Twice per year.
    SemiAnnual,
    /// Four times per year.
    Quarterly,
    /// Twelve times per year.
    Monthly,
    /// Frequency not specified or unrecognized.
    Unknown(u8),
}

impl DividendFrequency {
    /// Create from raw API value.
    #[must_use]
    pub fn from_raw(value: u8) -> Self {
        match value {
            1 => Self::Annual,
            2 => Self::SemiAnnual,
            4 => Self::Quarterly,
            12 => Self::Monthly,
            other => Self::Unknown(other),
        }
    }

    /// Convert to raw API value.
    #[must_use]
    pub fn to_raw(self) -> u8 {
        match self {
            Self::Annual => 1,
            Self::SemiAnnual => 2,
            Self::Quarterly => 4,
            Self::Monthly => 12,
            Self::Unknown(v) => v,
        }
    }
}

impl std::fmt::Display for DividendFrequency {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Annual => write!(f, "annual"),
            Self::SemiAnnual => write!(f, "semi-annual"),
            Self::Quarterly => write!(f, "quarterly"),
            Self::Monthly => write!(f, "monthly"),
            Self::Unknown(v) => write!(f, "unknown({v})"),
        }
    }
}

/// Dividend information.
#[derive(Debug, Clone, PartialEq)]
pub struct Dividend {
    /// Ticker symbol.
    pub ticker: String,
    /// Cash amount per share.
    pub cash_amount: Decimal,
    /// Currency of the dividend.
    pub currency: String,
    /// Declaration date.
    pub declaration_date: Option<NaiveDate>,
    /// Ex-dividend date (must hold shares before this date).
    pub ex_dividend_date: NaiveDate,
    /// Record date.
    pub record_date: Option<NaiveDate>,
    /// Payment date.
    pub pay_date: Option<NaiveDate>,
    /// Dividend frequency.
    pub frequency: Option<DividendFrequency>,
    /// Distribution type (CD=regular, SC=special, LT=long-term, ST=short-term).
    pub dividend_type: Option<String>,
}

/// Stock split information.
#[derive(Debug, Clone, PartialEq)]
pub struct StockSplit {
    /// Ticker symbol.
    pub ticker: String,
    /// Execution date of the split.
    pub execution_date: NaiveDate,
    /// Split ratio numerator (shares after split).
    pub split_to: Decimal,
    /// Split ratio denominator (shares before split).
    pub split_from: Decimal,
}

// ============================================================================
// Query Builders — Corporate Actions
// ============================================================================

/// Filter parameters for the `/v3/reference/dividends` endpoint.
#[derive(Debug, Default, Clone)]
pub struct DividendQuery {
    /// Filter by ticker symbol.
    pub ticker: Option<String>,
    /// Filter by ex-dividend date (exact).
    pub ex_dividend_date: Option<NaiveDate>,
    /// Filter by ex-dividend date >= value.
    pub ex_dividend_date_gte: Option<NaiveDate>,
    /// Filter by ex-dividend date <= value.
    pub ex_dividend_date_lte: Option<NaiveDate>,
    /// Filter by dividend type (CD, SC, LT, ST).
    pub dividend_type: Option<String>,
    /// Results per page (max 1000).
    pub limit: Option<u16>,
    /// Sort direction.
    pub order: Option<SortOrder>,
}

impl DividendQuery {
    /// Create a new empty query.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Filter by ticker symbol.
    #[must_use]
    pub fn ticker(mut self, v: impl Into<String>) -> Self {
        self.ticker = Some(v.into());
        self
    }

    /// Filter by exact ex-dividend date.
    #[must_use]
    pub fn ex_dividend_date(mut self, v: NaiveDate) -> Self {
        self.ex_dividend_date = Some(v);
        self
    }

    /// Filter by ex-dividend date >= value.
    #[must_use]
    pub fn ex_dividend_date_gte(mut self, v: NaiveDate) -> Self {
        self.ex_dividend_date_gte = Some(v);
        self
    }

    /// Filter by ex-dividend date <= value.
    #[must_use]
    pub fn ex_dividend_date_lte(mut self, v: NaiveDate) -> Self {
        self.ex_dividend_date_lte = Some(v);
        self
    }

    /// Filter by dividend type (CD=regular, SC=special, LT=long-term, ST=short-term).
    #[must_use]
    pub fn dividend_type(mut self, v: impl Into<String>) -> Self {
        self.dividend_type = Some(v.into());
        self
    }

    /// Set results per page (clamped to max 1000).
    #[must_use]
    pub fn limit(mut self, v: u16) -> Self {
        self.limit = Some(v.min(1000));
        self
    }

    /// Set sort direction.
    #[must_use]
    pub fn order(mut self, v: SortOrder) -> Self {
        self.order = Some(v);
        self
    }

    /// Validate the query for conflicting filters.
    ///
    /// Returns an error if both an exact `ex_dividend_date` and a range filter
    /// (`ex_dividend_date_gte` or `ex_dividend_date_lte`) are set. The API behavior
    /// when both are provided is undefined.
    pub fn validate(&self) -> Result<(), MassiveError> {
        let has_exact = self.ex_dividend_date.is_some();
        let has_range = self.ex_dividend_date_gte.is_some() || self.ex_dividend_date_lte.is_some();

        if has_exact && has_range {
            return Err(MassiveError::InvalidInput {
                message: "DividendQuery: cannot set both ex_dividend_date (exact) and \
                          ex_dividend_date_gte/lte (range) filters"
                    .into(),
            });
        }
        Ok(())
    }

    /// Build query string parameters.
    fn to_query_string(&self) -> String {
        let mut pairs: Vec<(&str, String)> = Vec::new();

        if let Some(ref v) = self.ticker {
            pairs.push(("ticker", v.clone()));
        }
        if let Some(v) = self.ex_dividend_date {
            pairs.push(("ex_dividend_date", v.to_string()));
        }
        if let Some(v) = self.ex_dividend_date_gte {
            pairs.push(("ex_dividend_date.gte", v.to_string()));
        }
        if let Some(v) = self.ex_dividend_date_lte {
            pairs.push(("ex_dividend_date.lte", v.to_string()));
        }
        if let Some(ref v) = self.dividend_type {
            pairs.push(("dividend_type", v.clone()));
        }
        if let Some(v) = self.limit {
            pairs.push(("limit", v.to_string()));
        }
        if let Some(v) = self.order {
            pairs.push(("order", v.as_str().to_string()));
        }

        if pairs.is_empty() {
            String::new()
        } else {
            let encoded: Vec<String> = pairs
                .into_iter()
                .map(|(k, v)| format!("{}={}", k, urlencoding::encode(&v)))
                .collect();
            format!("?{}", encoded.join("&"))
        }
    }
}

/// Filter parameters for the `/v3/reference/splits` endpoint.
#[derive(Debug, Default, Clone)]
pub struct SplitQuery {
    /// Filter by ticker symbol.
    pub ticker: Option<String>,
    /// Filter by execution date (exact).
    pub execution_date: Option<NaiveDate>,
    /// Filter by execution date >= value.
    pub execution_date_gte: Option<NaiveDate>,
    /// Filter by execution date <= value.
    pub execution_date_lte: Option<NaiveDate>,
    /// Results per page (max 1000).
    pub limit: Option<u16>,
    /// Sort direction.
    pub order: Option<SortOrder>,
}

impl SplitQuery {
    /// Create a new empty query.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Filter by ticker symbol.
    #[must_use]
    pub fn ticker(mut self, v: impl Into<String>) -> Self {
        self.ticker = Some(v.into());
        self
    }

    /// Filter by exact execution date.
    #[must_use]
    pub fn execution_date(mut self, v: NaiveDate) -> Self {
        self.execution_date = Some(v);
        self
    }

    /// Filter by execution date >= value.
    #[must_use]
    pub fn execution_date_gte(mut self, v: NaiveDate) -> Self {
        self.execution_date_gte = Some(v);
        self
    }

    /// Filter by execution date <= value.
    #[must_use]
    pub fn execution_date_lte(mut self, v: NaiveDate) -> Self {
        self.execution_date_lte = Some(v);
        self
    }

    /// Set results per page (clamped to max 1000).
    #[must_use]
    pub fn limit(mut self, v: u16) -> Self {
        self.limit = Some(v.min(1000));
        self
    }

    /// Set sort direction.
    #[must_use]
    pub fn order(mut self, v: SortOrder) -> Self {
        self.order = Some(v);
        self
    }

    /// Validate the query for conflicting filters.
    ///
    /// Returns an error if both an exact `execution_date` and a range filter
    /// (`execution_date_gte` or `execution_date_lte`) are set. The API behavior
    /// when both are provided is undefined.
    pub fn validate(&self) -> Result<(), MassiveError> {
        let has_exact = self.execution_date.is_some();
        let has_range = self.execution_date_gte.is_some() || self.execution_date_lte.is_some();

        if has_exact && has_range {
            return Err(MassiveError::InvalidInput {
                message: "SplitQuery: cannot set both execution_date (exact) and \
                          execution_date_gte/lte (range) filters"
                    .into(),
            });
        }
        Ok(())
    }

    /// Build query string parameters.
    fn to_query_string(&self) -> String {
        let mut pairs: Vec<(&str, String)> = Vec::new();

        if let Some(ref v) = self.ticker {
            pairs.push(("ticker", v.clone()));
        }
        if let Some(v) = self.execution_date {
            pairs.push(("execution_date", v.to_string()));
        }
        if let Some(v) = self.execution_date_gte {
            pairs.push(("execution_date.gte", v.to_string()));
        }
        if let Some(v) = self.execution_date_lte {
            pairs.push(("execution_date.lte", v.to_string()));
        }
        if let Some(v) = self.limit {
            pairs.push(("limit", v.to_string()));
        }
        if let Some(v) = self.order {
            pairs.push(("order", v.as_str().to_string()));
        }

        if pairs.is_empty() {
            String::new()
        } else {
            let encoded: Vec<String> = pairs
                .into_iter()
                .map(|(k, v)| format!("{}={}", k, urlencoding::encode(&v)))
                .collect();
            format!("?{}", encoded.join("&"))
        }
    }
}

// ============================================================================
// Internal Deserialization Types
// ============================================================================

#[derive(Deserialize)]
struct TickersResponse {
    results: Option<Vec<RawTicker>>,
    next_url: Option<String>,
    // Deserialized to satisfy serde; not surfaced in public type.
    #[allow(dead_code)]
    status: Option<String>,
    // Deserialized to satisfy serde; not surfaced in public type.
    #[allow(dead_code)]
    count: Option<u32>,
}

#[derive(Deserialize)]
struct RawTicker {
    ticker: String,
    name: Option<String>,
    market: Option<String>,
    #[serde(rename = "type")]
    asset_type: Option<String>,
    active: Option<bool>,
    primary_exchange: Option<String>,
    currency_name: Option<String>,
    composite_figi: Option<String>,
    locale: Option<String>,
    last_updated_utc: Option<String>,
}

impl RawTicker {
    fn into_ticker(self) -> Ticker {
        Ticker {
            ticker: self.ticker,
            name: self.name.unwrap_or_default(),
            market: self.market.unwrap_or_default(),
            asset_type: self.asset_type.unwrap_or_default(),
            active: self.active.unwrap_or(true),
            primary_exchange: self.primary_exchange,
            currency_name: self.currency_name,
            composite_figi: self.composite_figi,
            locale: self.locale,
            last_updated_utc: self.last_updated_utc.and_then(parse_rfc3339_or_warn),
        }
    }
}

/// Parse an RFC3339 timestamp, logging a warning and returning `None` on failure.
fn parse_rfc3339_or_warn(s: String) -> Option<DateTime<Utc>> {
    match DateTime::parse_from_rfc3339(&s) {
        Ok(dt) => Some(dt.with_timezone(&Utc)),
        Err(e) => {
            warn!(value = %s, error = %e, "Failed to parse RFC3339 timestamp from Massive API");
            None
        }
    }
}

#[derive(Deserialize)]
struct TickerDetailsResponse {
    results: Option<RawTickerDetails>,
    // Deserialized to satisfy serde; not surfaced in public type.
    #[allow(dead_code)]
    status: Option<String>,
}

#[derive(Deserialize)]
struct RawTickerDetails {
    ticker: String,
    name: Option<String>,
    market: Option<String>,
    #[serde(rename = "type")]
    asset_type: Option<String>,
    active: Option<bool>,
    primary_exchange: Option<String>,
    currency_name: Option<String>,
    composite_figi: Option<String>,
    locale: Option<String>,
    last_updated_utc: Option<String>,
    description: Option<String>,
    homepage_url: Option<String>,
    total_employees: Option<u64>,
    market_cap: Option<f64>,
    phone_number: Option<String>,
    address: Option<RawAddress>,
    sic_code: Option<String>,
    sic_description: Option<String>,
    ticker_root: Option<String>,
    list_date: Option<String>,
    delisted_utc: Option<String>,
    share_class_shares_outstanding: Option<u64>,
    weighted_shares_outstanding: Option<u64>,
    round_lot: Option<u32>,
}

#[derive(Deserialize)]
struct RawAddress {
    address1: Option<String>,
    city: Option<String>,
    state: Option<String>,
    postal_code: Option<String>,
}

impl RawTickerDetails {
    fn into_ticker_details(self) -> TickerDetails {
        let ticker = Ticker {
            ticker: self.ticker,
            name: self.name.unwrap_or_default(),
            market: self.market.unwrap_or_default(),
            asset_type: self.asset_type.unwrap_or_default(),
            active: self.active.unwrap_or(true),
            primary_exchange: self.primary_exchange,
            currency_name: self.currency_name,
            composite_figi: self.composite_figi,
            locale: self.locale,
            last_updated_utc: self.last_updated_utc.and_then(parse_rfc3339_or_warn),
        };

        TickerDetails {
            ticker,
            description: self.description,
            homepage_url: self.homepage_url,
            total_employees: self.total_employees,
            market_cap: self.market_cap.and_then(|v| {
                let d = Decimal::from_f64_retain(v);
                if d.is_none() {
                    warn!(value = %v, "Non-finite market_cap from Massive API; dropping");
                }
                d
            }),
            phone_number: self.phone_number,
            address: self.address.map(|a| Address {
                address1: a.address1,
                city: a.city,
                state: a.state,
                postal_code: a.postal_code,
            }),
            sic_code: self.sic_code,
            sic_description: self.sic_description,
            ticker_root: self.ticker_root,
            list_date: self.list_date.and_then(|s| match s.parse() {
                Ok(d) => Some(d),
                Err(e) => {
                    warn!(value = %s, error = %e, "Failed to parse list_date from Massive API");
                    None
                }
            }),
            delisted_utc: self.delisted_utc.and_then(parse_rfc3339_or_warn),
            share_class_shares_outstanding: self.share_class_shares_outstanding,
            weighted_shares_outstanding: self.weighted_shares_outstanding,
            round_lot: self.round_lot,
        }
    }
}

#[derive(Deserialize)]
struct ExchangesResponse {
    results: Option<Vec<RawExchange>>,
    // Deserialized to satisfy serde; not surfaced in public type.
    #[allow(dead_code)]
    status: Option<String>,
}

#[derive(Deserialize)]
struct RawExchange {
    id: i64,
    name: String,
    acronym: Option<String>,
    mic: Option<String>,
    operating_mic: Option<String>,
    asset_class: Option<String>,
    locale: Option<String>,
    #[serde(rename = "type")]
    exchange_type: Option<String>,
    url: Option<String>,
}

impl RawExchange {
    fn into_exchange(self) -> Exchange {
        Exchange {
            id: self.id,
            name: self.name,
            acronym: self.acronym,
            mic: self.mic,
            operating_mic: self.operating_mic,
            asset_class: self.asset_class.unwrap_or_default(),
            locale: self.locale.unwrap_or_default(),
            exchange_type: self.exchange_type,
            url: self.url,
        }
    }
}

#[derive(Deserialize)]
struct RawMarketStatus {
    market: Option<String>,
    #[serde(rename = "serverTime")]
    server_time: Option<String>,
    #[serde(rename = "afterHours")]
    after_hours: Option<bool>,
    #[serde(rename = "earlyHours")]
    early_hours: Option<bool>,
    currencies: Option<RawCurrencyStatus>,
    exchanges: Option<HashMap<String, String>>,
    #[serde(rename = "indicesGroups")]
    indices_groups: Option<HashMap<String, String>>,
}

#[derive(Deserialize)]
struct RawCurrencyStatus {
    crypto: Option<String>,
    fx: Option<String>,
}

impl RawMarketStatus {
    fn into_market_status(self) -> Result<MarketStatus, MassiveError> {
        let server_time = match self.server_time {
            Some(s) => DateTime::parse_from_rfc3339(&s)
                .map(|dt| dt.with_timezone(&Utc))
                .map_err(|e| MassiveError::Deserialize {
                    message: format!("invalid serverTime: {e}"),
                    payload: s,
                })?,
            None => {
                return Err(MassiveError::Deserialize {
                    message: "missing serverTime".into(),
                    payload: String::new(),
                });
            }
        };

        Ok(MarketStatus {
            market: self.market.unwrap_or_else(|| "unknown".into()),
            server_time,
            after_hours: self.after_hours.unwrap_or(false),
            early_hours: self.early_hours.unwrap_or(false),
            currencies: self
                .currencies
                .map(|c| CurrencyStatus {
                    crypto: c.crypto.unwrap_or_else(|| "unknown".into()),
                    fx: c.fx.unwrap_or_else(|| "unknown".into()),
                })
                .unwrap_or_default(),
            exchanges: self.exchanges.unwrap_or_default(),
            indices_groups: self.indices_groups.unwrap_or_default(),
        })
    }
}

#[derive(Deserialize)]
struct RawMarketHoliday {
    date: String,
    exchange: Option<String>,
    name: Option<String>,
    status: Option<String>,
    open: Option<String>,
    close: Option<String>,
}

impl RawMarketHoliday {
    fn into_market_holiday(self) -> Result<MarketHoliday, MassiveError> {
        let date = self.date.parse().map_err(|e: chrono::format::ParseError| {
            MassiveError::Deserialize {
                message: format!("invalid date format: {e}"),
                payload: self.date.clone(),
            }
        })?;

        Ok(MarketHoliday {
            date,
            exchange: self.exchange.unwrap_or_default(),
            name: self.name.unwrap_or_default(),
            status: self.status.unwrap_or_else(|| "closed".into()),
            open: self.open.and_then(parse_rfc3339_or_warn),
            close: self.close.and_then(parse_rfc3339_or_warn),
        })
    }
}

#[derive(Deserialize)]
struct DividendsResponse {
    results: Option<Vec<RawDividend>>,
    next_url: Option<String>,
    // Deserialized to satisfy serde; not surfaced in public type.
    #[allow(dead_code)]
    status: Option<String>,
}

#[derive(Deserialize)]
struct RawDividend {
    ticker: String,
    #[serde(with = "rust_decimal::serde::float")]
    cash_amount: Decimal,
    #[serde(default)]
    currency: Option<String>,
    declaration_date: Option<String>,
    ex_dividend_date: String,
    record_date: Option<String>,
    pay_date: Option<String>,
    frequency: Option<u8>,
    dividend_type: Option<String>,
}

impl RawDividend {
    fn into_dividend(self) -> Result<Dividend, MassiveError> {
        let ex_dividend_date =
            self.ex_dividend_date
                .parse()
                .map_err(|e: chrono::format::ParseError| MassiveError::Deserialize {
                    message: format!("invalid ex_dividend_date: {e}"),
                    payload: self.ex_dividend_date.clone(),
                })?;

        Ok(Dividend {
            ticker: self.ticker,
            cash_amount: self.cash_amount,
            currency: self.currency.unwrap_or_else(|| "USD".into()),
            declaration_date: self.declaration_date.and_then(|s| match s.parse() {
                Ok(d) => Some(d),
                Err(e) => {
                    warn!(value = %s, error = %e, "Failed to parse declaration_date");
                    None
                }
            }),
            ex_dividend_date,
            record_date: self.record_date.and_then(|s| match s.parse() {
                Ok(d) => Some(d),
                Err(e) => {
                    warn!(value = %s, error = %e, "Failed to parse record_date");
                    None
                }
            }),
            pay_date: self.pay_date.and_then(|s| match s.parse() {
                Ok(d) => Some(d),
                Err(e) => {
                    warn!(value = %s, error = %e, "Failed to parse pay_date");
                    None
                }
            }),
            frequency: self.frequency.map(DividendFrequency::from_raw),
            dividend_type: self.dividend_type,
        })
    }
}

#[derive(Deserialize)]
struct SplitsResponse {
    results: Option<Vec<RawSplit>>,
    next_url: Option<String>,
    // Deserialized to satisfy serde; not surfaced in public type.
    #[allow(dead_code)]
    status: Option<String>,
}

#[derive(Deserialize)]
struct RawSplit {
    ticker: String,
    execution_date: String,
    #[serde(with = "rust_decimal::serde::float")]
    split_to: Decimal,
    #[serde(with = "rust_decimal::serde::float")]
    split_from: Decimal,
}

impl RawSplit {
    fn into_stock_split(self) -> Result<StockSplit, MassiveError> {
        let execution_date =
            self.execution_date
                .parse()
                .map_err(|e: chrono::format::ParseError| MassiveError::Deserialize {
                    message: format!("invalid execution_date: {e}"),
                    payload: self.execution_date.clone(),
                })?;

        Ok(StockSplit {
            ticker: self.ticker,
            execution_date,
            split_to: self.split_to,
            split_from: self.split_from,
        })
    }
}

// ============================================================================
// Client Implementation
// ============================================================================

impl MassiveRestClient {
    /// Fetch all tickers matching the query.
    ///
    /// Returns a stream that handles pagination automatically.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let query = TickerQuery::new().market("stocks").active(true);
    /// let mut stream = client.fetch_tickers(&query);
    /// while let Some(ticker) = stream.next().await {
    ///     println!("{:?}", ticker?);
    /// }
    /// ```
    pub fn fetch_tickers<'a>(
        &'a self,
        query: &'a TickerQuery,
    ) -> impl Stream<Item = Result<Ticker, MassiveError>> + 'a {
        let base_url = self.base_url();
        try_stream! {
            let initial_url = format!(
                "{}/v3/reference/tickers{}",
                base_url,
                query.to_query_string()
            );

            let mut next_url: Option<String> = Some(initial_url);

            while let Some(url) = next_url.take() {
                debug!(url = %url, "Fetching tickers page");

                let body = self.fetch_page_body(&url).await?;
                let parsed: TickersResponse = serde_json::from_str(&body).map_err(|e| {
                    MassiveError::Deserialize {
                        message: e.to_string(),
                        payload: body,
                    }
                })?;

                if let Some(results) = parsed.results {
                    for raw in results {
                        yield raw.into_ticker();
                    }
                }

                if let Some(ref url) = parsed.next_url {
                    Self::validate_next_url(url, base_url)?;
                }
                next_url = parsed.next_url;
            }
        }
    }

    /// Fetch detailed information for a single ticker.
    ///
    /// # Arguments
    ///
    /// * `ticker` - Ticker symbol (e.g., "AAPL", "X:BTCUSD")
    ///
    /// # Example
    ///
    /// ```ignore
    /// let details = client.fetch_ticker_details("AAPL").await?;
    /// println!("Market cap: {:?}", details.market_cap);
    /// ```
    pub async fn fetch_ticker_details(&self, ticker: &str) -> Result<TickerDetails, MassiveError> {
        Self::validate_ticker(ticker)?;

        let url = format!("{}/v3/reference/tickers/{}", self.base_url(), ticker);
        debug!(url = %url, "Fetching ticker details");

        let body = self.fetch_page_body(&url).await?;
        let parsed: TickerDetailsResponse =
            serde_json::from_str(&body).map_err(|e| MassiveError::Deserialize {
                message: e.to_string(),
                payload: body,
            })?;

        parsed
            .results
            .map(|r| r.into_ticker_details())
            .ok_or_else(|| MassiveError::Api {
                status: 404,
                message: format!("Ticker not found: {}", ticker),
            })
    }

    /// Fetch all exchanges.
    ///
    /// # Arguments
    ///
    /// * `asset_class` - Optional filter by asset class (stocks, crypto, fx, etc.)
    /// * `locale` - Optional filter by locale (us, global)
    ///
    /// # Example
    ///
    /// ```ignore
    /// let exchanges = client.fetch_exchanges(Some("stocks"), Some("us")).await?;
    /// for exchange in exchanges {
    ///     println!("{}: {}", exchange.mic.unwrap_or_default(), exchange.name);
    /// }
    /// ```
    pub async fn fetch_exchanges(
        &self,
        asset_class: Option<&str>,
        locale: Option<&str>,
    ) -> Result<Vec<Exchange>, MassiveError> {
        let mut url = format!("{}/v3/reference/exchanges", self.base_url());

        let mut params = Vec::new();
        if let Some(ac) = asset_class {
            params.push(format!("asset_class={}", urlencoding::encode(ac)));
        }
        if let Some(loc) = locale {
            params.push(format!("locale={}", urlencoding::encode(loc)));
        }
        if !params.is_empty() {
            url.push('?');
            url.push_str(&params.join("&"));
        }

        debug!(url = %url, "Fetching exchanges");

        let body = self.fetch_page_body(&url).await?;
        let parsed: ExchangesResponse =
            serde_json::from_str(&body).map_err(|e| MassiveError::Deserialize {
                message: e.to_string(),
                payload: body,
            })?;

        Ok(parsed
            .results
            .unwrap_or_default()
            .into_iter()
            .map(|r| r.into_exchange())
            .collect())
    }

    /// Fetch current market status.
    ///
    /// Returns the current trading status for various exchanges and overall markets.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let status = client.fetch_market_status().await?;
    /// println!("Market: {}, Crypto: {}", status.market, status.currencies.crypto);
    /// ```
    pub async fn fetch_market_status(&self) -> Result<MarketStatus, MassiveError> {
        let url = format!("{}/v1/marketstatus/now", self.base_url());
        debug!(url = %url, "Fetching market status");

        let body = self.fetch_page_body(&url).await?;
        let parsed: RawMarketStatus =
            serde_json::from_str(&body).map_err(|e| MassiveError::Deserialize {
                message: e.to_string(),
                payload: body,
            })?;

        parsed.into_market_status()
    }

    /// Fetch upcoming market holidays.
    ///
    /// Returns a list of upcoming holidays with their corresponding open/close times.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let holidays = client.fetch_market_holidays().await?;
    /// for holiday in holidays {
    ///     println!("{}: {} ({})", holiday.date, holiday.name, holiday.status);
    /// }
    /// ```
    pub async fn fetch_market_holidays(&self) -> Result<Vec<MarketHoliday>, MassiveError> {
        let url = format!("{}/v1/marketstatus/upcoming", self.base_url());
        debug!(url = %url, "Fetching market holidays");

        let body = self.fetch_page_body(&url).await?;
        let parsed: Vec<RawMarketHoliday> =
            serde_json::from_str(&body).map_err(|e| MassiveError::Deserialize {
                message: e.to_string(),
                payload: body,
            })?;

        parsed
            .into_iter()
            .map(|r| r.into_market_holiday())
            .collect()
    }

    /// Fetch dividends matching the query.
    ///
    /// Returns a stream that handles pagination automatically.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let query = DividendQuery::new().ticker("AAPL").limit(100);
    /// let mut stream = client.fetch_dividends(&query);
    /// while let Some(dividend) = stream.next().await {
    ///     println!("{:?}", dividend?);
    /// }
    /// ```
    pub fn fetch_dividends<'a>(
        &'a self,
        query: &'a DividendQuery,
    ) -> impl Stream<Item = Result<Dividend, MassiveError>> + 'a {
        let base_url = self.base_url();
        try_stream! {
            query.validate()?;

            let initial_url = format!(
                "{}/v3/reference/dividends{}",
                base_url,
                query.to_query_string()
            );

            let mut next_url: Option<String> = Some(initial_url);

            while let Some(url) = next_url.take() {
                debug!(url = %url, "Fetching dividends page");

                let body = self.fetch_page_body(&url).await?;
                let parsed: DividendsResponse = serde_json::from_str(&body).map_err(|e| {
                    MassiveError::Deserialize {
                        message: e.to_string(),
                        payload: body,
                    }
                })?;

                if let Some(results) = parsed.results {
                    for raw in results {
                        yield raw.into_dividend()?;
                    }
                }

                if let Some(ref url) = parsed.next_url {
                    Self::validate_next_url(url, base_url)?;
                }
                next_url = parsed.next_url;
            }
        }
    }

    /// Fetch stock splits matching the query.
    ///
    /// Returns a stream that handles pagination automatically.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let query = SplitQuery::new().ticker("AAPL").limit(100);
    /// let mut stream = client.fetch_splits(&query);
    /// while let Some(split) = stream.next().await {
    ///     println!("{:?}", split?);
    /// }
    /// ```
    pub fn fetch_splits<'a>(
        &'a self,
        query: &'a SplitQuery,
    ) -> impl Stream<Item = Result<StockSplit, MassiveError>> + 'a {
        let base_url = self.base_url();
        try_stream! {
            query.validate()?;

            let initial_url = format!(
                "{}/v3/reference/splits{}",
                base_url,
                query.to_query_string()
            );

            let mut next_url: Option<String> = Some(initial_url);

            while let Some(url) = next_url.take() {
                debug!(url = %url, "Fetching splits page");

                let body = self.fetch_page_body(&url).await?;
                let parsed: SplitsResponse = serde_json::from_str(&body).map_err(|e| {
                    MassiveError::Deserialize {
                        message: e.to_string(),
                        payload: body,
                    }
                })?;

                if let Some(results) = parsed.results {
                    for raw in results {
                        yield raw.into_stock_split()?;
                    }
                }

                if let Some(ref url) = parsed.next_url {
                    Self::validate_next_url(url, base_url)?;
                }
                next_url = parsed.next_url;
            }
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Tests should panic on unexpected values
mod tests {
    use super::*;

    #[test]
    fn test_ticker_query_empty() {
        let query = TickerQuery::new();
        assert_eq!(query.to_query_string(), "");
    }

    #[test]
    fn test_ticker_query_single_param() {
        let query = TickerQuery::new().market("stocks");
        assert_eq!(query.to_query_string(), "?market=stocks");
    }

    #[test]
    fn test_ticker_query_multiple_params() {
        let query = TickerQuery::new().market("stocks").active(true).limit(500);
        let qs = query.to_query_string();
        assert!(qs.contains("market=stocks"));
        assert!(qs.contains("active=true"));
        assert!(qs.contains("limit=500"));
    }

    #[test]
    fn test_ticker_query_type_serialization() {
        let query = TickerQuery::new().asset_type("CS");
        assert_eq!(query.to_query_string(), "?type=CS");
    }

    #[test]
    fn test_ticker_query_limit_clamping() {
        let query = TickerQuery::new().limit(5000);
        assert_eq!(query.limit, Some(1000));
    }

    #[test]
    fn test_ticker_query_url_encoding() {
        let query = TickerQuery::new().search("apple inc");
        assert_eq!(query.to_query_string(), "?search=apple%20inc");
    }

    #[test]
    fn test_ticker_query_range_operators() {
        let query = TickerQuery::new().ticker_gte("A").ticker_lt("B");
        let qs = query.to_query_string();
        assert!(qs.contains("ticker.gte=A"));
        assert!(qs.contains("ticker.lt=B"));
    }

    #[test]
    fn test_parse_ticker_response() {
        let json = r#"{
            "results": [{
                "ticker": "AAPL",
                "name": "Apple Inc.",
                "market": "stocks",
                "type": "CS",
                "active": true,
                "primary_exchange": "XNAS",
                "currency_name": "usd"
            }],
            "status": "OK",
            "count": 1
        }"#;

        let parsed: TickersResponse = serde_json::from_str(json).unwrap();
        let results = parsed.results.unwrap();
        assert_eq!(results.len(), 1);

        let ticker = results.into_iter().next().unwrap().into_ticker();
        assert_eq!(ticker.ticker, "AAPL");
        assert_eq!(ticker.name, "Apple Inc.");
        assert_eq!(ticker.market, "stocks");
        assert_eq!(ticker.asset_type, "CS");
        assert!(ticker.active);
        assert_eq!(ticker.primary_exchange, Some("XNAS".into()));
    }

    #[test]
    fn test_parse_ticker_details_response() {
        let json = r#"{
            "results": {
                "ticker": "AAPL",
                "name": "Apple Inc.",
                "market": "stocks",
                "type": "CS",
                "active": true,
                "description": "Apple Inc. designs, manufactures, and markets smartphones.",
                "market_cap": 2500000000000.0,
                "total_employees": 164000,
                "homepage_url": "https://www.apple.com",
                "address": {
                    "address1": "One Apple Park Way",
                    "city": "Cupertino",
                    "state": "CA",
                    "postal_code": "95014"
                },
                "list_date": "1980-12-12"
            },
            "status": "OK"
        }"#;

        let parsed: TickerDetailsResponse = serde_json::from_str(json).unwrap();
        let details = parsed.results.unwrap().into_ticker_details();

        assert_eq!(details.ticker.ticker, "AAPL");
        assert!(details.description.unwrap().contains("Apple"));
        assert_eq!(
            details.market_cap,
            Decimal::from_f64_retain(2_500_000_000_000.0)
        );
        assert_eq!(details.total_employees, Some(164_000));
        assert!(details.address.is_some());
        let addr = details.address.unwrap();
        assert_eq!(addr.city, Some("Cupertino".into()));
        assert_eq!(
            details.list_date,
            Some(NaiveDate::from_ymd_opt(1980, 12, 12).unwrap())
        );
    }

    #[test]
    fn test_parse_exchanges_response() {
        let json = r#"{
            "results": [{
                "id": 1,
                "name": "NYSE American, LLC",
                "acronym": "AMEX",
                "mic": "XASE",
                "operating_mic": "XNYS",
                "asset_class": "stocks",
                "locale": "us",
                "type": "exchange"
            }],
            "status": "OK"
        }"#;

        let parsed: ExchangesResponse = serde_json::from_str(json).unwrap();
        let results = parsed.results.unwrap();
        assert_eq!(results.len(), 1);

        let exchange = results.into_iter().next().unwrap().into_exchange();
        assert_eq!(exchange.id, 1);
        assert_eq!(exchange.name, "NYSE American, LLC");
        assert_eq!(exchange.mic, Some("XASE".into()));
        assert_eq!(exchange.asset_class, "stocks");
    }

    #[test]
    fn test_parse_market_status_response() {
        let json = r#"{
            "market": "open",
            "serverTime": "2026-05-06T14:30:00-04:00",
            "afterHours": false,
            "earlyHours": false,
            "currencies": {
                "crypto": "open",
                "fx": "open"
            },
            "exchanges": {
                "nasdaq": "open",
                "nyse": "open"
            },
            "indicesGroups": {
                "s_and_p": "open"
            }
        }"#;

        let parsed: RawMarketStatus = serde_json::from_str(json).unwrap();
        let status = parsed.into_market_status().unwrap();

        assert_eq!(status.market, "open");
        assert!(!status.after_hours);
        assert_eq!(status.currencies.crypto, "open");
        assert_eq!(status.exchanges.get("nasdaq"), Some(&"open".into()));
    }

    #[test]
    fn test_parse_market_holidays_response() {
        let json = r#"[
            {
                "date": "2026-12-25",
                "exchange": "NYSE",
                "name": "Christmas",
                "status": "closed"
            },
            {
                "date": "2026-11-27",
                "exchange": "NYSE",
                "name": "Thanksgiving",
                "status": "early-close",
                "open": "2026-11-27T13:30:00Z",
                "close": "2026-11-27T18:00:00Z"
            }
        ]"#;

        let parsed: Vec<RawMarketHoliday> = serde_json::from_str(json).unwrap();
        let holidays: Vec<MarketHoliday> = parsed
            .into_iter()
            .map(|r| r.into_market_holiday().unwrap())
            .collect();

        assert_eq!(holidays.len(), 2);
        assert_eq!(holidays[0].name, "Christmas");
        assert_eq!(holidays[0].status, "closed");
        assert!(holidays[0].open.is_none());

        assert_eq!(holidays[1].name, "Thanksgiving");
        assert_eq!(holidays[1].status, "early-close");
        assert!(holidays[1].open.is_some());
        assert!(holidays[1].close.is_some());
    }

    #[test]
    fn test_sort_order() {
        assert_eq!(SortOrder::Asc.as_str(), "asc");
        assert_eq!(SortOrder::Desc.as_str(), "desc");
        assert_eq!(SortOrder::default(), SortOrder::Asc);
    }

    // ========================================================================
    // Corporate Actions Tests
    // ========================================================================

    #[test]
    fn test_dividend_query_empty() {
        let query = DividendQuery::new();
        assert_eq!(query.to_query_string(), "");
    }

    #[test]
    fn test_dividend_query_with_ticker() {
        let query = DividendQuery::new().ticker("AAPL");
        assert_eq!(query.to_query_string(), "?ticker=AAPL");
    }

    #[test]
    fn test_dividend_query_with_date_range() {
        let from = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let to = NaiveDate::from_ymd_opt(2024, 12, 31).unwrap();
        let query = DividendQuery::new()
            .ticker("AAPL")
            .ex_dividend_date_gte(from)
            .ex_dividend_date_lte(to);
        let qs = query.to_query_string();
        assert!(qs.contains("ticker=AAPL"));
        assert!(qs.contains("ex_dividend_date.gte=2024-01-01"));
        assert!(qs.contains("ex_dividend_date.lte=2024-12-31"));
    }

    #[test]
    fn test_dividend_query_limit_clamping() {
        let query = DividendQuery::new().limit(5000);
        assert_eq!(query.limit, Some(1000));
    }

    #[test]
    fn test_parse_dividends_response() {
        let json = r#"{
            "results": [{
                "ticker": "AAPL",
                "cash_amount": 0.25,
                "currency": "USD",
                "declaration_date": "2024-02-01",
                "ex_dividend_date": "2024-02-09",
                "record_date": "2024-02-12",
                "pay_date": "2024-02-15",
                "frequency": 4,
                "dividend_type": "CD"
            }],
            "status": "OK"
        }"#;

        let parsed: DividendsResponse = serde_json::from_str(json).unwrap();
        let results = parsed.results.unwrap();
        assert_eq!(results.len(), 1);

        let dividend = results.into_iter().next().unwrap().into_dividend().unwrap();
        assert_eq!(dividend.ticker, "AAPL");
        assert_eq!(
            dividend.cash_amount,
            Decimal::from_f64_retain(0.25).unwrap()
        );
        assert_eq!(dividend.currency, "USD");
        assert_eq!(
            dividend.ex_dividend_date,
            NaiveDate::from_ymd_opt(2024, 2, 9).unwrap()
        );
        assert_eq!(dividend.frequency, Some(DividendFrequency::Quarterly));
        assert_eq!(dividend.dividend_type, Some("CD".into()));
    }

    #[test]
    fn test_parse_dividends_minimal() {
        let json = r#"{
            "results": [{
                "ticker": "MSFT",
                "cash_amount": 0.75,
                "ex_dividend_date": "2024-05-15"
            }],
            "status": "OK"
        }"#;

        let parsed: DividendsResponse = serde_json::from_str(json).unwrap();
        let results = parsed.results.unwrap();
        let dividend = results.into_iter().next().unwrap().into_dividend().unwrap();

        assert_eq!(dividend.ticker, "MSFT");
        assert_eq!(dividend.currency, "USD"); // Defaults to USD
        assert!(dividend.declaration_date.is_none());
        assert!(dividend.frequency.is_none());
    }

    #[test]
    fn test_split_query_empty() {
        let query = SplitQuery::new();
        assert_eq!(query.to_query_string(), "");
    }

    #[test]
    fn test_split_query_with_ticker() {
        let query = SplitQuery::new().ticker("TSLA");
        assert_eq!(query.to_query_string(), "?ticker=TSLA");
    }

    #[test]
    fn test_split_query_with_date_range() {
        let from = NaiveDate::from_ymd_opt(2020, 1, 1).unwrap();
        let to = NaiveDate::from_ymd_opt(2024, 12, 31).unwrap();
        let query = SplitQuery::new()
            .ticker("NVDA")
            .execution_date_gte(from)
            .execution_date_lte(to);
        let qs = query.to_query_string();
        assert!(qs.contains("ticker=NVDA"));
        assert!(qs.contains("execution_date.gte=2020-01-01"));
        assert!(qs.contains("execution_date.lte=2024-12-31"));
    }

    #[test]
    fn test_split_query_limit_clamping() {
        let query = SplitQuery::new().limit(2000);
        assert_eq!(query.limit, Some(1000));
    }

    #[test]
    fn test_parse_splits_response() {
        let json = r#"{
            "results": [{
                "ticker": "TSLA",
                "execution_date": "2022-08-25",
                "split_to": 3.0,
                "split_from": 1.0
            }],
            "status": "OK"
        }"#;

        let parsed: SplitsResponse = serde_json::from_str(json).unwrap();
        let results = parsed.results.unwrap();
        assert_eq!(results.len(), 1);

        let split = results
            .into_iter()
            .next()
            .unwrap()
            .into_stock_split()
            .unwrap();
        assert_eq!(split.ticker, "TSLA");
        assert_eq!(
            split.execution_date,
            NaiveDate::from_ymd_opt(2022, 8, 25).unwrap()
        );
        assert_eq!(split.split_to, Decimal::from(3));
        assert_eq!(split.split_from, Decimal::from(1));
    }

    #[test]
    fn test_parse_splits_reverse_split() {
        let json = r#"{
            "results": [{
                "ticker": "GE",
                "execution_date": "2021-08-02",
                "split_to": 1.0,
                "split_from": 8.0
            }],
            "status": "OK"
        }"#;

        let parsed: SplitsResponse = serde_json::from_str(json).unwrap();
        let results = parsed.results.unwrap();
        let split = results
            .into_iter()
            .next()
            .unwrap()
            .into_stock_split()
            .unwrap();

        assert_eq!(split.ticker, "GE");
        assert_eq!(split.split_to, Decimal::from(1));
        assert_eq!(split.split_from, Decimal::from(8)); // 1:8 reverse split
    }
}
