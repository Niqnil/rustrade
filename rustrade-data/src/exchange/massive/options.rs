//! Options reference data endpoints for Massive API.
//!
//! Provides access to option contract discovery and snapshots with Greeks.
//!
//! # Subscription Requirements
//!
//! | Endpoint | Minimum Plan | Data Recency |
//! |----------|--------------|--------------|
//! | Contracts (`/v3/reference/options/contracts`) | Options Basic (free) | Daily |
//! | Chain Snapshot (`/v3/snapshot/options/{underlying}`) | Options Starter | 15-min delayed (Starter/Developer), Real-time (Advanced/Business) |
//! | Contract Snapshot (`/v3/snapshot/options/{underlying}/{contract}`) | Options Starter | 15-min delayed (Starter/Developer), Real-time (Advanced/Business) |
//!
//! # Example
//!
//! ```ignore
//! use rustrade_data::exchange::massive::{MassiveRestClient, OptionContractQuery};
//!
//! let client = MassiveRestClient::from_env()?;
//!
//! // Discover AAPL options expiring in the next 30 days
//! let query = OptionContractQuery::new()
//!     .underlying_ticker("AAPL")
//!     .expiration_date_gte(chrono::Utc::now().date_naive())
//!     .expiration_date_lte(chrono::Utc::now().date_naive() + chrono::Duration::days(30))
//!     .limit(100);
//!
//! let contracts = client.fetch_option_contracts(&query).await?;
//! ```

use super::error::MassiveError;
use super::reference::SortOrder;
use super::rest::MassiveRestClient;
use crate::subscription::greeks::OptionGreeks;
use chrono::{NaiveDate, NaiveTime};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use rustrade_instrument::instrument::kind::option::{OptionExercise, OptionKind};
use rustrade_instrument::instrument::market_data::kind::MarketDataOptionContract;
use serde::Deserialize;
use tracing::{debug, warn};

// ============================================================================
// Query Builders
// ============================================================================

/// Filter parameters for the `/v3/reference/options/contracts` endpoint.
///
/// All fields are optional. Construct with [`OptionContractQuery::new`] and chain
/// setters. The fetch method handles pagination automatically.
///
/// # Example
///
/// ```ignore
/// use rustrade_data::exchange::massive::OptionContractQuery;
/// use chrono::NaiveDate;
///
/// let query = OptionContractQuery::new()
///     .underlying_ticker("AAPL")
///     .contract_type(OptionKind::Call)
///     .expiration_date_gte(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap())
///     .strike_price_gte(dec!(150))
///     .strike_price_lte(dec!(200))
///     .limit(500);
/// ```
#[derive(Debug, Default, Clone)]
pub struct OptionContractQuery {
    /// Filter by underlying stock ticker.
    pub underlying_ticker: Option<String>,
    /// Filter by contract type (call/put).
    pub contract_type: Option<OptionKind>,
    /// Filter by exact expiration date.
    pub expiration_date: Option<NaiveDate>,
    /// Filter by expiration date >= value.
    pub expiration_date_gte: Option<NaiveDate>,
    /// Filter by expiration date > value.
    pub expiration_date_gt: Option<NaiveDate>,
    /// Filter by expiration date <= value.
    pub expiration_date_lte: Option<NaiveDate>,
    /// Filter by expiration date < value.
    pub expiration_date_lt: Option<NaiveDate>,
    /// Filter by exact strike price.
    pub strike_price: Option<Decimal>,
    /// Filter by strike price >= value.
    pub strike_price_gte: Option<Decimal>,
    /// Filter by strike price > value.
    pub strike_price_gt: Option<Decimal>,
    /// Filter by strike price <= value.
    pub strike_price_lte: Option<Decimal>,
    /// Filter by strike price < value.
    pub strike_price_lt: Option<Decimal>,
    /// Include expired contracts (default: false).
    pub expired: Option<bool>,
    /// Point-in-time query date (default: today).
    pub as_of: Option<NaiveDate>,
    /// Results per page (max 1000).
    pub limit: Option<u16>,
    /// Sort direction.
    pub order: Option<SortOrder>,
    /// Field to sort by.
    pub sort: Option<String>,
}

impl OptionContractQuery {
    /// Create a new empty query.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Filter by underlying stock ticker (e.g., "AAPL").
    #[must_use]
    pub fn underlying_ticker(mut self, v: impl Into<String>) -> Self {
        self.underlying_ticker = Some(v.into());
        self
    }

    /// Filter by contract type (Call or Put).
    #[must_use]
    pub fn contract_type(mut self, v: OptionKind) -> Self {
        self.contract_type = Some(v);
        self
    }

    /// Filter by exact expiration date.
    #[must_use]
    pub fn expiration_date(mut self, v: NaiveDate) -> Self {
        self.expiration_date = Some(v);
        self
    }

    /// Filter by expiration date >= value.
    #[must_use]
    pub fn expiration_date_gte(mut self, v: NaiveDate) -> Self {
        self.expiration_date_gte = Some(v);
        self
    }

    /// Filter by expiration date > value.
    #[must_use]
    pub fn expiration_date_gt(mut self, v: NaiveDate) -> Self {
        self.expiration_date_gt = Some(v);
        self
    }

    /// Filter by expiration date <= value.
    #[must_use]
    pub fn expiration_date_lte(mut self, v: NaiveDate) -> Self {
        self.expiration_date_lte = Some(v);
        self
    }

    /// Filter by expiration date < value.
    #[must_use]
    pub fn expiration_date_lt(mut self, v: NaiveDate) -> Self {
        self.expiration_date_lt = Some(v);
        self
    }

    /// Filter by exact strike price.
    #[must_use]
    pub fn strike_price(mut self, v: Decimal) -> Self {
        self.strike_price = Some(v);
        self
    }

    /// Filter by strike price >= value.
    #[must_use]
    pub fn strike_price_gte(mut self, v: Decimal) -> Self {
        self.strike_price_gte = Some(v);
        self
    }

    /// Filter by strike price > value.
    #[must_use]
    pub fn strike_price_gt(mut self, v: Decimal) -> Self {
        self.strike_price_gt = Some(v);
        self
    }

    /// Filter by strike price <= value.
    #[must_use]
    pub fn strike_price_lte(mut self, v: Decimal) -> Self {
        self.strike_price_lte = Some(v);
        self
    }

    /// Filter by strike price < value.
    #[must_use]
    pub fn strike_price_lt(mut self, v: Decimal) -> Self {
        self.strike_price_lt = Some(v);
        self
    }

    /// Include expired contracts (default: false).
    #[must_use]
    pub fn expired(mut self, v: bool) -> Self {
        self.expired = Some(v);
        self
    }

    /// Point-in-time query date (default: today).
    #[must_use]
    pub fn as_of(mut self, v: NaiveDate) -> Self {
        self.as_of = Some(v);
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

    /// Validate the query for conflicting filters.
    pub fn validate(&self) -> Result<(), MassiveError> {
        let has_exact_exp = self.expiration_date.is_some();
        let has_range_exp = self.expiration_date_gte.is_some()
            || self.expiration_date_gt.is_some()
            || self.expiration_date_lte.is_some()
            || self.expiration_date_lt.is_some();

        if has_exact_exp && has_range_exp {
            return Err(MassiveError::InvalidInput {
                message: "OptionContractQuery: cannot set both expiration_date (exact) and \
                          expiration_date range filters"
                    .into(),
            });
        }

        let has_exact_strike = self.strike_price.is_some();
        let has_range_strike = self.strike_price_gte.is_some()
            || self.strike_price_gt.is_some()
            || self.strike_price_lte.is_some()
            || self.strike_price_lt.is_some();

        if has_exact_strike && has_range_strike {
            return Err(MassiveError::InvalidInput {
                message: "OptionContractQuery: cannot set both strike_price (exact) and \
                          strike_price range filters"
                    .into(),
            });
        }

        Ok(())
    }

    /// Build query string parameters.
    fn to_query_string(&self) -> String {
        let mut pairs: Vec<(&str, String)> = Vec::new();

        if let Some(ref v) = self.underlying_ticker {
            pairs.push(("underlying_ticker", v.clone()));
        }
        if let Some(v) = self.contract_type {
            pairs.push(("contract_type", v.to_string()));
        }
        if let Some(v) = self.expiration_date {
            pairs.push(("expiration_date", v.to_string()));
        }
        if let Some(v) = self.expiration_date_gte {
            pairs.push(("expiration_date.gte", v.to_string()));
        }
        if let Some(v) = self.expiration_date_gt {
            pairs.push(("expiration_date.gt", v.to_string()));
        }
        if let Some(v) = self.expiration_date_lte {
            pairs.push(("expiration_date.lte", v.to_string()));
        }
        if let Some(v) = self.expiration_date_lt {
            pairs.push(("expiration_date.lt", v.to_string()));
        }
        if let Some(v) = self.strike_price {
            pairs.push(("strike_price", v.to_string()));
        }
        if let Some(v) = self.strike_price_gte {
            pairs.push(("strike_price.gte", v.to_string()));
        }
        if let Some(v) = self.strike_price_gt {
            pairs.push(("strike_price.gt", v.to_string()));
        }
        if let Some(v) = self.strike_price_lte {
            pairs.push(("strike_price.lte", v.to_string()));
        }
        if let Some(v) = self.strike_price_lt {
            pairs.push(("strike_price.lt", v.to_string()));
        }
        if let Some(v) = self.expired {
            pairs.push(("expired", v.to_string()));
        }
        if let Some(v) = self.as_of {
            pairs.push(("as_of", v.to_string()));
        }
        if let Some(v) = self.limit {
            pairs.push(("limit", v.to_string()));
        }
        if let Some(v) = self.order {
            pairs.push(("order", v.to_string()));
        }
        if let Some(ref v) = self.sort {
            pairs.push(("sort", v.clone()));
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

/// Filter parameters for option snapshot endpoints.
///
/// Used with [`MassiveRestClient::fetch_option_chain_snapshot`]. Note that
/// the snapshot endpoints expose only `gte`/`lte` range operators (no `gt`/`lt`),
/// unlike [`OptionContractQuery`] which supports the full set.
#[derive(Debug, Default, Clone)]
pub struct OptionSnapshotQuery {
    /// Filter by exact strike price.
    pub strike_price: Option<Decimal>,
    /// Filter by strike price >= value.
    pub strike_price_gte: Option<Decimal>,
    /// Filter by strike price <= value.
    pub strike_price_lte: Option<Decimal>,
    /// Filter by exact expiration date.
    pub expiration_date: Option<NaiveDate>,
    /// Filter by expiration date >= value.
    pub expiration_date_gte: Option<NaiveDate>,
    /// Filter by expiration date <= value.
    pub expiration_date_lte: Option<NaiveDate>,
    /// Filter by contract type (call/put).
    pub contract_type: Option<OptionKind>,
    /// Results per page (max 250 for snapshots).
    pub limit: Option<u16>,
    /// Sort direction.
    pub order: Option<SortOrder>,
    /// Field to sort by.
    pub sort: Option<String>,
}

impl OptionSnapshotQuery {
    /// Create a new empty query.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Filter by exact strike price.
    #[must_use]
    pub fn strike_price(mut self, v: Decimal) -> Self {
        self.strike_price = Some(v);
        self
    }

    /// Filter by strike price >= value.
    #[must_use]
    pub fn strike_price_gte(mut self, v: Decimal) -> Self {
        self.strike_price_gte = Some(v);
        self
    }

    /// Filter by strike price <= value.
    #[must_use]
    pub fn strike_price_lte(mut self, v: Decimal) -> Self {
        self.strike_price_lte = Some(v);
        self
    }

    /// Filter by exact expiration date.
    #[must_use]
    pub fn expiration_date(mut self, v: NaiveDate) -> Self {
        self.expiration_date = Some(v);
        self
    }

    /// Filter by expiration date >= value.
    #[must_use]
    pub fn expiration_date_gte(mut self, v: NaiveDate) -> Self {
        self.expiration_date_gte = Some(v);
        self
    }

    /// Filter by expiration date <= value.
    #[must_use]
    pub fn expiration_date_lte(mut self, v: NaiveDate) -> Self {
        self.expiration_date_lte = Some(v);
        self
    }

    /// Filter by contract type (Call or Put).
    #[must_use]
    pub fn contract_type(mut self, v: OptionKind) -> Self {
        self.contract_type = Some(v);
        self
    }

    /// Set results per page (clamped to max 250 for snapshots).
    #[must_use]
    pub fn limit(mut self, v: u16) -> Self {
        self.limit = Some(v.min(250));
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

    /// Validate the query for conflicting filters.
    pub fn validate(&self) -> Result<(), MassiveError> {
        if self.strike_price.is_some()
            && (self.strike_price_gte.is_some() || self.strike_price_lte.is_some())
        {
            return Err(MassiveError::InvalidInput {
                message: "OptionSnapshotQuery: cannot set both strike_price (exact) and \
                          strike_price range filters"
                    .into(),
            });
        }

        if self.expiration_date.is_some()
            && (self.expiration_date_gte.is_some() || self.expiration_date_lte.is_some())
        {
            return Err(MassiveError::InvalidInput {
                message: "OptionSnapshotQuery: cannot set both expiration_date (exact) and \
                          expiration_date range filters"
                    .into(),
            });
        }

        Ok(())
    }

    /// Build query string parameters.
    fn to_query_string(&self) -> String {
        let mut pairs: Vec<(&str, String)> = Vec::new();

        if let Some(v) = self.strike_price {
            pairs.push(("strike_price", v.to_string()));
        }
        if let Some(v) = self.strike_price_gte {
            pairs.push(("strike_price.gte", v.to_string()));
        }
        if let Some(v) = self.strike_price_lte {
            pairs.push(("strike_price.lte", v.to_string()));
        }
        if let Some(v) = self.expiration_date {
            pairs.push(("expiration_date", v.to_string()));
        }
        if let Some(v) = self.expiration_date_gte {
            pairs.push(("expiration_date.gte", v.to_string()));
        }
        if let Some(v) = self.expiration_date_lte {
            pairs.push(("expiration_date.lte", v.to_string()));
        }
        if let Some(v) = self.contract_type {
            pairs.push(("contract_type", v.to_string()));
        }
        if let Some(v) = self.limit {
            pairs.push(("limit", v.to_string()));
        }
        if let Some(v) = self.order {
            pairs.push(("order", v.to_string()));
        }
        if let Some(ref v) = self.sort {
            pairs.push(("sort", v.clone()));
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

/// Option contract from the Massive contracts endpoint.
///
/// Carries Massive-specific fields (OCC ticker symbol, primary exchange MIC, CFI code,
/// shares per contract) alongside the standard option terms. Use
/// [`MassiveOptionContract::to_market_data_contract`] to obtain a library-standard
/// [`MarketDataOptionContract`] for cross-exchange consumers.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct MassiveOptionContract {
    /// Options contract ticker symbol (e.g., "O:AAPL251219C00150000").
    pub ticker: String,
    /// Underlying stock ticker (e.g., "AAPL").
    pub underlying_ticker: String,
    /// Contract type (Call or Put).
    pub contract_type: OptionKind,
    /// Exercise style (American, European, Bermudan).
    pub exercise_style: OptionExercise,
    /// Expiration date.
    pub expiration_date: NaiveDate,
    /// Strike price.
    pub strike_price: Decimal,
    /// Number of shares per contract (typically 100).
    pub shares_per_contract: u32,
    /// Primary exchange MIC code (e.g., "XCBO" for CBOE).
    pub primary_exchange: Option<String>,
    /// ISO 10962 CFI code.
    pub cfi: Option<String>,
}

impl MassiveOptionContract {
    /// Convert to a library-standard [`MarketDataOptionContract`].
    ///
    /// Massive returns expiration as a date with no intraday time component;
    /// this method anchors it to UTC midnight for the `DateTime<Utc>` field.
    ///
    /// A `From` impl is not provided because both `MassiveOptionContract` and
    /// `MarketDataOptionContract` are foreign to each other's defining crate
    /// (orphan rule).
    #[must_use]
    pub fn to_market_data_contract(&self) -> MarketDataOptionContract {
        MarketDataOptionContract {
            kind: self.contract_type,
            exercise: self.exercise_style,
            expiry: self.expiration_date.and_time(NaiveTime::MIN).and_utc(),
            strike: self.strike_price,
        }
    }
}

/// Option snapshot with Greeks and market data from Massive.
///
/// Returned by the snapshot endpoints. Requires Options Starter+ subscription.
///
/// # Note on Greeks
///
/// Greeks may be `None` for deep ITM options or when the exchange doesn't
/// compute them. Rho is NOT available from Massive.
///
/// `implied_volatility` is exposed at the snapshot level because Massive may
/// return IV even when no greeks block is present (deep ITM case).
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct MassiveOptionSnapshot {
    /// Contract details.
    pub contract: MassiveOptionContract,
    /// Greeks values (delta, gamma, theta, vega).
    pub greeks: Option<OptionGreeks>,
    /// Implied volatility. May be present even when `greeks` is `None`.
    pub implied_volatility: Option<f64>,
    /// Open interest (end of day).
    pub open_interest: Option<u64>,
    /// Break-even price.
    pub break_even_price: Option<Decimal>,
    /// Daily OHLCV data.
    pub day: Option<OptionDayBar>,
    /// Last quote (bid/ask).
    pub last_quote: Option<OptionQuote>,
    /// Last trade.
    pub last_trade: Option<OptionTrade>,
    /// Underlying asset price and metrics.
    pub underlying: Option<UnderlyingAsset>,
}

/// Daily OHLCV bar for an option contract.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct OptionDayBar {
    pub open: Option<Decimal>,
    pub high: Option<Decimal>,
    pub low: Option<Decimal>,
    pub close: Option<Decimal>,
    pub volume: Option<u64>,
    pub vwap: Option<Decimal>,
    pub change: Option<Decimal>,
    pub change_percent: Option<f64>,
}

/// Option quote (bid/ask).
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct OptionQuote {
    pub bid: Option<Decimal>,
    pub bid_size: Option<u64>,
    pub ask: Option<Decimal>,
    pub ask_size: Option<u64>,
    pub midpoint: Option<Decimal>,
}

/// Option trade.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct OptionTrade {
    pub price: Option<Decimal>,
    pub size: Option<u64>,
    /// Massive exchange ID integer code. See Massive API "Exchanges" reference docs
    /// for the mapping from integer to exchange MIC.
    pub exchange: Option<i32>,
    /// Massive sale condition integer codes. See Massive API "Conditions" reference
    /// docs for the mapping from integer to condition meaning.
    pub conditions: Option<Vec<i32>>,
}

/// Underlying asset data included in option snapshots.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct UnderlyingAsset {
    pub ticker: String,
    pub price: Option<Decimal>,
    pub change: Option<Decimal>,
    pub change_percent: Option<f64>,
}

// ============================================================================
// Internal Deserialization Types
// ============================================================================

#[derive(Deserialize)]
struct ContractsResponse {
    results: Option<Vec<RawContract>>,
    next_url: Option<String>,
    // Deserialized to satisfy serde; not surfaced in public type.
    #[allow(dead_code)]
    status: Option<String>,
    // Deserialized to satisfy serde; not surfaced in public type.
    #[allow(dead_code)]
    request_id: Option<String>,
}

#[derive(Deserialize)]
struct RawContract {
    ticker: String,
    underlying_ticker: Option<String>,
    contract_type: Option<String>,
    exercise_style: Option<String>,
    expiration_date: Option<String>,
    #[serde(default, with = "rust_decimal::serde::float_option")]
    strike_price: Option<Decimal>,
    shares_per_contract: Option<u32>,
    primary_exchange: Option<String>,
    cfi: Option<String>,
}

impl RawContract {
    fn into_massive_option_contract(self) -> Result<MassiveOptionContract, MassiveError> {
        let ticker = self.ticker;

        let underlying_ticker =
            self.underlying_ticker
                .ok_or_else(|| MassiveError::Deserialize {
                    message: "missing underlying_ticker".into(),
                    payload: ticker.clone(),
                })?;

        let contract_type = match self.contract_type.as_deref() {
            Some("call") => OptionKind::Call,
            Some("put") => OptionKind::Put,
            other => {
                return Err(MassiveError::Deserialize {
                    message: format!("invalid contract_type: {:?}", other),
                    payload: ticker.clone(),
                });
            }
        };

        let exercise_style = match self.exercise_style.as_deref() {
            Some("american") => OptionExercise::American,
            Some("european") => OptionExercise::European,
            Some("bermudan") => OptionExercise::Bermudan,
            other => {
                warn!(
                    ticker = %ticker,
                    exercise_style = ?other,
                    "Unknown exercise_style, defaulting to American"
                );
                OptionExercise::American
            }
        };

        let expiration_date = self
            .expiration_date
            .ok_or_else(|| MassiveError::Deserialize {
                message: "missing expiration_date".into(),
                payload: ticker.clone(),
            })?
            .parse()
            .map_err(|e| MassiveError::Deserialize {
                message: format!("invalid expiration_date: {e}"),
                payload: ticker.clone(),
            })?;

        let strike_price = self.strike_price.ok_or_else(|| MassiveError::Deserialize {
            message: "missing strike_price".into(),
            payload: ticker.clone(),
        })?;

        Ok(MassiveOptionContract {
            ticker,
            underlying_ticker,
            contract_type,
            exercise_style,
            expiration_date,
            strike_price,
            shares_per_contract: self.shares_per_contract.unwrap_or(100),
            primary_exchange: self.primary_exchange,
            cfi: self.cfi,
        })
    }
}

#[derive(Deserialize)]
struct SnapshotResponse {
    results: Option<Vec<RawSnapshot>>,
    next_url: Option<String>,
    // Deserialized to satisfy serde; not surfaced in public type.
    #[allow(dead_code)]
    status: Option<String>,
    // Deserialized to satisfy serde; not surfaced in public type.
    #[allow(dead_code)]
    request_id: Option<String>,
}

#[derive(Deserialize)]
struct SingleSnapshotResponse {
    results: Option<RawSnapshot>,
    // Deserialized to satisfy serde; not surfaced in public type.
    #[allow(dead_code)]
    status: Option<String>,
    // Deserialized to satisfy serde; not surfaced in public type.
    #[allow(dead_code)]
    request_id: Option<String>,
}

#[derive(Deserialize)]
struct RawSnapshot {
    details: Option<RawContract>,
    greeks: Option<RawGreeks>,
    implied_volatility: Option<f64>,
    open_interest: Option<u64>,
    #[serde(default, with = "rust_decimal::serde::float_option")]
    break_even_price: Option<Decimal>,
    day: Option<RawDayBar>,
    last_quote: Option<RawQuote>,
    last_trade: Option<RawTrade>,
    underlying_asset: Option<RawUnderlying>,
}

#[derive(Deserialize)]
struct RawGreeks {
    delta: Option<f64>,
    gamma: Option<f64>,
    theta: Option<f64>,
    vega: Option<f64>,
}

#[derive(Deserialize)]
struct RawDayBar {
    #[serde(default, with = "rust_decimal::serde::float_option")]
    open: Option<Decimal>,
    #[serde(default, with = "rust_decimal::serde::float_option")]
    high: Option<Decimal>,
    #[serde(default, with = "rust_decimal::serde::float_option")]
    low: Option<Decimal>,
    #[serde(default, with = "rust_decimal::serde::float_option")]
    close: Option<Decimal>,
    volume: Option<u64>,
    #[serde(default, with = "rust_decimal::serde::float_option")]
    vwap: Option<Decimal>,
    #[serde(default, with = "rust_decimal::serde::float_option")]
    change: Option<Decimal>,
    change_percent: Option<f64>,
}

#[derive(Deserialize)]
struct RawQuote {
    #[serde(default, with = "rust_decimal::serde::float_option")]
    bid: Option<Decimal>,
    bid_size: Option<u64>,
    #[serde(default, with = "rust_decimal::serde::float_option")]
    ask: Option<Decimal>,
    ask_size: Option<u64>,
    #[serde(default, with = "rust_decimal::serde::float_option")]
    midpoint: Option<Decimal>,
}

#[derive(Deserialize)]
struct RawTrade {
    #[serde(default, with = "rust_decimal::serde::float_option")]
    price: Option<Decimal>,
    size: Option<u64>,
    exchange: Option<i32>,
    conditions: Option<Vec<i32>>,
}

#[derive(Deserialize)]
struct RawUnderlying {
    ticker: Option<String>,
    #[serde(default, with = "rust_decimal::serde::float_option")]
    price: Option<Decimal>,
    #[serde(default, with = "rust_decimal::serde::float_option")]
    change: Option<Decimal>,
    change_percent: Option<f64>,
}

impl RawSnapshot {
    fn into_massive_option_snapshot(self) -> Result<MassiveOptionSnapshot, MassiveError> {
        let contract = self
            .details
            .ok_or_else(|| MassiveError::Deserialize {
                message: "missing details in snapshot".into(),
                payload: String::new(),
            })?
            .into_massive_option_contract()?;

        let greeks = self.greeks.map(|g| OptionGreeks {
            delta: g.delta,
            gamma: g.gamma,
            theta: g.theta,
            vega: g.vega,
            implied_volatility: self.implied_volatility,
            theoretical_price: None,
            // Decimal::to_f64 returns None only for values outside f64 range
            // (impossible for realistic underlying prices); fall back to NAN
            // rather than 0.0 to preserve the "data unavailable" signal in
            // downstream Greeks computations.
            underlying_price: self
                .underlying_asset
                .as_ref()
                .and_then(|u| u.price)
                .map(|d| d.to_f64().unwrap_or(f64::NAN)),
        });

        let day = self.day.map(|d| OptionDayBar {
            open: d.open,
            high: d.high,
            low: d.low,
            close: d.close,
            volume: d.volume,
            vwap: d.vwap,
            change: d.change,
            change_percent: d.change_percent,
        });

        let last_quote = self.last_quote.map(|q| OptionQuote {
            bid: q.bid,
            bid_size: q.bid_size,
            ask: q.ask,
            ask_size: q.ask_size,
            midpoint: q.midpoint,
        });

        let last_trade = self.last_trade.map(|t| OptionTrade {
            price: t.price,
            size: t.size,
            exchange: t.exchange,
            conditions: t.conditions,
        });

        let underlying = self.underlying_asset.and_then(|u| {
            u.ticker.map(|ticker| UnderlyingAsset {
                ticker,
                price: u.price,
                change: u.change,
                change_percent: u.change_percent,
            })
        });

        Ok(MassiveOptionSnapshot {
            contract,
            greeks,
            implied_volatility: self.implied_volatility,
            open_interest: self.open_interest,
            break_even_price: self.break_even_price,
            day,
            last_quote,
            last_trade,
            underlying,
        })
    }
}

// ============================================================================
// Client Implementation
// ============================================================================

impl MassiveRestClient {
    /// Fetch option contracts matching the query.
    ///
    /// Automatically paginates through all results.
    ///
    /// # Subscription
    ///
    /// Available on all Options plans including Basic (free tier).
    ///
    /// # Example
    ///
    /// ```ignore
    /// use rustrade_data::exchange::massive::{MassiveRestClient, OptionContractQuery};
    ///
    /// let client = MassiveRestClient::from_env()?;
    /// let query = OptionContractQuery::new()
    ///     .underlying_ticker("AAPL")
    ///     .contract_type(OptionKind::Call)
    ///     .limit(100);
    ///
    /// let contracts = client.fetch_option_contracts(&query).await?;
    /// for c in &contracts {
    ///     println!("{}: {} {} @ {}", c.ticker, c.expiration_date, c.contract_type, c.strike_price);
    /// }
    /// ```
    pub async fn fetch_option_contracts(
        &self,
        query: &OptionContractQuery,
    ) -> Result<Vec<MassiveOptionContract>, MassiveError> {
        query.validate()?;

        let base_url = self.base_url();
        let initial_url = format!(
            "{}/v3/reference/options/contracts{}",
            base_url,
            query.to_query_string()
        );

        let mut contracts: Vec<MassiveOptionContract> =
            Vec::with_capacity(query.limit.unwrap_or(100) as usize);
        let mut next_url: Option<String> = Some(initial_url);

        while let Some(url) = next_url.take() {
            debug!(url = %url, "Fetching option contracts page");

            let body = self.fetch_page_body(&url).await?;
            let parsed: ContractsResponse =
                serde_json::from_str(&body).map_err(|e| MassiveError::Deserialize {
                    message: e.to_string(),
                    payload: body,
                })?;

            if let Some(results) = parsed.results {
                for raw in results {
                    contracts.push(raw.into_massive_option_contract()?);
                }
            }

            if let Some(ref url) = parsed.next_url {
                Self::validate_next_url(url, base_url)?;
            }
            next_url = parsed.next_url;
        }

        Ok(contracts)
    }

    /// Fetch option chain snapshot for an underlying asset.
    ///
    /// Returns all option contracts for the underlying with current market data
    /// including Greeks.
    ///
    /// # Subscription
    ///
    /// Requires Options Starter or higher subscription. Data recency:
    /// - Starter/Developer: 15-minute delayed
    /// - Advanced/Business: Real-time
    ///
    /// # Note
    ///
    /// Greeks may be `None` for deep ITM options. Rho is NOT available.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use rustrade_data::exchange::massive::{MassiveRestClient, OptionSnapshotQuery};
    ///
    /// let client = MassiveRestClient::from_env()?;
    /// let query = OptionSnapshotQuery::new()
    ///     .strike_price_gte(dec!(150))
    ///     .strike_price_lte(dec!(200))
    ///     .limit(50);
    ///
    /// let snapshots = client.fetch_option_chain_snapshot("AAPL", &query).await?;
    /// for snap in snapshots {
    ///     if let Some(greeks) = &snap.greeks {
    ///         println!("{}: delta={:?}", snap.contract.ticker, greeks.delta);
    ///     }
    /// }
    /// ```
    pub async fn fetch_option_chain_snapshot(
        &self,
        underlying: &str,
        query: &OptionSnapshotQuery,
    ) -> Result<Vec<MassiveOptionSnapshot>, MassiveError> {
        Self::validate_ticker(underlying)?;
        query.validate()?;

        let base_url = self.base_url();
        let initial_url = format!(
            "{}/v3/snapshot/options/{}{}",
            base_url,
            underlying,
            query.to_query_string()
        );

        let mut snapshots: Vec<MassiveOptionSnapshot> =
            Vec::with_capacity(query.limit.unwrap_or(50) as usize);
        let mut next_url: Option<String> = Some(initial_url);

        while let Some(url) = next_url.take() {
            debug!(url = %url, "Fetching option chain snapshot page");

            let body = self.fetch_page_body(&url).await?;
            let parsed: SnapshotResponse =
                serde_json::from_str(&body).map_err(|e| MassiveError::Deserialize {
                    message: e.to_string(),
                    payload: body,
                })?;

            if let Some(results) = parsed.results {
                for raw in results {
                    snapshots.push(raw.into_massive_option_snapshot()?);
                }
            }

            if let Some(ref url) = parsed.next_url {
                Self::validate_next_url(url, base_url)?;
            }
            next_url = parsed.next_url;
        }

        Ok(snapshots)
    }

    /// Fetch a single option contract snapshot.
    ///
    /// # Subscription
    ///
    /// Requires Options Starter or higher subscription.
    ///
    /// # Arguments
    ///
    /// * `underlying` - Underlying ticker (e.g., "AAPL")
    /// * `contract` - Option contract ticker (e.g., "O:AAPL251219C00150000")
    ///
    /// # Example
    ///
    /// ```ignore
    /// let snapshot = client.fetch_option_snapshot("AAPL", "O:AAPL251219C00150000").await?;
    /// println!("IV: {:?}, Delta: {:?}",
    ///     snapshot.implied_volatility,
    ///     snapshot.greeks.as_ref().and_then(|g| g.delta)
    /// );
    /// ```
    pub async fn fetch_option_snapshot(
        &self,
        underlying: &str,
        contract: &str,
    ) -> Result<MassiveOptionSnapshot, MassiveError> {
        Self::validate_ticker(underlying)?;
        Self::validate_ticker(contract)?;

        let url = format!(
            "{}/v3/snapshot/options/{}/{}",
            self.base_url(),
            underlying,
            contract
        );

        debug!(url = %url, "Fetching option contract snapshot");

        let body = self.fetch_page_body(&url).await?;
        let parsed: SingleSnapshotResponse =
            serde_json::from_str(&body).map_err(|e| MassiveError::Deserialize {
                message: e.to_string(),
                payload: body,
            })?;

        parsed
            .results
            .ok_or_else(|| MassiveError::Api {
                status: 404,
                message: format!("Option contract not found: {}", contract),
            })?
            .into_massive_option_snapshot()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn test_option_contract_query_empty() {
        let query = OptionContractQuery::new();
        assert_eq!(query.to_query_string(), "");
    }

    #[test]
    fn test_option_contract_query_underlying() {
        let query = OptionContractQuery::new().underlying_ticker("AAPL");
        assert_eq!(query.to_query_string(), "?underlying_ticker=AAPL");
    }

    #[test]
    fn test_option_contract_query_full() {
        let query = OptionContractQuery::new()
            .underlying_ticker("AAPL")
            .contract_type(OptionKind::Call)
            .expiration_date_gte(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap())
            .strike_price_gte(dec!(150))
            .strike_price_lte(dec!(200))
            .limit(500);

        let qs = query.to_query_string();
        assert!(qs.contains("underlying_ticker=AAPL"));
        assert!(qs.contains("contract_type=call"));
        assert!(qs.contains("expiration_date.gte=2024-01-01"));
        assert!(qs.contains("strike_price.gte=150"));
        assert!(qs.contains("strike_price.lte=200"));
        assert!(qs.contains("limit=500"));
    }

    #[test]
    fn test_option_contract_query_validation_ok() {
        let query = OptionContractQuery::new()
            .expiration_date_gte(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap())
            .expiration_date_lte(NaiveDate::from_ymd_opt(2024, 12, 31).unwrap());
        assert!(query.validate().is_ok());
    }

    #[test]
    fn test_option_contract_query_validation_conflict_expiration() {
        let query = OptionContractQuery::new()
            .expiration_date(NaiveDate::from_ymd_opt(2024, 6, 21).unwrap())
            .expiration_date_gte(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap());
        assert!(query.validate().is_err());
    }

    #[test]
    fn test_option_contract_query_validation_conflict_strike() {
        let query = OptionContractQuery::new()
            .strike_price(dec!(150))
            .strike_price_gte(dec!(100));
        assert!(query.validate().is_err());
    }

    #[test]
    fn test_option_snapshot_query_empty() {
        let query = OptionSnapshotQuery::new();
        assert_eq!(query.to_query_string(), "");
    }

    #[test]
    fn test_option_snapshot_query_limit_clamped() {
        let query = OptionSnapshotQuery::new().limit(500);
        assert!(query.to_query_string().contains("limit=250"));
    }

    #[test]
    fn test_option_snapshot_query_validation_conflict_strike() {
        let query = OptionSnapshotQuery::new()
            .strike_price(dec!(150))
            .strike_price_gte(dec!(100));
        assert!(query.validate().is_err());
    }

    #[test]
    fn test_option_snapshot_query_validation_conflict_expiration() {
        let query = OptionSnapshotQuery::new()
            .expiration_date(NaiveDate::from_ymd_opt(2024, 6, 21).unwrap())
            .expiration_date_lte(NaiveDate::from_ymd_opt(2024, 12, 31).unwrap());
        assert!(query.validate().is_err());
    }

    #[test]
    fn test_raw_contract_parsing() {
        let json = r#"{
            "ticker": "O:AAPL251219C00150000",
            "underlying_ticker": "AAPL",
            "contract_type": "call",
            "exercise_style": "american",
            "expiration_date": "2025-12-19",
            "strike_price": 150.0,
            "shares_per_contract": 100,
            "primary_exchange": "XCBO",
            "cfi": "OCASPS"
        }"#;

        let raw: RawContract = serde_json::from_str(json).unwrap();
        let contract = raw.into_massive_option_contract().unwrap();

        assert_eq!(contract.ticker, "O:AAPL251219C00150000");
        assert_eq!(contract.underlying_ticker, "AAPL");
        assert_eq!(contract.contract_type, OptionKind::Call);
        assert_eq!(contract.exercise_style, OptionExercise::American);
        assert_eq!(
            contract.expiration_date,
            NaiveDate::from_ymd_opt(2025, 12, 19).unwrap()
        );
        assert_eq!(contract.strike_price, dec!(150));
        assert_eq!(contract.shares_per_contract, 100);
        assert_eq!(contract.primary_exchange, Some("XCBO".to_string()));
    }

    #[test]
    fn test_raw_contract_parsing_put() {
        let json = r#"{
            "ticker": "O:AAPL251219P00150000",
            "underlying_ticker": "AAPL",
            "contract_type": "put",
            "exercise_style": "european",
            "expiration_date": "2025-12-19",
            "strike_price": 150.0
        }"#;

        let raw: RawContract = serde_json::from_str(json).unwrap();
        let contract = raw.into_massive_option_contract().unwrap();

        assert_eq!(contract.contract_type, OptionKind::Put);
        assert_eq!(contract.exercise_style, OptionExercise::European);
        assert_eq!(contract.shares_per_contract, 100); // default
    }

    #[test]
    fn test_to_market_data_contract() {
        let raw: RawContract = serde_json::from_str(
            r#"{
                "ticker": "O:AAPL251219C00150000",
                "underlying_ticker": "AAPL",
                "contract_type": "call",
                "exercise_style": "american",
                "expiration_date": "2025-12-19",
                "strike_price": 150.0
            }"#,
        )
        .unwrap();
        let massive = raw.into_massive_option_contract().unwrap();
        let md = massive.to_market_data_contract();

        assert_eq!(md.kind, OptionKind::Call);
        assert_eq!(md.exercise, OptionExercise::American);
        assert_eq!(md.strike, dec!(150));
        // Expiration is anchored at UTC midnight on the expiration date.
        assert_eq!(
            md.expiry,
            NaiveDate::from_ymd_opt(2025, 12, 19)
                .unwrap()
                .and_time(NaiveTime::MIN)
                .and_utc()
        );
    }

    #[test]
    fn test_raw_snapshot_parsing() {
        let json = r#"{
            "details": {
                "ticker": "O:AAPL251219C00150000",
                "underlying_ticker": "AAPL",
                "contract_type": "call",
                "exercise_style": "american",
                "expiration_date": "2025-12-19",
                "strike_price": 150.0
            },
            "greeks": {
                "delta": 0.55,
                "gamma": 0.02,
                "theta": -0.05,
                "vega": 0.15
            },
            "implied_volatility": 0.25,
            "open_interest": 5000,
            "break_even_price": 155.50,
            "day": {
                "open": 5.00,
                "high": 5.50,
                "low": 4.80,
                "close": 5.25,
                "volume": 1000
            },
            "last_quote": {
                "bid": 5.20,
                "ask": 5.30,
                "bid_size": 50,
                "ask_size": 75
            },
            "underlying_asset": {
                "ticker": "AAPL",
                "price": 175.50,
                "change": 2.50,
                "change_percent": 1.44
            }
        }"#;

        let raw: RawSnapshot = serde_json::from_str(json).unwrap();
        let snapshot = raw.into_massive_option_snapshot().unwrap();

        assert_eq!(snapshot.contract.ticker, "O:AAPL251219C00150000");
        assert!(snapshot.greeks.is_some());

        let greeks = snapshot.greeks.unwrap();
        assert_eq!(greeks.delta, Some(0.55));
        assert_eq!(greeks.gamma, Some(0.02));
        assert_eq!(greeks.theta, Some(-0.05));
        assert_eq!(greeks.vega, Some(0.15));
        assert_eq!(greeks.implied_volatility, Some(0.25));
        assert_eq!(greeks.underlying_price, Some(175.50));

        assert_eq!(snapshot.implied_volatility, Some(0.25));
        assert_eq!(snapshot.open_interest, Some(5000));
        assert_eq!(snapshot.break_even_price, Some(dec!(155.50)));

        let day = snapshot.day.unwrap();
        assert_eq!(day.open, Some(dec!(5.00)));
        assert_eq!(day.close, Some(dec!(5.25)));

        let underlying = snapshot.underlying.unwrap();
        assert_eq!(underlying.ticker, "AAPL");
        assert_eq!(underlying.price, Some(dec!(175.50)));
    }

    #[test]
    fn test_raw_snapshot_minimal() {
        let json = r#"{
            "details": {
                "ticker": "O:AAPL251219C00150000",
                "underlying_ticker": "AAPL",
                "contract_type": "call",
                "exercise_style": "american",
                "expiration_date": "2025-12-19",
                "strike_price": 150.0
            }
        }"#;

        let raw: RawSnapshot = serde_json::from_str(json).unwrap();
        let snapshot = raw.into_massive_option_snapshot().unwrap();

        assert_eq!(snapshot.contract.ticker, "O:AAPL251219C00150000");
        assert!(snapshot.greeks.is_none());
        assert!(snapshot.day.is_none());
        assert!(snapshot.last_quote.is_none());
    }

    #[test]
    fn test_raw_snapshot_iv_without_greeks() {
        // Massive may return implied_volatility at the snapshot level
        // even when no greeks block is present (deep ITM case).
        let json = r#"{
            "details": {
                "ticker": "O:AAPL251219C00150000",
                "underlying_ticker": "AAPL",
                "contract_type": "call",
                "exercise_style": "american",
                "expiration_date": "2025-12-19",
                "strike_price": 150.0
            },
            "implied_volatility": 0.42
        }"#;

        let raw: RawSnapshot = serde_json::from_str(json).unwrap();
        let snapshot = raw.into_massive_option_snapshot().unwrap();

        assert!(snapshot.greeks.is_none());
        assert_eq!(snapshot.implied_volatility, Some(0.42));
    }
}
