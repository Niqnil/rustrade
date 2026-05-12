//! Option contract discovery types and methods.
//!
//! Implements `GET /v2/options/contracts` for querying available option contracts.

use super::{AlpacaOptionsClient, AlpacaOptionsError};
use chrono::NaiveDate;
use rust_decimal::Decimal;
use rustrade_instrument::instrument::kind::option::{OptionExercise, OptionKind};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use tracing::debug;

/// Maximum contracts per page (Alpaca API limit).
const MAX_PAGE_SIZE: usize = 10_000;

/// Default page size for contract queries.
const DEFAULT_PAGE_SIZE: usize = 1_000;

/// Maximum pages to fetch before stopping (safety limit).
const MAX_PAGES: usize = 100;

/// Query parameters for option contract discovery.
///
/// Use builder methods to construct the query, then pass to
/// [`AlpacaOptionsClient::fetch_contracts`].
///
/// # Example
///
/// ```
/// use rustrade_data::exchange::alpaca::options::AlpacaOptionContractQuery;
/// use chrono::NaiveDate;
/// use rust_decimal_macros::dec;
///
/// let query = AlpacaOptionContractQuery::new(vec!["AAPL".into(), "TSLA".into()])
///     .expiration_gte(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap())
///     .expiration_lte(NaiveDate::from_ymd_opt(2024, 3, 31).unwrap())
///     .strike_gte(dec!(100))
///     .strike_lte(dec!(200))
///     .call_only();
/// ```
#[derive(Debug, Clone, Default)]
pub struct AlpacaOptionContractQuery {
    /// Filter by underlying symbols (e.g., ["AAPL", "TSLA"]).
    pub underlying_symbols: Vec<String>,
    /// Minimum expiration date (inclusive).
    pub expiration_date_gte: Option<NaiveDate>,
    /// Maximum expiration date (inclusive).
    pub expiration_date_lte: Option<NaiveDate>,
    /// Minimum strike price (inclusive).
    pub strike_price_gte: Option<Decimal>,
    /// Maximum strike price (inclusive).
    pub strike_price_lte: Option<Decimal>,
    /// Filter by option type (call or put).
    pub option_type: Option<OptionKind>,
    /// Filter by exercise style (american or european).
    pub style: Option<OptionExercise>,
    /// Page size (default 1000, max 10000).
    pub limit: Option<usize>,
}

impl AlpacaOptionContractQuery {
    /// Create a new query for the given underlying symbols.
    pub fn new(underlying_symbols: Vec<String>) -> Self {
        Self {
            underlying_symbols,
            ..Default::default()
        }
    }

    /// Filter contracts expiring on or after this date.
    #[must_use]
    pub fn expiration_gte(mut self, date: NaiveDate) -> Self {
        self.expiration_date_gte = Some(date);
        self
    }

    /// Filter contracts expiring on or before this date.
    #[must_use]
    pub fn expiration_lte(mut self, date: NaiveDate) -> Self {
        self.expiration_date_lte = Some(date);
        self
    }

    /// Filter contracts with strike price at or above this value.
    #[must_use]
    pub fn strike_gte(mut self, strike: Decimal) -> Self {
        self.strike_price_gte = Some(strike);
        self
    }

    /// Filter contracts with strike price at or below this value.
    #[must_use]
    pub fn strike_lte(mut self, strike: Decimal) -> Self {
        self.strike_price_lte = Some(strike);
        self
    }

    /// Filter to call options only.
    #[must_use]
    pub fn call_only(mut self) -> Self {
        self.option_type = Some(OptionKind::Call);
        self
    }

    /// Filter to put options only.
    #[must_use]
    pub fn put_only(mut self) -> Self {
        self.option_type = Some(OptionKind::Put);
        self
    }

    /// Filter by option type.
    #[must_use]
    pub fn option_type(mut self, kind: OptionKind) -> Self {
        self.option_type = Some(kind);
        self
    }

    /// Filter by exercise style.
    #[must_use]
    pub fn style(mut self, style: OptionExercise) -> Self {
        self.style = Some(style);
        self
    }

    /// Set page size (max 10000).
    #[must_use]
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit.min(MAX_PAGE_SIZE));
        self
    }

    /// Build query parameters for the API request.
    fn to_query_params(&self) -> Vec<(&'static str, String)> {
        let mut params = Vec::new();

        if !self.underlying_symbols.is_empty() {
            params.push(("underlying_symbols", self.underlying_symbols.join(",")));
        }
        if let Some(date) = self.expiration_date_gte {
            params.push(("expiration_date_gte", date.format("%Y-%m-%d").to_string()));
        }
        if let Some(date) = self.expiration_date_lte {
            params.push(("expiration_date_lte", date.format("%Y-%m-%d").to_string()));
        }
        if let Some(strike) = self.strike_price_gte {
            params.push(("strike_price_gte", strike.to_string()));
        }
        if let Some(strike) = self.strike_price_lte {
            params.push(("strike_price_lte", strike.to_string()));
        }
        if let Some(kind) = self.option_type {
            params.push((
                "type",
                match kind {
                    OptionKind::Call => "call",
                    OptionKind::Put => "put",
                }
                .to_string(),
            ));
        }
        if let Some(style) = self.style {
            // Alpaca only supports American and European styles; Bermudan is silently
            // omitted from the request (the API would reject it).
            let style_str = match style {
                OptionExercise::American => Some("american"),
                OptionExercise::European => Some("european"),
                OptionExercise::Bermudan => None,
            };
            if let Some(s) = style_str {
                params.push(("style", s.to_string()));
            }
        }

        let limit = self.limit.unwrap_or(DEFAULT_PAGE_SIZE).min(MAX_PAGE_SIZE);
        params.push(("limit", limit.to_string()));

        // Only return active contracts
        params.push(("status", "active".to_string()));

        params
    }
}

/// Alpaca option contract from the API response.
///
/// Represents a single option contract with its metadata.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AlpacaOptionContract {
    /// Unique contract identifier.
    pub id: String,
    /// OCC symbol (e.g., "AAPL240119C00150000").
    pub symbol: String,
    /// Human-readable name (e.g., "AAPL Jan 19 2024 150 Call").
    pub name: String,
    /// Contract status ("active" or "inactive").
    pub status: String,
    /// Whether the contract is tradable.
    pub tradable: bool,
    /// Expiration date.
    #[serde(with = "date_format")]
    pub expiration_date: NaiveDate,
    /// Root symbol (e.g., "AAPL").
    pub root_symbol: String,
    /// Underlying symbol.
    pub underlying_symbol: String,
    /// Underlying asset ID.
    pub underlying_asset_id: String,
    /// Option type ("call" or "put").
    #[serde(rename = "type")]
    pub option_type: String,
    /// Exercise style ("american" or "european").
    pub style: String,
    /// Strike price.
    #[serde(deserialize_with = "deserialize_decimal_string")]
    pub strike_price: Decimal,
    /// Contract multiplier (e.g., "100").
    #[serde(deserialize_with = "deserialize_decimal_string")]
    pub size: Decimal,
    /// Open interest (number of outstanding contracts).
    #[serde(default, deserialize_with = "deserialize_option_decimal_string")]
    pub open_interest: Option<Decimal>,
    /// Date of open interest data.
    #[serde(default, with = "option_date_format")]
    pub open_interest_date: Option<NaiveDate>,
    /// Previous close price.
    #[serde(default, deserialize_with = "deserialize_option_decimal_string")]
    pub close_price: Option<Decimal>,
    /// Date of close price.
    #[serde(default, with = "option_date_format")]
    pub close_price_date: Option<NaiveDate>,
}

impl AlpacaOptionContract {
    /// Get the option kind (Call or Put).
    pub fn kind(&self) -> Option<OptionKind> {
        match self.option_type.as_str() {
            "call" => Some(OptionKind::Call),
            "put" => Some(OptionKind::Put),
            _ => None,
        }
    }

    /// Get the exercise style (American or European).
    pub fn exercise(&self) -> Option<OptionExercise> {
        match self.style.as_str() {
            "american" => Some(OptionExercise::American),
            "european" => Some(OptionExercise::European),
            _ => None,
        }
    }
}

/// API response wrapper for option contracts.
#[derive(Debug, Deserialize)]
struct ContractsResponse {
    option_contracts: Option<Vec<AlpacaOptionContract>>,
    #[serde(default)]
    next_page_token: Option<String>,
}

impl AlpacaOptionsClient {
    /// Fetch option contracts matching the query.
    ///
    /// Automatically paginates through all results up to the safety limit.
    ///
    /// # Arguments
    ///
    /// * `query` - Query parameters for filtering contracts
    ///
    /// # Returns
    ///
    /// Vector of matching option contracts.
    ///
    /// # Errors
    ///
    /// Returns error on network failure, API error, or invalid response.
    pub async fn fetch_contracts(
        &self,
        query: &AlpacaOptionContractQuery,
    ) -> Result<Vec<AlpacaOptionContract>, AlpacaOptionsError> {
        let mut all_contracts = Vec::new();
        let mut page_token: Option<String> = None;
        let mut pages = 0usize;

        loop {
            if pages >= MAX_PAGES {
                debug!(
                    pages,
                    contracts = all_contracts.len(),
                    "reached max pages limit"
                );
                break;
            }
            pages += 1;

            let mut params = query.to_query_params();
            if let Some(ref token) = page_token {
                params.push(("page_token", token.clone()));
            }

            let url = format!("{}/v2/options/contracts", self.broker_base);
            let request = self.http.get(&url).query(&params);

            let response: ContractsResponse = self.request_with_retry(request).await?;

            let contracts = response.option_contracts.unwrap_or_default();
            let count = contracts.len();
            all_contracts.extend(contracts);

            debug!(
                page = pages,
                count,
                total = all_contracts.len(),
                "fetched contracts page"
            );

            match response.next_page_token {
                Some(token) if !token.is_empty() => {
                    page_token = Some(token);
                }
                _ => break,
            }
        }

        Ok(all_contracts)
    }
}

// Custom deserializer for decimal strings
fn deserialize_decimal_string<'de, D>(deserializer: D) -> Result<Decimal, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    Decimal::from_str(&s).map_err(serde::de::Error::custom)
}

fn deserialize_option_decimal_string<'de, D>(deserializer: D) -> Result<Option<Decimal>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let opt: Option<String> = Option::deserialize(deserializer)?;
    match opt {
        Some(s) if !s.is_empty() => Decimal::from_str(&s)
            .map(Some)
            .map_err(serde::de::Error::custom),
        _ => Ok(None),
    }
}

mod date_format {
    use chrono::NaiveDate;
    use serde::{self, Deserialize, Deserializer, Serializer};

    pub(super) const FORMAT: &str = "%Y-%m-%d";

    pub fn serialize<S>(date: &NaiveDate, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&date.format(FORMAT).to_string())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<NaiveDate, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        NaiveDate::parse_from_str(&s, FORMAT).map_err(serde::de::Error::custom)
    }
}

mod option_date_format {
    use chrono::NaiveDate;
    use serde::{self, Deserialize, Deserializer, Serializer};

    use super::date_format::FORMAT;

    pub fn serialize<S>(date: &Option<NaiveDate>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match date {
            Some(d) => serializer.serialize_some(&d.format(FORMAT).to_string()),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<NaiveDate>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let opt: Option<String> = Option::deserialize(deserializer)?;
        match opt {
            Some(s) if !s.is_empty() => NaiveDate::parse_from_str(&s, FORMAT)
                .map(Some)
                .map_err(serde::de::Error::custom),
            _ => Ok(None),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn query_builder_basic() {
        let query = AlpacaOptionContractQuery::new(vec!["AAPL".into()]);
        let params = query.to_query_params();

        assert!(
            params
                .iter()
                .any(|(k, v)| *k == "underlying_symbols" && v == "AAPL")
        );
        assert!(params.iter().any(|(k, v)| *k == "status" && v == "active"));
    }

    #[test]
    fn query_builder_full() {
        let query = AlpacaOptionContractQuery::new(vec!["AAPL".into(), "TSLA".into()])
            .expiration_gte(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap())
            .expiration_lte(NaiveDate::from_ymd_opt(2024, 3, 31).unwrap())
            .strike_gte(dec!(100))
            .strike_lte(dec!(200))
            .call_only()
            .style(OptionExercise::American)
            .limit(500);

        let params = query.to_query_params();

        assert!(
            params
                .iter()
                .any(|(k, v)| *k == "underlying_symbols" && v == "AAPL,TSLA")
        );
        assert!(
            params
                .iter()
                .any(|(k, v)| *k == "expiration_date_gte" && v == "2024-01-01")
        );
        assert!(
            params
                .iter()
                .any(|(k, v)| *k == "expiration_date_lte" && v == "2024-03-31")
        );
        assert!(
            params
                .iter()
                .any(|(k, v)| *k == "strike_price_gte" && v == "100")
        );
        assert!(
            params
                .iter()
                .any(|(k, v)| *k == "strike_price_lte" && v == "200")
        );
        assert!(params.iter().any(|(k, v)| *k == "type" && v == "call"));
        assert!(params.iter().any(|(k, v)| *k == "style" && v == "american"));
        assert!(params.iter().any(|(k, v)| *k == "limit" && v == "500"));
    }

    #[test]
    fn query_bermudan_style_skips_style_param() {
        // Alpaca does not support Bermudan; the builder must omit the `style` filter
        // while preserving `limit` and `status` (regression guard for #65).
        let query =
            AlpacaOptionContractQuery::new(vec!["AAPL".into()]).style(OptionExercise::Bermudan);
        let params = query.to_query_params();

        assert!(!params.iter().any(|(k, _)| *k == "style"));
        assert!(params.iter().any(|(k, _)| *k == "limit"));
        assert!(params.iter().any(|(k, v)| *k == "status" && v == "active"));
    }

    #[test]
    fn query_limit_capped_at_max() {
        let query = AlpacaOptionContractQuery::new(vec!["AAPL".into()]).limit(999_999);
        let params = query.to_query_params();

        let limit = params
            .iter()
            .find(|(k, _)| *k == "limit")
            .map(|(_, v)| v.as_str())
            .unwrap();
        assert_eq!(limit, "10000");
    }

    #[test]
    fn contract_deserialize() {
        let json = r#"{
            "id": "test-id",
            "symbol": "AAPL240119C00150000",
            "name": "AAPL Jan 19 2024 150 Call",
            "status": "active",
            "tradable": true,
            "expiration_date": "2024-01-19",
            "root_symbol": "AAPL",
            "underlying_symbol": "AAPL",
            "underlying_asset_id": "asset-id",
            "type": "call",
            "style": "american",
            "strike_price": "150.00",
            "size": "100",
            "open_interest": "1234",
            "open_interest_date": "2024-01-18",
            "close_price": "5.50",
            "close_price_date": "2024-01-18"
        }"#;

        let contract: AlpacaOptionContract = serde_json::from_str(json).unwrap();

        assert_eq!(contract.symbol, "AAPL240119C00150000");
        assert_eq!(contract.strike_price, dec!(150.00));
        assert_eq!(contract.size, dec!(100));
        assert_eq!(contract.kind(), Some(OptionKind::Call));
        assert_eq!(contract.exercise(), Some(OptionExercise::American));
        assert_eq!(
            contract.expiration_date,
            NaiveDate::from_ymd_opt(2024, 1, 19).unwrap()
        );
    }

    #[test]
    fn contract_deserialize_minimal() {
        let json = r#"{
            "id": "test-id",
            "symbol": "AAPL240119P00150000",
            "name": "AAPL Jan 19 2024 150 Put",
            "status": "active",
            "tradable": true,
            "expiration_date": "2024-01-19",
            "root_symbol": "AAPL",
            "underlying_symbol": "AAPL",
            "underlying_asset_id": "asset-id",
            "type": "put",
            "style": "european",
            "strike_price": "150",
            "size": "100"
        }"#;

        let contract: AlpacaOptionContract = serde_json::from_str(json).unwrap();

        assert_eq!(contract.kind(), Some(OptionKind::Put));
        assert_eq!(contract.exercise(), Some(OptionExercise::European));
        assert!(contract.open_interest.is_none());
        assert!(contract.close_price.is_none());
    }

    #[test]
    fn contracts_response_deserialize() {
        let json = r#"{
            "option_contracts": [
                {
                    "id": "test-id",
                    "symbol": "AAPL240119C00150000",
                    "name": "AAPL Jan 19 2024 150 Call",
                    "status": "active",
                    "tradable": true,
                    "expiration_date": "2024-01-19",
                    "root_symbol": "AAPL",
                    "underlying_symbol": "AAPL",
                    "underlying_asset_id": "asset-id",
                    "type": "call",
                    "style": "american",
                    "strike_price": "150",
                    "size": "100"
                }
            ],
            "next_page_token": "abc123"
        }"#;

        let response: ContractsResponse = serde_json::from_str(json).unwrap();

        assert_eq!(response.option_contracts.unwrap().len(), 1);
        assert_eq!(response.next_page_token, Some("abc123".to_string()));
    }

    #[test]
    fn contracts_response_empty() {
        let json = r#"{}"#;

        let response: ContractsResponse = serde_json::from_str(json).unwrap();

        assert!(response.option_contracts.is_none());
        assert!(response.next_page_token.is_none());
    }
}
