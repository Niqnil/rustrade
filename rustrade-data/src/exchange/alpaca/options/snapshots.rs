//! Option chain snapshots with Greeks.
//!
//! Implements `GET /v1beta1/options/snapshots` for fetching option snapshots with Greeks.

use super::{AlpacaOptionsClient, AlpacaOptionsError};
use crate::subscription::greeks::OptionGreeks;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::debug;

/// Maximum symbols per snapshot request (Alpaca API limit).
const MAX_SYMBOLS_PER_REQUEST: usize = 100;

/// Maximum pages to fetch before stopping (safety limit).
const MAX_PAGES: usize = 100;

/// Alpaca options data feed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AlpacaOptionFeed {
    /// Real-time OPRA feed (requires paid subscription).
    Opra,
    /// 15-minute delayed indicative feed (free).
    #[default]
    Indicative,
}

impl AlpacaOptionFeed {
    /// Wire value sent in `feed=` query strings and emitted by the `Serialize` impl.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Opra => "opra",
            Self::Indicative => "indicative",
        }
    }
}

/// Option quote data (bid/ask).
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AlpacaOptionQuote {
    /// Quote timestamp.
    #[serde(rename = "t")]
    pub timestamp: DateTime<Utc>,
    /// Ask exchange code.
    #[serde(rename = "ax")]
    pub ask_exchange: String,
    /// Ask price.
    #[serde(rename = "ap")]
    pub ask_price: Decimal,
    /// Ask size (number of contracts).
    #[serde(rename = "as")]
    pub ask_size: u32,
    /// Bid exchange code.
    #[serde(rename = "bx")]
    pub bid_exchange: String,
    /// Bid price.
    #[serde(rename = "bp")]
    pub bid_price: Decimal,
    /// Bid size (number of contracts).
    #[serde(rename = "bs")]
    pub bid_size: u32,
}

/// Option trade data.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AlpacaOptionTrade {
    /// Trade timestamp.
    #[serde(rename = "t")]
    pub timestamp: DateTime<Utc>,
    /// Exchange code.
    #[serde(rename = "x")]
    pub exchange: String,
    /// Trade price.
    #[serde(rename = "p")]
    pub price: Decimal,
    /// Trade size (number of contracts).
    #[serde(rename = "s")]
    pub size: u32,
}

/// Greeks data from Alpaca API.
#[derive(Debug, Clone, Copy, PartialEq, Default, Deserialize)]
struct AlpacaGreeks {
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

impl From<AlpacaGreeks> for OptionGreeks {
    fn from(g: AlpacaGreeks) -> Self {
        // `rho` is intentionally dropped — `OptionGreeks` does not expose it.
        Self {
            delta: g.delta,
            gamma: g.gamma,
            theta: g.theta,
            vega: g.vega,
            implied_volatility: None,
            theoretical_price: None,
            underlying_price: None,
        }
    }
}

/// Option snapshot with quote and Greeks.
///
/// Contains point-in-time market data for an option contract.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AlpacaOptionSnapshot {
    /// OCC symbol (e.g., "AAPL240119C00150000").
    /// Note: This is populated from the HashMap key, not deserialized from JSON.
    #[serde(default)]
    pub symbol: String,
    /// Latest quote (bid/ask).
    #[serde(default)]
    pub latest_quote: Option<AlpacaOptionQuote>,
    /// Latest trade.
    #[serde(default)]
    pub latest_trade: Option<AlpacaOptionTrade>,
    /// Option Greeks.
    #[serde(default, skip_serializing)]
    greeks: Option<AlpacaGreeks>,
    /// Implied volatility.
    #[serde(default)]
    pub implied_volatility: Option<f64>,
}

impl AlpacaOptionSnapshot {
    /// Get option Greeks in the standard format.
    ///
    /// Returns [`OptionGreeks`](crate::subscription::greeks::OptionGreeks) with delta, gamma, theta, vega populated from the
    /// Alpaca response. Implied volatility is stored separately in [`Self::implied_volatility`].
    pub fn greeks(&self) -> OptionGreeks {
        let mut greeks: OptionGreeks = self.greeks.unwrap_or_default().into();
        greeks.implied_volatility = self.implied_volatility;
        greeks
    }

    /// Check if this snapshot has any Greek data.
    pub fn has_greeks(&self) -> bool {
        self.greeks.is_some() || self.implied_volatility.is_some()
    }
}

/// API response wrapper for snapshots.
#[derive(Debug, Deserialize)]
struct SnapshotsResponse {
    snapshots: Option<HashMap<String, AlpacaOptionSnapshot>>,
    #[serde(default)]
    next_page_token: Option<String>,
}

impl AlpacaOptionsClient {
    /// Fetch option snapshots with Greeks for the given symbols.
    ///
    /// Automatically batches requests if more than 100 symbols are provided,
    /// and paginates through all results.
    ///
    /// # Arguments
    ///
    /// * `symbols` - OCC symbols to fetch (e.g., ["AAPL240119C00150000"])
    /// * `feed` - Data feed to use (OPRA for real-time, Indicative for delayed)
    ///
    /// # Returns
    ///
    /// Vector of option snapshots with quote and Greeks data.
    ///
    /// # Errors
    ///
    /// Returns error on network failure, API error, or invalid response.
    ///
    /// # Note
    ///
    /// Greeks streaming is NOT available via WebSocket. This method provides
    /// point-in-time snapshot data only.
    pub async fn fetch_snapshots(
        &self,
        symbols: &[String],
        feed: AlpacaOptionFeed,
    ) -> Result<Vec<AlpacaOptionSnapshot>, AlpacaOptionsError> {
        if symbols.is_empty() {
            return Ok(Vec::new());
        }

        let mut all_snapshots = Vec::new();

        // Batch symbols into chunks of MAX_SYMBOLS_PER_REQUEST
        for chunk in symbols.chunks(MAX_SYMBOLS_PER_REQUEST) {
            let chunk_snapshots = self.fetch_snapshots_batch(chunk, feed).await?;
            all_snapshots.extend(chunk_snapshots);
        }

        Ok(all_snapshots)
    }

    /// Fetch snapshots for a single batch of symbols (max 100).
    async fn fetch_snapshots_batch(
        &self,
        symbols: &[String],
        feed: AlpacaOptionFeed,
    ) -> Result<Vec<AlpacaOptionSnapshot>, AlpacaOptionsError> {
        let mut all_snapshots = Vec::new();
        let mut page_token: Option<String> = None;
        let mut pages = 0usize;

        let symbols_param = symbols.join(",");

        loop {
            if pages >= MAX_PAGES {
                debug!(
                    pages,
                    snapshots = all_snapshots.len(),
                    "reached max pages limit"
                );
                break;
            }
            pages += 1;

            let mut params: Vec<(&str, &str)> =
                vec![("symbols", &symbols_param), ("feed", feed.as_str())];

            // `token_string` is hoisted to outlive `params` (which borrows it).
            let token_string;
            if let Some(ref token) = page_token {
                token_string = token.clone();
                params.push(("page_token", &token_string));
            }

            let url = format!("{}/v1beta1/options/snapshots", self.data_base);
            let request = self.http.get(&url).query(&params);

            let response: SnapshotsResponse = self.request_with_retry(request).await?;

            if let Some(snapshots_map) = response.snapshots {
                let count = snapshots_map.len();
                all_snapshots.extend(snapshots_map.into_iter().map(|(symbol, mut snapshot)| {
                    snapshot.symbol = symbol;
                    snapshot
                }));

                debug!(
                    page = pages,
                    count,
                    total = all_snapshots.len(),
                    "fetched snapshots page"
                );
            }

            match response.next_page_token {
                Some(token) if !token.is_empty() => {
                    page_token = Some(token);
                }
                _ => break,
            }
        }

        Ok(all_snapshots)
    }

    /// Fetch snapshots for all options of an underlying symbol.
    ///
    /// This is a convenience method that first fetches all contracts for the
    /// underlying, then fetches snapshots for those contracts.
    ///
    /// # Arguments
    ///
    /// * `underlying` - Underlying symbol (e.g., "AAPL")
    /// * `feed` - Data feed to use
    ///
    /// # Returns
    ///
    /// Vector of option snapshots for all active contracts of the underlying.
    pub async fn fetch_chain_snapshots(
        &self,
        underlying: &str,
        feed: AlpacaOptionFeed,
    ) -> Result<Vec<AlpacaOptionSnapshot>, AlpacaOptionsError> {
        use super::AlpacaOptionContractQuery;

        // First, fetch all contracts for this underlying
        let query = AlpacaOptionContractQuery::new(vec![underlying.to_string()]);
        let contracts = self.fetch_contracts(&query).await?;

        if contracts.is_empty() {
            return Ok(Vec::new());
        }

        debug!(
            underlying,
            contracts = contracts.len(),
            "fetching snapshots for chain"
        );

        // Extract symbols and fetch snapshots
        let symbols: Vec<String> = contracts.iter().map(|c| c.symbol.clone()).collect();
        self.fetch_snapshots(&symbols, feed).await
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn feed_as_str() {
        assert_eq!(AlpacaOptionFeed::Opra.as_str(), "opra");
        assert_eq!(AlpacaOptionFeed::Indicative.as_str(), "indicative");
    }

    #[test]
    fn quote_deserialize() {
        let json = r#"{
            "t": "2024-01-15T14:27:51.742904322Z",
            "ax": "C",
            "ap": 2.95,
            "as": 50,
            "bx": "N",
            "bp": 2.85,
            "bs": 75
        }"#;

        let quote: AlpacaOptionQuote = serde_json::from_str(json).unwrap();

        assert_eq!(quote.ask_exchange, "C");
        assert_eq!(quote.ask_price, dec!(2.95));
        assert_eq!(quote.ask_size, 50);
        assert_eq!(quote.bid_exchange, "N");
        assert_eq!(quote.bid_price, dec!(2.85));
        assert_eq!(quote.bid_size, 75);
    }

    #[test]
    fn trade_deserialize() {
        let json = r#"{
            "t": "2024-01-15T14:25:48.889796106Z",
            "x": "N",
            "p": 2.84,
            "s": 100
        }"#;

        let trade: AlpacaOptionTrade = serde_json::from_str(json).unwrap();

        assert_eq!(trade.exchange, "N");
        assert_eq!(trade.price, dec!(2.84));
        assert_eq!(trade.size, 100);
    }

    #[test]
    fn snapshot_deserialize_full() {
        let json = r#"{
            "symbol": "AAPL240119C00150000",
            "latest_quote": {
                "t": "2024-01-15T14:27:51.742904322Z",
                "ax": "C",
                "ap": 2.95,
                "as": 50,
                "bx": "N",
                "bp": 2.85,
                "bs": 75
            },
            "latest_trade": {
                "t": "2024-01-15T14:25:48.889796106Z",
                "x": "N",
                "p": 2.84,
                "s": 100
            },
            "greeks": {
                "delta": 0.6234,
                "gamma": 0.0412,
                "theta": -0.0285,
                "vega": 0.3156,
                "rho": 0.1829
            },
            "implied_volatility": 0.287
        }"#;

        let snapshot: AlpacaOptionSnapshot = serde_json::from_str(json).unwrap();

        assert_eq!(snapshot.symbol, "AAPL240119C00150000");
        assert!(snapshot.latest_quote.is_some());
        assert!(snapshot.latest_trade.is_some());
        assert!(snapshot.has_greeks());

        let greeks = snapshot.greeks();
        assert_eq!(greeks.delta, Some(0.6234));
        assert_eq!(greeks.gamma, Some(0.0412));
        assert_eq!(greeks.theta, Some(-0.0285));
        assert_eq!(greeks.vega, Some(0.3156));
        assert_eq!(greeks.implied_volatility, Some(0.287));
    }

    #[test]
    fn snapshot_deserialize_minimal() {
        let json = r#"{
            "symbol": "AAPL240119C00150000"
        }"#;

        let snapshot: AlpacaOptionSnapshot = serde_json::from_str(json).unwrap();

        assert_eq!(snapshot.symbol, "AAPL240119C00150000");
        assert!(snapshot.latest_quote.is_none());
        assert!(snapshot.latest_trade.is_none());
        assert!(!snapshot.has_greeks());
    }

    #[test]
    fn greeks_conversion() {
        let alpaca_greeks = AlpacaGreeks {
            delta: Some(0.55),
            gamma: Some(0.02),
            theta: Some(-0.05),
            vega: Some(0.15),
            rho: Some(0.10),
        };

        let greeks: OptionGreeks = alpaca_greeks.into();

        assert_eq!(greeks.delta, Some(0.55));
        assert_eq!(greeks.gamma, Some(0.02));
        assert_eq!(greeks.theta, Some(-0.05));
        assert_eq!(greeks.vega, Some(0.15));
        // rho is not in OptionGreeks, so it's dropped
        // implied_volatility comes from snapshot, not greeks struct
        assert!(greeks.implied_volatility.is_none());
    }

    #[test]
    fn snapshots_response_deserialize() {
        let json = r#"{
            "snapshots": {
                "AAPL240119C00150000": {
                    "symbol": "",
                    "implied_volatility": 0.25
                }
            },
            "next_page_token": "abc123"
        }"#;

        let response: SnapshotsResponse = serde_json::from_str(json).unwrap();

        assert!(response.snapshots.is_some());
        assert_eq!(response.snapshots.unwrap().len(), 1);
        assert_eq!(response.next_page_token, Some("abc123".to_string()));
    }

    #[test]
    fn snapshots_response_empty() {
        let json = r#"{}"#;

        let response: SnapshotsResponse = serde_json::from_str(json).unwrap();

        assert!(response.snapshots.is_none());
        assert!(response.next_page_token.is_none());
    }
}
