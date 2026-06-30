//! Alpaca corporate-actions reference data (`GET /v1beta1/corporate-actions`).
//!
//! The raw REST surface behind the [`StockSplitSource`] adapter (see
//! [`corporate_action`](super::corporate_action)): a [`CorporateActionsQuery`] builder, the nested
//! response types, and the paginated [`AlpacaRestClient::fetch_splits_raw`] stream. Only stock-split
//! actions are requested (`types=forward_split,reverse_split`); each row is normalised into an
//! [`AlpacaStockSplit`].
//!
//! # Date semantics
//!
//! [`AlpacaStockSplit::ex_date`] is the split's **ex-date** — the session on which the share count
//! re-bases and the price adjusts (observed equal to `process_date` across every sampled split).
//! It is the field the [`StockSplitSource`] adapter maps onto the descriptor's `effective_date`.
//! Alpaca also returns `payable_date`, which can fall a trading day *before* `ex_date` for forward
//! splits and therefore must NOT be used as the effective date — see
//! [`corporate_action`](super::corporate_action) for the full rationale.
//!
//! [`StockSplitSource`]: rustrade_integration::corporate_action::StockSplitSource

use super::rest::{AlpacaRestClient, AlpacaRestError};
use async_stream::try_stream;
use chrono::NaiveDate;
use futures::Stream;
use rust_decimal::Decimal;
use serde::Deserialize;
use tracing::{debug, warn};

/// Maximum results per page (Alpaca API limit).
const MAX_LIMIT: u16 = 1000;

/// Maximum pages to fetch before stopping (safety limit against an unbounded `page_token` loop).
const MAX_PAGES: usize = 1000;

/// The `types` filter value — this surface only fetches stock splits, never other action kinds.
const SPLIT_TYPES: &str = "forward_split,reverse_split";

// ============================================================================
// Query Builder
// ============================================================================

/// Query parameters for the `GET /v1beta1/corporate-actions` splits endpoint.
///
/// Construct with [`CorporateActionsQuery::new`] and chain setters. The `types` filter is fixed to
/// `forward_split,reverse_split`; pagination via `page_token` is handled automatically by
/// [`AlpacaRestClient::fetch_splits_raw`].
///
/// # Example
///
/// ```
/// use rustrade_data::exchange::alpaca::CorporateActionsQuery;
/// use chrono::NaiveDate;
///
/// let query = CorporateActionsQuery::new()
///     .symbols(["NVDA", "AAPL"])
///     .start(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap())
///     .end(NaiveDate::from_ymd_opt(2024, 12, 31).unwrap());
/// ```
#[derive(Debug, Default, Clone)]
pub struct CorporateActionsQuery {
    /// Underlying symbols to filter by; empty means no symbol restriction (all symbols in range).
    pub symbols: Vec<String>,
    /// Inclusive lower bound on the action's effective date.
    pub start: Option<NaiveDate>,
    /// Inclusive upper bound on the action's effective date.
    pub end: Option<NaiveDate>,
    /// Results per page (clamped to [`MAX_LIMIT`]).
    pub limit: Option<u16>,
}

impl CorporateActionsQuery {
    /// Create a new empty query (all symbols, no date bounds, default pagination).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Filter by underlying symbols (joined into Alpaca's comma-separated `symbols` parameter).
    #[must_use]
    pub fn symbols<I, S>(mut self, symbols: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.symbols = symbols.into_iter().map(Into::into).collect();
        self
    }

    /// Filter to actions effective on or after `date`.
    #[must_use]
    pub fn start(mut self, date: NaiveDate) -> Self {
        self.start = Some(date);
        self
    }

    /// Filter to actions effective on or before `date`.
    #[must_use]
    pub fn end(mut self, date: NaiveDate) -> Self {
        self.end = Some(date);
        self
    }

    /// Set results per page (clamped to the API maximum of 1000).
    #[must_use]
    pub fn limit(mut self, limit: u16) -> Self {
        self.limit = Some(limit.min(MAX_LIMIT));
        self
    }

    /// Build the query-string parameters (excluding `page_token`, which the stream adds per page).
    fn to_query_params(&self) -> Vec<(&'static str, String)> {
        let mut params = vec![("types", SPLIT_TYPES.to_string())];

        if !self.symbols.is_empty() {
            params.push(("symbols", self.symbols.join(",")));
        }
        if let Some(start) = self.start {
            params.push(("start", start.format("%Y-%m-%d").to_string()));
        }
        if let Some(end) = self.end {
            params.push(("end", end.format("%Y-%m-%d").to_string()));
        }
        let limit = self.limit.unwrap_or(MAX_LIMIT).min(MAX_LIMIT);
        params.push(("limit", limit.to_string()));

        params
    }
}

// ============================================================================
// Response Types
// ============================================================================

/// A normalised Alpaca stock split (forward or reverse).
///
/// The direction is encoded by the ratio: a forward split has `new_rate > old_rate`, a reverse
/// split has `new_rate < old_rate`. These map directly onto the provider-agnostic
/// `CorporateActionKind::stock_split(split_to, split_from)` helper (`new_rate` = `split_to`,
/// `old_rate` = `split_from`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlpacaStockSplit {
    /// Underlying symbol (e.g. `"NVDA"`).
    pub symbol: String,
    /// Ex-date — the market execution date on which the split takes effect. See the module docs.
    pub ex_date: NaiveDate,
    /// Shares *after* the split (the ratio numerator; `split_to`).
    pub new_rate: Decimal,
    /// Shares *before* the split (the ratio denominator; `split_from`).
    pub old_rate: Decimal,
}

/// Top-level response from the corporate-actions endpoint.
#[derive(Debug, Default, Deserialize)]
struct CorporateActionsResponse {
    #[serde(default)]
    corporate_actions: CorporateActions,
    #[serde(default)]
    next_page_token: Option<String>,
}

/// The `corporate_actions` object: split rows partitioned into forward and reverse arrays.
#[derive(Debug, Default, Deserialize)]
struct CorporateActions {
    #[serde(default)]
    forward_splits: Vec<RawSplit>,
    #[serde(default)]
    reverse_splits: Vec<RawSplit>,
}

impl CorporateActions {
    /// Flatten both arrays into normalised splits, forward rows first.
    fn into_stock_splits(self) -> impl Iterator<Item = AlpacaStockSplit> {
        self.forward_splits
            .into_iter()
            .chain(self.reverse_splits)
            .map(RawSplit::into_stock_split)
    }
}

/// A raw split row. Forward and reverse rows share the fields we consume (`symbol`, `new_rate`,
/// `old_rate`, `ex_date`); they differ only in their CUSIP shape (`cusip` vs `new_cusip`/
/// `old_cusip`) and other dates (`payable_date`, `process_date`, …), none of which a split source
/// needs — those are simply ignored, so a single struct deserialises both arrays.
///
/// `new_rate` / `old_rate` arrive as JSON numbers (e.g. `10`, `1`), so they use the float decimal
/// representation — identical to the Massive split client.
#[derive(Debug, Deserialize)]
struct RawSplit {
    symbol: String,
    #[serde(with = "rust_decimal::serde::float")]
    new_rate: Decimal,
    #[serde(with = "rust_decimal::serde::float")]
    old_rate: Decimal,
    ex_date: NaiveDate,
}

impl RawSplit {
    fn into_stock_split(self) -> AlpacaStockSplit {
        AlpacaStockSplit {
            symbol: self.symbol,
            ex_date: self.ex_date,
            new_rate: self.new_rate,
            old_rate: self.old_rate,
        }
    }
}

// ============================================================================
// Client
// ============================================================================

impl AlpacaRestClient {
    /// Fetch stock splits matching `query` from `GET /v1beta1/corporate-actions`.
    ///
    /// Returns a stream that handles `page_token` pagination automatically, yielding each
    /// forward/reverse split as a normalised [`AlpacaStockSplit`]. The stream stops after
    /// [`MAX_PAGES`] pages as a safety bound and logs a warning if that limit is hit.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use rustrade_data::exchange::alpaca::{AlpacaRestClient, CorporateActionsQuery};
    /// use futures::StreamExt;
    ///
    /// let client = AlpacaRestClient::from_env()?;
    /// let query = CorporateActionsQuery::new().symbols(["NVDA"]);
    /// let mut stream = client.fetch_splits_raw(&query);
    /// while let Some(split) = stream.next().await {
    ///     println!("{:?}", split?);
    /// }
    /// ```
    pub fn fetch_splits_raw<'a>(
        &'a self,
        query: &'a CorporateActionsQuery,
    ) -> impl Stream<Item = Result<AlpacaStockSplit, AlpacaRestError>> + 'a {
        try_stream! {
            let url = format!("{}/v1beta1/corporate-actions", self.data_base());
            let mut page_token: Option<String> = None;
            let mut pages = 0usize;

            loop {
                if pages >= MAX_PAGES {
                    warn!(pages, "Alpaca corporate-actions hit the max-pages safety limit; stopping");
                    break;
                }
                pages += 1;

                let mut params = query.to_query_params();
                if let Some(ref token) = page_token {
                    params.push(("page_token", token.clone()));
                }

                debug!(url = %url, page = pages, "Fetching Alpaca corporate-actions page");
                let request = self.get(&url).query(&params);
                let response: CorporateActionsResponse = self.request_with_retry(request).await?;

                let CorporateActionsResponse { corporate_actions, next_page_token } = response;
                for split in corporate_actions.into_stock_splits() {
                    yield split;
                }

                match next_page_token {
                    Some(token) if !token.is_empty() => page_token = Some(token),
                    _ => break,
                }
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
    fn query_empty_sets_only_types_and_limit() {
        let params = CorporateActionsQuery::new().to_query_params();
        assert!(params.contains(&("types", SPLIT_TYPES.to_string())));
        assert!(params.iter().any(|(k, v)| *k == "limit" && v == "1000"));
        assert!(!params.iter().any(|(k, _)| *k == "symbols"));
        assert!(!params.iter().any(|(k, _)| *k == "start" || *k == "end"));
    }

    #[test]
    fn query_joins_symbols_and_dates() {
        let params = CorporateActionsQuery::new()
            .symbols(["NVDA", "AAPL"])
            .start(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap())
            .end(NaiveDate::from_ymd_opt(2024, 12, 31).unwrap())
            .to_query_params();

        assert!(
            params
                .iter()
                .any(|(k, v)| *k == "symbols" && v == "NVDA,AAPL")
        );
        assert!(
            params
                .iter()
                .any(|(k, v)| *k == "start" && v == "2024-01-01")
        );
        assert!(params.iter().any(|(k, v)| *k == "end" && v == "2024-12-31"));
    }

    #[test]
    fn query_limit_clamped_to_max() {
        let params = CorporateActionsQuery::new().limit(5000).to_query_params();
        assert!(params.iter().any(|(k, v)| *k == "limit" && v == "1000"));
    }

    #[test]
    fn parse_forward_split_response() {
        // NVDA 10-for-1 forward split (ex == process == payable here).
        let json = r#"{
            "corporate_actions": {
                "forward_splits": [{
                    "symbol": "NVDA", "cusip": "67066G104",
                    "new_rate": 10, "old_rate": 1,
                    "ex_date": "2024-06-10", "process_date": "2024-06-10",
                    "payable_date": "2024-06-10", "record_date": "2024-06-07",
                    "due_bill_redemption_date": "2024-06-10",
                    "id": "50199fac-0af8-43ef-9846-eaf64c6d322d"
                }]
            },
            "next_page_token": null
        }"#;

        let parsed: CorporateActionsResponse = serde_json::from_str(json).unwrap();
        assert!(parsed.next_page_token.is_none());

        let splits: Vec<_> = parsed.corporate_actions.into_stock_splits().collect();
        assert_eq!(splits.len(), 1);
        assert_eq!(
            splits[0],
            AlpacaStockSplit {
                symbol: "NVDA".to_string(),
                ex_date: NaiveDate::from_ymd_opt(2024, 6, 10).unwrap(),
                new_rate: Decimal::from(10),
                old_rate: Decimal::from(1),
            }
        );
    }

    #[test]
    fn parse_reverse_split_response_with_dual_cusip() {
        // ATRA 1-for-25 reverse split — note the `new_cusip`/`old_cusip` shape (ignored).
        let json = r#"{
            "corporate_actions": {
                "reverse_splits": [{
                    "symbol": "ATRA", "new_rate": 1, "old_rate": 25,
                    "new_cusip": "046513206", "old_cusip": "046513107",
                    "ex_date": "2024-06-20", "process_date": "2024-06-20",
                    "payable_date": "2024-06-20", "record_date": "2024-06-20",
                    "id": "446f18f3-92fc-42a8-8b93-4700f06bc8e0"
                }]
            }
        }"#;

        let parsed: CorporateActionsResponse = serde_json::from_str(json).unwrap();
        let splits: Vec<_> = parsed.corporate_actions.into_stock_splits().collect();
        assert_eq!(splits.len(), 1);
        assert_eq!(splits[0].symbol, "ATRA");
        assert_eq!(splits[0].new_rate, Decimal::from(1));
        assert_eq!(splits[0].old_rate, Decimal::from(25));
        assert_eq!(
            splits[0].ex_date,
            NaiveDate::from_ymd_opt(2024, 6, 20).unwrap()
        );
    }

    #[test]
    fn parse_both_arrays_forward_first_with_page_token() {
        let json = r#"{
            "corporate_actions": {
                "forward_splits": [{
                    "symbol": "NVDA", "new_rate": 10, "old_rate": 1, "ex_date": "2024-06-10"
                }],
                "reverse_splits": [{
                    "symbol": "ATRA", "new_rate": 1, "old_rate": 25, "ex_date": "2024-06-20"
                }]
            },
            "next_page_token": "next123"
        }"#;

        let parsed: CorporateActionsResponse = serde_json::from_str(json).unwrap();
        assert_eq!(parsed.next_page_token.as_deref(), Some("next123"));

        let splits: Vec<_> = parsed.corporate_actions.into_stock_splits().collect();
        // Forward rows come first.
        assert_eq!(splits[0].symbol, "NVDA");
        assert_eq!(splits[1].symbol, "ATRA");
    }

    #[test]
    fn parse_empty_response() {
        let json = r#"{ "corporate_actions": {}, "next_page_token": null }"#;
        let parsed: CorporateActionsResponse = serde_json::from_str(json).unwrap();
        assert_eq!(parsed.corporate_actions.into_stock_splits().count(), 0);
    }

    #[test]
    fn ex_date_provenance_ignores_payable_date() {
        // APH forward split where `payable_date` (2024-06-11) precedes `ex_date` (2024-06-12) by a
        // trading day. Pins that the parsed `ex_date` is the JSON `ex_date` — NOT `payable_date`
        // (which would apply the split a day early; see `corporate_action`'s impl rustdoc).
        let json = r#"{
            "corporate_actions": {
                "forward_splits": [{
                    "symbol": "APH", "new_rate": 2, "old_rate": 1,
                    "ex_date": "2024-06-12", "process_date": "2024-06-12",
                    "payable_date": "2024-06-11", "record_date": "2024-05-31"
                }]
            }
        }"#;

        let parsed: CorporateActionsResponse = serde_json::from_str(json).unwrap();
        let splits: Vec<_> = parsed.corporate_actions.into_stock_splits().collect();
        assert_eq!(splits.len(), 1);
        assert_eq!(
            splits[0].ex_date,
            NaiveDate::from_ymd_opt(2024, 6, 12).unwrap(),
            "ex_date must be the JSON ex_date, never payable_date (2024-06-11)"
        );
    }
}
