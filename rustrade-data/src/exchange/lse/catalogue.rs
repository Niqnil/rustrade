//! The LSE Data 3.0 catalogue: the markets (`/v3/markets`) and the instruments listed in each
//! (`/v3/pairs`).
//!
//! These are provider-shaped records, not provider-agnostic abstractions: they say what a key may
//! download and how much history each instrument holds, so a caller can choose a symbol and a
//! range before spending rows on it.
//!
//! # ⚠️ Licensing
//! Catalogue contents are provider data and are **not redistributable**. Do not commit catalogue
//! responses as fixtures or example datasets. See the [module documentation](super#licensing).

use crate::exchange::lse::client::LseClient;
use crate::exchange::lse::error::LseError;
use crate::exchange::lse::market::LseMarketCode;
use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

/// One market, as `/v3/markets` lists it for the calling key.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LseMarketInfo {
    /// The provider's code, e.g. `US_STOCKS` or `XNAS`.
    pub code: SmolStr,
    /// The folder name every `/v3` route takes as `market`, e.g. `us_stocks`. See
    /// [`market`](Self::market).
    pub folder: SmolStr,
    /// The market's name.
    pub name: String,
    /// The market's short label.
    #[serde(default)]
    pub label: String,
    /// `listed`, or another status the provider sets.
    #[serde(default)]
    pub status: SmolStr,
    /// Whether the calling key may download this market.
    pub allowed: bool,
    /// The market's time zone, as an IANA name (`America/New_York`, `UTC`).
    pub time_zone: SmolStr,
    /// The data levels the market serves (`L1`, `L2`, `L3`).
    #[serde(default)]
    pub levels: Vec<SmolStr>,
    /// Which selections the market serves.
    pub capabilities: LseCapabilities,
    /// How many instruments `/v3/pairs` lists for it.
    #[serde(default)]
    pub pairs: u64,
    /// The cheapest plan that opens the market, for an exchange market the key cannot download;
    /// `None` otherwise.
    #[serde(default)]
    pub required_plan: Option<LseRequiredPlan>,
}

impl LseMarketInfo {
    /// The market as an [`LseMarketCode`], for passing to a fetch.
    ///
    /// # Errors
    /// [`LseError::UnknownMarket`] for a market the provider added since this integration was
    /// written.
    pub fn market(&self) -> Result<LseMarketCode, LseError> {
        self.folder.parse()
    }
}

/// Which selections a market serves.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct LseCapabilities {
    /// Candles, at every resolution.
    #[serde(default)]
    pub candles: bool,
    /// The trade slice (`timeframe=tick`).
    #[serde(default)]
    pub tick: bool,
    /// Trades (exchange markets).
    #[serde(default)]
    pub trades: bool,
    /// Quotes (exchange markets).
    #[serde(default)]
    pub quotes: bool,
    /// Level 3 orders (exchange markets).
    #[serde(default)]
    pub orders: bool,
    /// The Level 2 book (exchange markets).
    #[serde(default)]
    pub book: bool,
    /// Option Greeks.
    #[serde(default)]
    pub greeks: bool,
}

/// The cheapest plan that opens an exchange market.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LseRequiredPlan {
    /// The plan's name.
    pub name: String,
    /// Its tier.
    #[serde(default)]
    pub tier: Option<String>,
    /// Where to subscribe.
    #[serde(default)]
    pub subscribe_url: Option<String>,
}

/// One instrument, as `/v3/pairs` lists it.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LsePair {
    /// The provider's number for the instrument. Every market data route accepts it in place of
    /// the symbol.
    pub pair_id: u64,
    /// The symbol, spelt as every REST route takes it (`AAPL`, `EUR_USD`, `BTCUSDT`).
    pub symbol: SmolStr,
    /// The instrument's name.
    pub name: String,
    /// The market's folder name.
    pub folder: SmolStr,
    /// Whether the instrument is still listed.
    pub active: bool,
    /// When it was listed and delisted, as the provider writes them, where it does.
    #[serde(default)]
    pub listed_date: Option<String>,
    #[serde(default)]
    pub delisted_date: Option<String>,
    /// Its splits and consolidations, oldest first.
    #[serde(default)]
    pub splits: Vec<LseSplit>,
    /// Rows the provider stores for it (trades, for an option contract), where counted; see
    /// [`tick_count_status`](Self::tick_count_status).
    #[serde(default)]
    pub tick_count: Option<u64>,
    /// `counted`, `not-collected` or `unavailable`.
    #[serde(default)]
    pub tick_count_status: Option<SmolStr>,
    /// Its first and last stored tick, as the provider writes them, where it reports them.
    #[serde(default)]
    pub tick_first: Option<String>,
    #[serde(default)]
    pub tick_last: Option<String>,
}

/// A split or consolidation, as the factors the provider adjusts history by.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LseSplit {
    /// The effective date.
    pub date: NaiveDate,
    /// What prices before it are multiplied by (`0.5` for a two-for-one split).
    pub price_factor: f64,
    /// What volumes before it are multiplied by (`2.0` for a two-for-one split).
    pub volume_factor: f64,
    /// `split` or `consolidation`.
    #[serde(rename = "type")]
    pub kind: SmolStr,
}

/// One page of `/v3/pairs`.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LsePairPage {
    /// How many instruments matched, across every page.
    pub total: u64,
    /// This page's instruments.
    pub pairs: Vec<LsePair>,
}

/// The markets answer.
#[derive(Debug, Deserialize)]
struct MarketsAnswer {
    markets: Vec<LseMarketInfo>,
}

impl LseClient {
    /// Fetch the markets the provider lists, with what the calling key may download.
    ///
    /// Markets the provider lists as coming soon are left out.
    ///
    /// # Errors
    /// See [`LseError`].
    pub async fn fetch_markets(&self) -> Result<Vec<LseMarketInfo>, LseError> {
        let answer: MarketsAnswer = self.get_json("markets", &[]).await?;
        Ok(answer.markets)
    }

    /// Fetch one page of a market's instruments, by symbol.
    ///
    /// * `search` - an exact symbol (`Some(LsePairSearch::Symbol)`), a search over symbols and
    ///   names (`Some(LsePairSearch::Text)`), or `None` for the whole listing.
    /// * `limit` - at most 10,000; `offset` - instruments to skip. An offset past the end is an
    ///   empty page.
    ///
    /// # ⚠️ Not for contract markets
    /// US options and CME Group list their contracts by underlying, paged by a cursor this method
    /// does not follow. List an underlying's option contracts with
    /// [`fetch_option_chain`](Self::fetch_option_chain).
    ///
    /// # Errors
    /// See [`LseError`]; an exact symbol the market does not list is `404 symbol-unknown`.
    pub async fn fetch_pairs(
        &self,
        market: LseMarketCode,
        search: Option<LsePairSearch<'_>>,
        limit: u32,
        offset: u64,
    ) -> Result<LsePairPage, LseError> {
        let mut query = vec![
            ("market", market.as_str().to_owned()),
            ("limit", limit.to_string()),
            ("offset", offset.to_string()),
        ];
        match search {
            Some(LsePairSearch::Symbol(symbol)) => query.push(("symbol", symbol.to_owned())),
            Some(LsePairSearch::Text(text)) => query.push(("q", text.to_owned())),
            None => {}
        }
        self.get_json("pairs", &query).await
    }
}

/// How [`fetch_pairs`](LseClient::fetch_pairs) narrows a listing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LsePairSearch<'a> {
    /// One exact symbol, any case.
    Symbol(&'a str),
    /// A search over symbols and names.
    Text(&'a str),
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;

    // Shapes measured on `/v3` 2026-11-27; every value is invented.

    #[test]
    fn a_market_decodes_and_resolves_to_its_code() {
        let json = r#"{"markets": [{"code": "US_STOCKS", "folder": "us_stocks", "name": "US",
            "label": "US", "status": "listed", "allowed": true, "enabled": true, "derived": false,
            "files": [], "product": "global", "time_zone": "America/New_York", "levels": ["L1"],
            "capabilities": {"candles": true, "tick": true, "trades": false, "quotes": false,
                             "orders": false, "book": false, "greeks": false},
            "pairs": 3, "pairs_folder": "us_stocks", "required_plan": null, "underlyings": null}],
            "coming_soon": [], "plan": "free", "switches": "", "manifest_built_at": 1}"#;
        let markets: MarketsAnswer = serde_json::from_str(json).unwrap();
        let market = &markets.markets[0];

        assert_eq!(market.market().unwrap(), LseMarketCode::UsStocks);
        assert!(market.allowed);
        assert!(market.capabilities.tick && !market.capabilities.quotes);
        assert_eq!(market.required_plan, None);
    }

    #[test]
    fn a_paid_market_names_its_plan() {
        let json = r#"{"code": "XNAS", "folder": "nasdaq", "name": "Nasdaq", "allowed": false,
            "time_zone": "America/New_York", "capabilities": {"book": true},
            "required_plan": {"market": "nasdaq", "name": "Nasdaq Lite", "tier": "lite",
                              "subscribe_url": "https://example.invalid"}}"#;
        let market: LseMarketInfo = serde_json::from_str(json).unwrap();

        assert_eq!(market.required_plan.unwrap().name, "Nasdaq Lite");
        assert!(market.capabilities.book);
    }

    #[test]
    fn a_pair_page_decodes_with_splits_and_null_dates() {
        let json = r#"{"market": "us_stocks", "total": 1, "count": 1, "limit": 1, "offset": 0,
            "sort": "symbol", "sort_basis": "symbol", "pairs": [{"folder": "us_stocks",
            "symbol": "TEST", "name": "Test Inc", "pair_id": 7, "active": true,
            "listed_date": null, "delisted_date": null, "photo": {"light": "x", "dark": "y"},
            "splits": [{"date": "2000-06-21", "price_factor": 0.5, "type": "split",
                        "volume_factor": 2.0}],
            "availability": {"candles": {"available": null, "reason": "unknown"}},
            "tick_count": 12, "tick_count_status": "counted", "tick_count_basis": "stored_rows",
            "tick_count_as_of": "2026-11-27T00:30:00Z", "tick_first": null, "tick_last": null}]}"#;
        let page: LsePairPage = serde_json::from_str(json).unwrap();
        let pair = &page.pairs[0];

        assert_eq!(
            (page.total, pair.pair_id, pair.tick_count),
            (1, 7, Some(12))
        );
        assert_eq!(pair.splits[0].kind, "split");
        assert_eq!(pair.splits[0].volume_factor, 2.0);
        assert_eq!(pair.listed_date, None);
    }
}
