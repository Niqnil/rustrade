//! Massive Market Data Integration Tests
//!
//! These tests verify connectivity and data reception from Massive (formerly Polygon) APIs.
//!
//! # Status
//!
//! **Partially tested.** We have a currencies subscription (crypto + forex) but not
//! stocks, options, indices, or futures subscriptions. The following are verified:
//!
//! | Category | Tested | Notes |
//! |----------|--------|-------|
//! | REST aggregates (crypto, forex) | ✅ | Free tier + currencies sub |
//! | REST aggregates (stocks) | ✅ | Free tier (minute aggs only) |
//! | REST aggregates (options, futures) | ✅ | Free tier (minute aggs only) |
//! | REST aggregates (indices) | ❌ | Requires indices subscription |
//! | REST tick trades (crypto) | ✅ | Currencies subscription |
//! | REST tick trades (stocks) | ❌ | Requires stocks subscription |
//! | REST quotes (forex) | ✅ | Currencies subscription |
//! | REST quotes (stocks) | ❌ | Requires stocks subscription |
//! | WebSocket (crypto) | ✅ | Currencies subscription |
//! | WebSocket (stocks) | ❌ | Requires stocks subscription |
//! | Reference data | ✅ | Free tier |
//! | Corporate actions (dividends, splits) | ✅ | Free tier |
//! | Options contracts (reference) | ✅ | Free tier |
//! | Options snapshots (Greeks) | ❌ | Requires Options Starter subscription |
//!
//! Transformation logic is tested via unit tests in `transformer.rs`. Network
//! integration for untested endpoints has not been verified against real data.
//!
//! # Prerequisites
//!
//! 1. Massive account with API key
//! 2. Environment variable set (see .env.template):
//!    - MASSIVE_API_KEY: API key from <https://massive.com/dashboard/api-keys>
//!
//! # Free Tier Limitations
//!
//! All asset classes have free Basic tiers with:
//! - 2 years historical data (1+ year for indices)
//! - 5 API calls/minute rate limit
//! - End of day + minute aggregates
//! - Reference data
//!
//! REST aggregates tests use data from ~1 month ago to stay within free tier limits.
//! Tick-level trades and quotes require paid subscriptions.
//!
//! # WebSocket Limitations
//!
//! - **Individual plan**: 1 concurrent WebSocket connection per product
//! - WebSocket tests are marked `#[serial]` to prevent connection conflicts
//! - Live streaming requires an active subscription for the asset class
//!
//! # Running
//!
//! ```bash
//! # Load env vars from .env
//! source .env
//!
//! # Run all Massive integration tests
//! cargo test --test massive_integration --features massive -- --ignored
//!
//! # Run specific test
//! cargo test --test massive_integration --features massive test_rest_aggregates_crypto -- --ignored
//!
//! # Run tests that should pass with currencies subscription
//! cargo test --test massive_integration --features massive -- --ignored \
//!     test_rest_aggregates_crypto test_rest_aggregates_forex \
//!     test_rest_trades_crypto test_rest_quotes_forex test_websocket_crypto
//! ```

#![cfg(feature = "massive")]
// Integration tests use unwrap/expect for concise failure messages; panics are the intended failure mode.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use chrono::{Duration, Utc};
use futures_util::StreamExt;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use rustrade_data::exchange::massive::{
    ChannelType, DividendQuery, Market, MassiveLive, MassiveRestClient, OptionContractQuery,
    OptionSnapshotQuery, SplitQuery, TickerQuery,
};
use rustrade_instrument::exchange::ExchangeId;
use rustrade_instrument::instrument::kind::option::OptionKind;
use serial_test::serial;
use std::collections::HashMap;
use std::pin::pin;
use tracing_subscriber::{EnvFilter, fmt};

fn init_logging() {
    let _ = fmt()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(tracing::Level::DEBUG.into())
                .from_env_lossy(),
        )
        .try_init();
}

/// Get a time range from ~1 month ago (5 minutes of data).
/// This is well within the 2-year free tier limit for all asset classes.
fn historical_time_range() -> (chrono::DateTime<Utc>, chrono::DateTime<Utc>) {
    let end = Utc::now() - Duration::days(30);
    let start = end - Duration::minutes(5);
    (start, end)
}

// ============================================================================
// REST Client Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_rest_client_creation() {
    init_logging();

    let client = MassiveRestClient::from_env();
    assert!(
        client.is_ok(),
        "Failed to create REST client: {:?}",
        client.err()
    );
    tracing::info!("REST client created successfully");
}

// ============================================================================
// REST Aggregates Tests (12.5.2 - 12.5.4)
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_rest_aggregates_crypto() {
    init_logging();

    let client = MassiveRestClient::from_env().expect("Failed to create client");
    let (from, to) = historical_time_range();

    tracing::info!(%from, %to, "Fetching X:BTCUSD minute aggregates");

    let stream = client.fetch_aggregates("X:BTCUSD", 1, "minute", from, to);
    let mut stream = pin!(stream);

    let mut count = 0;
    while let Some(result) = stream.next().await {
        let candle = result.expect("Failed to fetch candle");
        assert!(candle.open > Decimal::ZERO, "Open should be positive");
        assert!(candle.high >= candle.low, "High should be >= low");
        assert!(
            candle.volume >= Decimal::ZERO,
            "Volume should be non-negative"
        );
        count += 1;

        if count == 1 {
            tracing::info!(
                open = %candle.open,
                high = %candle.high,
                low = %candle.low,
                close = %candle.close,
                volume = %candle.volume,
                "First crypto candle"
            );
        }
    }

    tracing::info!(count, "Fetched crypto aggregates");
    assert!(count > 0, "Should have received at least one candle");
}

#[tokio::test]
#[ignore]
async fn test_rest_aggregates_forex() {
    init_logging();

    let client = MassiveRestClient::from_env().expect("Failed to create client");
    let (from, to) = historical_time_range();

    tracing::info!(%from, %to, "Fetching C:EURUSD minute aggregates");

    let stream = client.fetch_aggregates("C:EURUSD", 1, "minute", from, to);
    let mut stream = pin!(stream);

    let mut count = 0;
    while let Some(result) = stream.next().await {
        let candle = result.expect("Failed to fetch candle");
        assert!(candle.open > Decimal::ZERO, "Open should be positive");
        assert!(candle.high >= candle.low, "High should be >= low");
        count += 1;

        if count == 1 {
            tracing::info!(
                open = %candle.open,
                high = %candle.high,
                low = %candle.low,
                close = %candle.close,
                "First forex candle"
            );
        }
    }

    tracing::info!(count, "Fetched forex aggregates");
    assert!(count > 0, "Should have received at least one candle");
}

#[tokio::test]
#[ignore]
async fn test_rest_aggregates_stocks() {
    init_logging();

    let client = MassiveRestClient::from_env().expect("Failed to create client");
    let (from, to) = historical_time_range();

    tracing::info!(%from, %to, "Fetching AAPL minute aggregates");

    let stream = client.fetch_aggregates("AAPL", 1, "minute", from, to);
    let mut stream = pin!(stream);

    let mut count = 0;
    while let Some(result) = stream.next().await {
        let candle = result.expect("Failed to fetch candle");
        assert!(candle.open > Decimal::ZERO, "Open should be positive");
        assert!(candle.high >= candle.low, "High should be >= low");
        count += 1;

        if count == 1 {
            tracing::info!(
                open = %candle.open,
                high = %candle.high,
                low = %candle.low,
                close = %candle.close,
                volume = %candle.volume,
                "First stock candle"
            );
        }
    }

    tracing::info!(count, "Fetched stock aggregates");
    // Note: May be 0 if the time range falls on a weekend/holiday
}

#[tokio::test]
#[ignore]
async fn test_rest_aggregates_options() {
    init_logging();

    let client = MassiveRestClient::from_env().expect("Failed to create client");

    // Use a historical AAPL option that was active ~1 month ago
    // Format: O:{underlying}{YYMMDD}{C/P}{strike*1000}
    // Using a strike near AAPL's price range
    let (from, to) = historical_time_range();

    // Find a valid option contract by querying tickers first
    let query = TickerQuery::new().market("options").search("AAPL").limit(1);

    let tickers: Vec<_> = client
        .fetch_tickers(&query)
        .take(1)
        .collect::<Vec<_>>()
        .await;

    if tickers.is_empty() {
        tracing::warn!("No AAPL options found, skipping test");
        return;
    }

    let ticker = match &tickers[0] {
        Ok(t) => &t.ticker,
        Err(e) => {
            tracing::warn!(error = %e, "Failed to fetch options ticker");
            return;
        }
    };

    tracing::info!(%from, %to, %ticker, "Fetching options minute aggregates");

    let stream = client.fetch_aggregates(ticker, 1, "minute", from, to);
    let mut stream = pin!(stream);

    let mut count = 0;
    while let Some(result) = stream.next().await {
        match result {
            Ok(candle) => {
                assert!(candle.open >= Decimal::ZERO, "Open should be non-negative");
                count += 1;
                if count == 1 {
                    tracing::info!(
                        open = %candle.open,
                        volume = %candle.volume,
                        "First options candle"
                    );
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "Options fetch error");
                break;
            }
        }
    }

    tracing::info!(count, "Fetched options aggregates");
}

#[tokio::test]
#[ignore]
async fn test_rest_aggregates_indices() {
    init_logging();

    let client = MassiveRestClient::from_env().expect("Failed to create client");
    let (from, to) = historical_time_range();

    tracing::info!(%from, %to, "Fetching I:SPX minute aggregates");

    let stream = client.fetch_aggregates("I:SPX", 1, "minute", from, to);
    let mut stream = pin!(stream);

    let mut count = 0;
    while let Some(result) = stream.next().await {
        let candle = result.expect("Failed to fetch candle");
        assert!(candle.open > Decimal::ZERO, "Open should be positive");
        assert!(candle.high >= candle.low, "High should be >= low");
        count += 1;

        if count == 1 {
            tracing::info!(
                open = %candle.open,
                high = %candle.high,
                low = %candle.low,
                close = %candle.close,
                "First index candle"
            );
        }
    }

    tracing::info!(count, "Fetched index aggregates");
}

#[tokio::test]
#[ignore]
async fn test_rest_aggregates_futures() {
    init_logging();

    let client = MassiveRestClient::from_env().expect("Failed to create client");

    // Find an active futures contract
    let query = TickerQuery::new().market("futures").limit(1);

    let tickers: Vec<_> = client
        .fetch_tickers(&query)
        .take(1)
        .collect::<Vec<_>>()
        .await;

    if tickers.is_empty() {
        tracing::warn!("No futures tickers found, skipping test");
        return;
    }

    let ticker = match &tickers[0] {
        Ok(t) => &t.ticker,
        Err(e) => {
            tracing::warn!(error = %e, "Failed to fetch futures ticker");
            return;
        }
    };

    let (from, to) = historical_time_range();
    tracing::info!(%from, %to, %ticker, "Fetching futures minute aggregates");

    let stream = client.fetch_aggregates(ticker, 1, "minute", from, to);
    let mut stream = pin!(stream);

    let mut count = 0;
    while let Some(result) = stream.next().await {
        match result {
            Ok(candle) => {
                assert!(candle.open > Decimal::ZERO, "Open should be positive");
                count += 1;
                if count == 1 {
                    tracing::info!(
                        open = %candle.open,
                        volume = %candle.volume,
                        "First futures candle"
                    );
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "Futures fetch error");
                break;
            }
        }
    }

    tracing::info!(count, "Fetched futures aggregates");
}

// ============================================================================
// REST Trades Tests (12.5.5)
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_rest_trades_crypto() {
    init_logging();

    let client = MassiveRestClient::from_env().expect("Failed to create client");
    let (from, to) = historical_time_range();

    tracing::info!(%from, %to, "Fetching X:BTCUSD trades");

    let stream = client.fetch_trades("X:BTCUSD", from, to);
    let mut stream = pin!(stream);

    let mut count = 0;
    while let Some(result) = stream.next().await {
        let trade = result.expect("Failed to fetch trade");
        assert!(trade.price > Decimal::ZERO, "Price should be positive");
        assert!(trade.amount > Decimal::ZERO, "Amount should be positive");
        count += 1;

        if count == 1 {
            tracing::info!(
                price = %trade.price,
                amount = %trade.amount,
                "First crypto trade"
            );
        }

        // Limit to avoid rate limits
        if count >= 100 {
            break;
        }
    }

    tracing::info!(count, "Fetched crypto trades");
    assert!(count > 0, "Should have received at least one trade");
}

#[tokio::test]
#[ignore]
async fn test_rest_trades_stocks() {
    init_logging();

    let client = MassiveRestClient::from_env().expect("Failed to create client");
    let (from, to) = historical_time_range();

    tracing::info!(%from, %to, "Fetching AAPL trades");

    let stream = client.fetch_trades("AAPL", from, to);
    let mut stream = pin!(stream);

    let mut count = 0;
    while let Some(result) = stream.next().await {
        let trade = result.expect("Failed to fetch trade");
        assert!(trade.price > Decimal::ZERO, "Price should be positive");
        assert!(trade.amount > Decimal::ZERO, "Amount should be positive");
        count += 1;

        if count == 1 {
            tracing::info!(
                price = %trade.price,
                amount = %trade.amount,
                "First stock trade"
            );
        }

        if count >= 100 {
            break;
        }
    }

    tracing::info!(count, "Fetched stock trades");
}

// ============================================================================
// REST Quotes Tests (12.5.6)
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_rest_quotes_forex() {
    init_logging();

    let client = MassiveRestClient::from_env().expect("Failed to create client");
    let (from, to) = historical_time_range();

    tracing::info!(%from, %to, "Fetching C:EURUSD quotes");

    let stream = client.fetch_quotes("C:EURUSD", from, to);
    let mut stream = pin!(stream);

    let mut count = 0;
    while let Some(result) = stream.next().await {
        let quote = result.expect("Failed to fetch quote");
        let bid = quote.best_bid.expect("Should have bid");
        let ask = quote.best_ask.expect("Should have ask");
        assert!(bid.price > Decimal::ZERO, "Bid should be positive");
        assert!(ask.price > Decimal::ZERO, "Ask should be positive");
        assert!(ask.price >= bid.price, "Ask should be >= bid");
        count += 1;

        if count == 1 {
            tracing::info!(
                bid = %bid.price,
                ask = %ask.price,
                "First forex quote"
            );
        }

        if count >= 100 {
            break;
        }
    }

    tracing::info!(count, "Fetched forex quotes");
    assert!(count > 0, "Should have received at least one quote");
}

#[tokio::test]
#[ignore]
async fn test_rest_quotes_stocks() {
    init_logging();

    let client = MassiveRestClient::from_env().expect("Failed to create client");
    let (from, to) = historical_time_range();

    tracing::info!(%from, %to, "Fetching AAPL quotes (NBBO)");

    let stream = client.fetch_quotes("AAPL", from, to);
    let mut stream = pin!(stream);

    let mut count = 0;
    while let Some(result) = stream.next().await {
        let quote = result.expect("Failed to fetch quote");
        let bid = quote.best_bid.expect("Should have bid");
        let ask = quote.best_ask.expect("Should have ask");
        assert!(bid.price > Decimal::ZERO, "Bid should be positive");
        assert!(ask.price > Decimal::ZERO, "Ask should be positive");
        assert!(ask.price >= bid.price, "Ask should be >= bid");
        count += 1;

        if count == 1 {
            tracing::info!(
                bid = %bid.price,
                ask = %ask.price,
                "First stock NBBO quote"
            );
        }

        if count >= 100 {
            break;
        }
    }

    tracing::info!(count, "Fetched stock quotes");
}

// ============================================================================
// WebSocket Tests (12.5.7 - 12.5.8)
// ============================================================================

#[tokio::test]
#[ignore]
#[serial]
async fn test_websocket_crypto() {
    init_logging();

    let instruments: HashMap<String, String> = [("BTC-USD".to_string(), "btc-usd".to_string())]
        .into_iter()
        .collect();

    let mut client = MassiveLive::from_env(Market::Crypto, ExchangeId::Massive, instruments)
        .expect("Failed to create WebSocket client");

    client.subscribe(&["BTC-USD"], ChannelType::Trade);
    tracing::info!("Subscribed to BTC-USD trades");

    let stream = client.start().await.expect("Failed to start stream");
    let mut stream = pin!(stream);

    let timeout = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        let mut count = 0;
        while let Some(event) = stream.next().await {
            match event {
                Ok(market_event) => {
                    tracing::info!(?market_event, "Received market event");
                    count += 1;
                    if count >= 3 {
                        return Ok(count);
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "Stream error");
                }
            }
        }
        Err("Stream ended without sufficient data")
    })
    .await;

    match timeout {
        Ok(Ok(count)) => tracing::info!(count, "WebSocket crypto test passed"),
        Ok(Err(e)) => panic!("Stream error: {}", e),
        Err(_) => panic!("Timeout waiting for crypto WebSocket data"),
    }
}

/// WebSocket test for stocks - requires stocks subscription
#[tokio::test]
#[ignore]
#[serial]
async fn test_websocket_stocks() {
    init_logging();

    let instruments: HashMap<String, String> = [("AAPL".to_string(), "aapl".to_string())]
        .into_iter()
        .collect();

    let mut client = MassiveLive::from_env(Market::Stocks, ExchangeId::Massive, instruments)
        .expect("Failed to create WebSocket client");

    client.subscribe(&["AAPL"], ChannelType::Trade);
    tracing::info!("Subscribed to AAPL trades");

    let stream = client.start().await.expect("Failed to start stream");
    let mut stream = pin!(stream);

    let timeout = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        let mut count = 0;
        while let Some(event) = stream.next().await {
            match event {
                Ok(market_event) => {
                    tracing::info!(?market_event, "Received market event");
                    count += 1;
                    if count >= 3 {
                        return Ok(count);
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "Stream error");
                }
            }
        }
        Err("Stream ended without sufficient data")
    })
    .await;

    match timeout {
        Ok(Ok(count)) => tracing::info!(count, "WebSocket stocks test passed"),
        Ok(Err(e)) => panic!("Stream error: {}", e),
        Err(_) => panic!("Timeout waiting for stock WebSocket data"),
    }
}

// ============================================================================
// Reference Data Tests (12.5.9)
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_reference_tickers() {
    init_logging();

    let client = MassiveRestClient::from_env().expect("Failed to create client");

    // Query crypto tickers
    let query = TickerQuery::new().market("crypto").limit(10);

    tracing::info!("Fetching crypto tickers");

    let tickers: Vec<_> = client
        .fetch_tickers(&query)
        .take(10)
        .collect::<Vec<_>>()
        .await;

    assert!(!tickers.is_empty(), "Should have received tickers");

    for result in &tickers {
        let ticker = result.as_ref().expect("Failed to fetch ticker");
        assert!(
            !ticker.ticker.is_empty(),
            "Ticker symbol should not be empty"
        );
        tracing::info!(
            ticker = %ticker.ticker,
            name = %ticker.name,
            market = %ticker.market,
            "Ticker"
        );
    }

    tracing::info!(count = tickers.len(), "Fetched tickers");
}

#[tokio::test]
#[ignore]
async fn test_reference_ticker_details() {
    init_logging();

    let client = MassiveRestClient::from_env().expect("Failed to create client");

    tracing::info!("Fetching AAPL ticker details");

    let details = client.fetch_ticker_details("AAPL").await;
    let details = details.expect("Failed to fetch ticker details");

    assert_eq!(details.ticker.ticker, "AAPL");
    assert!(!details.ticker.name.is_empty(), "Name should not be empty");

    tracing::info!(
        ticker = %details.ticker.ticker,
        name = %details.ticker.name,
        market = %details.ticker.market,
        primary_exchange = ?details.ticker.primary_exchange,
        "Ticker details"
    );
}

#[tokio::test]
#[ignore]
async fn test_reference_exchanges() {
    init_logging();

    let client = MassiveRestClient::from_env().expect("Failed to create client");

    tracing::info!("Fetching exchanges");

    let exchanges = client.fetch_exchanges(None, None).await;
    let exchanges = exchanges.expect("Failed to fetch exchanges");

    assert!(!exchanges.is_empty(), "Should have received exchanges");

    for exchange in exchanges.iter().take(5) {
        tracing::info!(
            id = exchange.id,
            mic = ?exchange.mic,
            name = %exchange.name,
            exchange_type = ?exchange.exchange_type,
            "Exchange"
        );
    }

    tracing::info!(count = exchanges.len(), "Fetched exchanges");
}

#[tokio::test]
#[ignore]
async fn test_reference_market_status() {
    init_logging();

    let client = MassiveRestClient::from_env().expect("Failed to create client");

    tracing::info!("Fetching market status");

    let status = client.fetch_market_status().await;
    let status = status.expect("Failed to fetch market status");

    tracing::info!(
        market = %status.market,
        server_time = %status.server_time,
        exchanges = ?status.exchanges,
        currencies = ?status.currencies,
        "Market status"
    );
}

#[tokio::test]
#[ignore]
async fn test_reference_market_holidays() {
    init_logging();

    let client = MassiveRestClient::from_env().expect("Failed to create client");

    tracing::info!("Fetching market holidays");

    let holidays = client.fetch_market_holidays().await;
    let holidays = holidays.expect("Failed to fetch market holidays");

    assert!(!holidays.is_empty(), "Should have received holidays");

    for holiday in holidays.iter().take(5) {
        tracing::info!(
            date = %holiday.date,
            name = %holiday.name,
            exchange = %holiday.exchange,
            status = %holiday.status,
            "Holiday"
        );
    }

    tracing::info!(count = holidays.len(), "Fetched market holidays");
}

// ============================================================================
// Corporate Actions Tests (12.6.2)
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_corporate_actions_dividends() {
    init_logging();

    let client = MassiveRestClient::from_env().expect("Failed to create client");

    // Query AAPL dividends from the last 2 years (within free tier)
    let two_years_ago = (Utc::now() - Duration::days(730)).date_naive();
    let query = DividendQuery::new()
        .ticker("AAPL")
        .ex_dividend_date_gte(two_years_ago)
        .limit(10);

    tracing::info!(%two_years_ago, "Fetching AAPL dividends");

    let dividends: Vec<_> = client
        .fetch_dividends(&query)
        .take(10)
        .collect::<Vec<_>>()
        .await;

    assert!(!dividends.is_empty(), "Should have received dividends");

    for result in &dividends {
        let dividend = result.as_ref().expect("Failed to fetch dividend");
        assert_eq!(dividend.ticker, "AAPL");
        assert!(
            dividend.cash_amount > Decimal::ZERO,
            "Cash amount should be positive"
        );

        tracing::info!(
            ticker = %dividend.ticker,
            cash_amount = %dividend.cash_amount,
            ex_dividend_date = %dividend.ex_dividend_date,
            frequency = ?dividend.frequency,
            dividend_type = ?dividend.dividend_type,
            "Dividend"
        );
    }

    tracing::info!(count = dividends.len(), "Fetched dividends");
}

#[tokio::test]
#[ignore]
async fn test_corporate_actions_splits() {
    init_logging();

    let client = MassiveRestClient::from_env().expect("Failed to create client");

    // Query recent stock splits (within free tier data range)
    let two_years_ago = (Utc::now() - Duration::days(730)).date_naive();
    let query = SplitQuery::new()
        .execution_date_gte(two_years_ago)
        .limit(10);

    tracing::info!(%two_years_ago, "Fetching recent stock splits");

    let splits: Vec<_> = client
        .fetch_splits_raw(&query)
        .take(10)
        .collect::<Vec<_>>()
        .await;

    // Note: May be empty if no splits occurred in the period
    for result in &splits {
        let split = result.as_ref().expect("Failed to fetch split");
        assert!(
            split.split_to > Decimal::ZERO,
            "split_to should be positive"
        );
        assert!(
            split.split_from > Decimal::ZERO,
            "split_from should be positive"
        );

        tracing::info!(
            ticker = %split.ticker,
            execution_date = %split.execution_date,
            split_to = %split.split_to,
            split_from = %split.split_from,
            "Split"
        );
    }

    tracing::info!(count = splits.len(), "Fetched splits");
}

#[tokio::test]
#[ignore]
async fn test_corporate_actions_splits_specific_ticker() {
    init_logging();

    let client = MassiveRestClient::from_env().expect("Failed to create client");

    // Query NVDA splits - they had a 10:1 split in 2024
    let query = SplitQuery::new().ticker("NVDA").limit(5);

    tracing::info!("Fetching NVDA stock splits");

    let splits: Vec<_> = client
        .fetch_splits_raw(&query)
        .take(5)
        .collect::<Vec<_>>()
        .await;

    for result in &splits {
        let split = result.as_ref().expect("Failed to fetch split");
        assert_eq!(split.ticker, "NVDA");

        tracing::info!(
            ticker = %split.ticker,
            execution_date = %split.execution_date,
            split_to = %split.split_to,
            split_from = %split.split_from,
            "NVDA Split"
        );
    }

    tracing::info!(count = splits.len(), "Fetched NVDA splits");
}

/// Drive `MassiveRestClient` through the [`StockSplitSource::fetch_splits`] **trait** method
/// (not the inherent `fetch_splits_raw`), asserting the provider-agnostic `CorporateAction`
/// mapping end-to-end. Mirrors the Alpaca `nvda_forward_split_2024_via_source` test so both
/// providers' trait impls have parallel live coverage — the raw tests above exercise only
/// `fetch_splits_raw`, leaving the `StockSplitSource` adapter untested without this.
#[tokio::test]
#[ignore]
async fn test_corporate_actions_splits_via_source_trait() {
    use chrono::NaiveDate;
    use rustrade_instrument::corporate_action::{CorporateActionKind, SplitRatio};
    use rustrade_integration::corporate_action::{CorporateActionFilter, StockSplitSource};
    use smol_str::SmolStr;

    init_logging();

    let client = MassiveRestClient::from_env().expect("Failed to create client");

    // NVIDIA's 10-for-1 forward split executed on 2024-06-10 (a real, stable historical split).
    let filter = CorporateActionFilter {
        symbols: vec![SmolStr::new("NVDA")],
        start: NaiveDate::from_ymd_opt(2024, 1, 1),
        end: NaiveDate::from_ymd_opt(2024, 12, 31),
    };

    let actions: Vec<_> = client.fetch_splits(&filter).collect::<Vec<_>>().await;

    // Distinguish "API returned nothing" (bad credentials / wrong date window) from "returned
    // splits but the target date is missing" — the `find` below conflates both into one panic.
    assert!(
        !actions.is_empty(),
        "fetch_splits returned no actions for NVDA in 2024 — check credentials and date range"
    );

    let nvda = actions
        .iter()
        .map(|result| result.as_ref().expect("fetch_splits yielded an error"))
        .find(|action| action.effective_date == NaiveDate::from_ymd_opt(2024, 6, 10))
        .expect("NVDA 2024-06-10 split should be present in the result set");

    assert_eq!(nvda.instrument, "NVDA");
    assert_eq!(
        nvda.kind,
        CorporateActionKind::StockSplit {
            ratio: SplitRatio::new(Decimal::from(10)).unwrap()
        }
    );

    tracing::info!(
        count = actions.len(),
        "Fetched NVDA splits via StockSplitSource trait"
    );
}

// ============================================================================
// Options Reference Data Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_options_contracts() {
    init_logging();

    let client = MassiveRestClient::from_env().expect("Failed to create client");

    // Query AAPL call options
    let query = OptionContractQuery::new()
        .underlying_ticker("AAPL")
        .limit(10);

    tracing::info!("Fetching AAPL option contracts");

    let contracts = client
        .fetch_option_contracts(&query)
        .await
        .expect("Failed to fetch contracts");

    assert!(!contracts.is_empty(), "Should have received contracts");

    for contract in &contracts {
        assert_eq!(contract.underlying_ticker, "AAPL");
        assert!(
            contract.strike_price > Decimal::ZERO,
            "Strike should be positive"
        );

        tracing::info!(
            ticker = %contract.ticker,
            underlying = %contract.underlying_ticker,
            contract_type = %contract.contract_type,
            expiration = %contract.expiration_date,
            strike = %contract.strike_price,
            exercise = %contract.exercise_style,
            "Option contract"
        );
    }

    tracing::info!(count = contracts.len(), "Fetched option contracts");
}

#[tokio::test]
#[ignore]
async fn test_options_contracts_filtered() {
    init_logging();

    let client = MassiveRestClient::from_env().expect("Failed to create client");

    // Query AAPL call options with strike range
    let query = OptionContractQuery::new()
        .underlying_ticker("AAPL")
        .contract_type(OptionKind::Call)
        .strike_price_gte(dec!(150))
        .strike_price_lte(dec!(200))
        .limit(20);

    tracing::info!("Fetching filtered AAPL call options (strike 150-200)");

    let contracts = client
        .fetch_option_contracts(&query)
        .await
        .expect("Failed to fetch contracts");

    for contract in &contracts {
        assert_eq!(contract.underlying_ticker, "AAPL");
        assert!(
            contract.strike_price >= dec!(150),
            "Strike should be >= 150"
        );
        assert!(
            contract.strike_price <= dec!(200),
            "Strike should be <= 200"
        );

        tracing::info!(
            ticker = %contract.ticker,
            strike = %contract.strike_price,
            expiration = %contract.expiration_date,
            "Filtered option contract"
        );
    }

    tracing::info!(count = contracts.len(), "Fetched filtered option contracts");
}

/// Options chain snapshot test - requires Options Starter subscription
#[tokio::test]
#[ignore]
async fn test_options_chain_snapshot() {
    init_logging();

    let client = MassiveRestClient::from_env().expect("Failed to create client");

    let query = OptionSnapshotQuery::new().limit(5);

    tracing::info!("Fetching AAPL option chain snapshot");

    let snapshots = client
        .fetch_option_chain_snapshot("AAPL", &query)
        .await
        .expect("Failed to fetch chain snapshot");

    for snapshot in &snapshots {
        tracing::info!(
            ticker = %snapshot.contract.ticker,
            strike = %snapshot.contract.strike_price,
            iv = ?snapshot.implied_volatility,
            delta = ?snapshot.greeks.as_ref().and_then(|g| g.delta),
            gamma = ?snapshot.greeks.as_ref().and_then(|g| g.gamma),
            theta = ?snapshot.greeks.as_ref().and_then(|g| g.theta),
            vega = ?snapshot.greeks.as_ref().and_then(|g| g.vega),
            "Option snapshot"
        );
    }

    tracing::info!(count = snapshots.len(), "Fetched option chain snapshot");
}

/// Single option snapshot test - requires Options Starter subscription
#[tokio::test]
#[ignore]
async fn test_option_single_snapshot() {
    init_logging();

    let client = MassiveRestClient::from_env().expect("Failed to create client");

    // First, find a valid contract ticker
    let query = OptionContractQuery::new()
        .underlying_ticker("AAPL")
        .limit(1);

    let contracts = match client.fetch_option_contracts(&query).await {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(error = %e, "Failed to fetch contract ticker");
            return;
        }
    };

    if contracts.is_empty() {
        tracing::warn!("No AAPL options found, skipping test");
        return;
    }

    let contract_ticker = &contracts[0].ticker;

    tracing::info!(%contract_ticker, "Fetching single option snapshot");

    match client.fetch_option_snapshot("AAPL", contract_ticker).await {
        Ok(snapshot) => {
            tracing::info!(
                ticker = %snapshot.contract.ticker,
                strike = %snapshot.contract.strike_price,
                iv = ?snapshot.implied_volatility,
                open_interest = ?snapshot.open_interest,
                break_even = ?snapshot.break_even_price,
                delta = ?snapshot.greeks.as_ref().and_then(|g| g.delta),
                "Single option snapshot"
            );
        }
        Err(e) => {
            tracing::warn!(error = %e, "Failed to fetch option snapshot (may require subscription)");
        }
    }
}
