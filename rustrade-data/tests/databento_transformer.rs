//! Offline tests for Databento DBN → rustrade event transformation.
//!
//! Uses pre-downloaded DBN fixtures to test transformation logic without API calls.
//! These tests run in CI.

use rustrade_data::exchange::databento::{load_quotes_from_dbn, load_trades_from_dbn};
use rustrade_instrument::exchange::ExchangeId;
use std::path::Path;

const FIXTURES_DIR: &str = "tests/fixtures/databento";

#[test]
fn test_load_trades_from_dbn_fixture() {
    let path = Path::new(FIXTURES_DIR).join("es_trades_sample.dbn.zst");

    if !path.exists() {
        eprintln!(
            "Skipping test: fixture not found at {}. Run download_databento_fixtures example.",
            path.display()
        );
        return;
    }

    let trades: Vec<_> = load_trades_from_dbn(&path, ExchangeId::DatabentoGlbx, "ESM4")
        .expect("Failed to open DBN file")
        .collect();

    // Should have loaded records
    assert!(!trades.is_empty(), "Expected at least one trade record");

    // Count successes and failures
    let (successes, failures): (Vec<_>, Vec<_>) = trades.into_iter().partition(|r| r.is_ok());

    println!(
        "Loaded {} trades successfully, {} failed",
        successes.len(),
        failures.len()
    );

    assert!(
        !successes.is_empty(),
        "Expected at least one valid trade record"
    );

    // Verify first trade has valid fields
    let first_trade = successes.into_iter().next().unwrap().unwrap();

    assert_eq!(first_trade.exchange, ExchangeId::DatabentoGlbx);
    assert_eq!(first_trade.instrument, "ESM4");
    assert!(first_trade.kind.price > 0.0, "Price should be positive");
    assert!(first_trade.kind.amount > 0.0, "Amount should be positive");

    // ES futures trade around June 2024 should be in 5000-6000 range
    assert!(
        first_trade.kind.price > 1000.0 && first_trade.kind.price < 10000.0,
        "ES price {} outside expected range",
        first_trade.kind.price
    );
}

#[test]
fn test_load_quotes_from_dbn_fixture() {
    let path = Path::new(FIXTURES_DIR).join("es_quotes_sample.dbn.zst");

    if !path.exists() {
        eprintln!(
            "Skipping test: fixture not found at {}. Run download_databento_fixtures example.",
            path.display()
        );
        return;
    }

    let quotes: Vec<_> = load_quotes_from_dbn(&path, ExchangeId::DatabentoGlbx, "ESM4")
        .expect("Failed to open DBN file")
        .collect();

    assert!(!quotes.is_empty(), "Expected at least one quote record");

    let (successes, failures): (Vec<_>, Vec<_>) = quotes.into_iter().partition(|r| r.is_ok());

    println!(
        "Loaded {} quotes successfully, {} failed",
        successes.len(),
        failures.len()
    );

    assert!(
        !successes.is_empty(),
        "Expected at least one valid quote record"
    );

    // Verify first quote has valid fields
    let first_quote = successes.into_iter().next().unwrap().unwrap();

    assert_eq!(first_quote.exchange, ExchangeId::DatabentoGlbx);
    assert_eq!(first_quote.instrument, "ESM4");

    let quote = &first_quote.kind;
    assert!(quote.bid_price > 0.0, "Bid price should be positive");
    assert!(quote.ask_price > 0.0, "Ask price should be positive");
    assert!(
        quote.ask_price >= quote.bid_price,
        "Ask should be >= bid, got bid={} ask={}",
        quote.bid_price,
        quote.ask_price
    );

    // ES futures around June 2024
    assert!(
        quote.bid_price > 1000.0 && quote.bid_price < 10000.0,
        "ES bid {} outside expected range",
        quote.bid_price
    );
}

#[test]
fn test_trade_timestamp_ordering() {
    let path = Path::new(FIXTURES_DIR).join("es_trades_sample.dbn.zst");

    if !path.exists() {
        return;
    }

    let trades: Vec<_> = load_trades_from_dbn(&path, ExchangeId::DatabentoGlbx, "ESM4")
        .expect("Failed to open DBN file")
        .filter_map(|r| r.ok())
        .take(100) // Check first 100
        .collect();

    // Verify timestamps are monotonically increasing (or equal)
    for window in trades.windows(2) {
        assert!(
            window[1].time_exchange >= window[0].time_exchange,
            "Timestamps should be monotonically increasing"
        );
    }
}

#[test]
fn test_quote_spread_is_reasonable() {
    let path = Path::new(FIXTURES_DIR).join("es_quotes_sample.dbn.zst");

    if !path.exists() {
        return;
    }

    let quotes: Vec<_> = load_quotes_from_dbn(&path, ExchangeId::DatabentoGlbx, "ESM4")
        .expect("Failed to open DBN file")
        .filter_map(|r| r.ok())
        .take(100)
        .collect();

    for quote in &quotes {
        let spread = quote.kind.ask_price - quote.kind.bid_price;
        // ES spread should be small (typically 0.25 = 1 tick, sometimes 0.50)
        assert!(
            (0.0..10.0).contains(&spread),
            "Spread {} is unreasonable for ES futures",
            spread
        );
    }
}
