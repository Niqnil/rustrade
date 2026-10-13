//! Live, read-only check of `BinanceMargin`'s shortability and borrow-capacity queries.
//!
//! Binance margin has no testnet, so this runs against production. It places no orders and moves
//! nothing: it reads a pair's margin listing, its base asset, the base asset's next hourly interest
//! rate and the account's max borrowable amounts. The reads cost about 250 of Binance's per-IP
//! request weight in all, of the 12000 a minute allows.
//!
//! The variables are named apart from `BINANCE_API_KEY`, which the other Binance tests read as a
//! testnet key:
//!
//! - `BINANCE_MARGIN_API_KEY`, `BINANCE_MARGIN_SECRET_KEY`: a production key that may read margin
//!   data. It needs no trading permission.
//! - `BINANCE_MARGIN_SYMBOL`: the pair, such as `BTCUSDT`.
//! - `BINANCE_MARGIN_ISOLATED`: `true` to ask the pair's isolated margin account, which must exist;
//!   otherwise cross margin.
//!
//! ```bash
//! cargo test -p rustrade-execution --features binance --test binance_margin_borrow -- --ignored --nocapture
//! ```
#![allow(clippy::unwrap_used, clippy::expect_used)] // Live check: a panic is the report.

use rust_decimal::Decimal;
use rustrade_execution::client::{
    BorrowCapacityClient, ExecutionClient, ShortabilityClient,
    binance::{BinanceMargin, BinanceMarginConfig},
};
use rustrade_instrument::{Side, instrument::name::InstrumentNameExchange};

fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{name} env var required"))
}

#[tokio::test]
#[ignore]
async fn margin_borrow_terms_and_capacity_read_back() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("rustrade_execution=info")
        .try_init();
    let instrument = InstrumentNameExchange::new(env("BINANCE_MARGIN_SYMBOL"));
    let is_isolated = std::env::var("BINANCE_MARGIN_ISOLATED").is_ok_and(|value| value == "true");
    let (api_key, secret_key) = (
        env("BINANCE_MARGIN_API_KEY"),
        env("BINANCE_MARGIN_SECRET_KEY"),
    );
    let config = if is_isolated {
        BinanceMarginConfig::isolated(api_key, secret_key, vec![instrument.clone()])
    } else {
        BinanceMarginConfig::cross_margin(api_key, secret_key)
    };
    let client = BinanceMargin::new(config);
    println!("{instrument}, isolated: {is_isolated}");

    let shortability = client
        .fetch_shortability(&instrument)
        .await
        .expect("fetch_shortability");
    println!("shortability: {shortability:?}");
    assert!(
        shortability.shortable.is_some(),
        "a listed pair's flags are known"
    );
    let fee_rate = shortability
        .fee_rate
        .expect("Binance reports a rate for the base asset");
    assert!(
        fee_rate > Decimal::ZERO && fee_rate < Decimal::ONE,
        "an annual rate as a fraction, not a percentage: {fee_rate}"
    );

    for side in [Side::Sell, Side::Buy] {
        let capacity = client
            .fetch_borrow_capacity(&instrument, side)
            .await
            .expect("fetch_borrow_capacity");
        println!(
            "{side:?} borrows {}: borrowable now is_some={} account limit is_some={}, now <= limit: {:?}",
            capacity.asset,
            capacity.borrowable_now.is_some(),
            capacity.account_limit.is_some(),
            capacity
                .borrowable_now
                .zip(capacity.account_limit)
                .map(|(now, limit)| now <= limit),
        );
    }
}
