//! Download minimal DBN fixtures for testing.
//!
//! This example downloads small samples of market data from Databento and saves
//! them as compressed DBN files for use in offline transformer tests.
//!
//! # Usage
//!
//! ```bash
//! # Set API key in .env or environment
//! export DATABENTO_API_KEY=db-xxxxx
//!
//! # Run from rustrade-data directory
//! cargo run --example download_databento_fixtures --features databento
//! ```
//!
//! # Output
//!
//! Creates files in `tests/fixtures/databento/`:
//! - `es_trades_sample.dbn.zst` - ES futures trades (~100-500 records)
//! - `es_quotes_sample.dbn.zst` - ES futures MBP-1 quotes (~100-500 records)
//!
//! These fixtures are committed to the repo so CI can run tests without API access.

use databento::HistoricalClient;
use databento::dbn::decode::DbnMetadata;
use databento::dbn::encode::{DbnEncoder, EncodeRecord};
use databento::dbn::{self, SType, Schema};
use databento::historical::timeseries::GetRangeParams;
use std::fs::File;
use std::path::Path;
use time::macros::datetime;

const FIXTURES_DIR: &str = "tests/fixtures/databento";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create fixtures directory
    std::fs::create_dir_all(FIXTURES_DIR)?;

    let mut client = HistoricalClient::builder().key_from_env()?.build()?;

    // Download ES futures trades - 5 minutes during RTH
    // 2024-06-10 09:30-09:35 CT (14:30-14:35 UTC) - market open, high activity
    download_trades(&mut client).await?;

    // Download MBP-1 quotes - 5 minutes
    download_quotes(&mut client).await?;

    println!("\nDone! Fixtures saved to {FIXTURES_DIR}/");
    Ok(())
}

async fn download_trades(client: &mut HistoricalClient) -> Result<(), Box<dyn std::error::Error>> {
    println!("Fetching ES futures trades (5 min sample)...");

    let params = GetRangeParams::builder()
        .dataset("GLBX.MDP3")
        .symbols(vec!["ESM4"]) // June 2024 ES contract
        .schema(Schema::Trades)
        .stype_in(SType::RawSymbol)
        .date_time_range(datetime!(2024-06-10 14:30 UTC)..datetime!(2024-06-10 14:35 UTC))
        .build();

    let mut decoder = client.timeseries().get_range(&params).await?;

    let output_path = Path::new(FIXTURES_DIR).join("es_trades_sample.dbn.zst");
    let file = File::create(&output_path)?;

    // Create encoder with metadata from decoder, using zstd compression
    let metadata = decoder.metadata().clone();
    let mut encoder = DbnEncoder::with_zstd(file, &metadata)?;

    let mut count = 0u64;
    while let Some(record) = decoder.decode_record::<dbn::TradeMsg>().await? {
        encoder.encode_record(record)?;
        count += 1;
    }

    let size = std::fs::metadata(&output_path)?.len();
    println!(
        "  Saved: {} ({} bytes, {} records)",
        output_path.display(),
        size,
        count
    );

    Ok(())
}

async fn download_quotes(client: &mut HistoricalClient) -> Result<(), Box<dyn std::error::Error>> {
    println!("Fetching ES futures quotes (5 min sample)...");

    let params = GetRangeParams::builder()
        .dataset("GLBX.MDP3")
        .symbols(vec!["ESM4"])
        .schema(Schema::Mbp1)
        .stype_in(SType::RawSymbol)
        .date_time_range(datetime!(2024-06-10 14:30 UTC)..datetime!(2024-06-10 14:35 UTC))
        .build();

    let mut decoder = client.timeseries().get_range(&params).await?;

    let output_path = Path::new(FIXTURES_DIR).join("es_quotes_sample.dbn.zst");
    let file = File::create(&output_path)?;

    let metadata = decoder.metadata().clone();
    let mut encoder = DbnEncoder::with_zstd(file, &metadata)?;

    let mut count = 0u64;
    while let Some(record) = decoder.decode_record::<dbn::Mbp1Msg>().await? {
        encoder.encode_record(record)?;
        count += 1;
    }

    let size = std::fs::metadata(&output_path)?.len();
    println!(
        "  Saved: {} ({} bytes, {} records)",
        output_path.display(),
        size,
        count
    );

    Ok(())
}
