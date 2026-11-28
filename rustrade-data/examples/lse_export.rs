#![allow(clippy::unwrap_used, clippy::expect_used)] // Example code: panics acceptable for demonstration

//! Download one LSE Data 3.0 export to disk, verified, and decode it into market events.
//!
//! # ⚠️ Licensing — the data is NOT redistributable
//!
//! This example's **code** is MIT-licensed like the rest of this repository. **The data it
//! retrieves is not.** London Strategic Edge's licence allows an account's data for its holder's
//! own personal use: their own research, trading and model training, and sharing charts and
//! results they produce. Supplying, showing, publishing or otherwise making the data available to
//! anyone else, in any form, or running a product or service on it, needs the provider's licence,
//! and so does use by or for an organisation.
//!
//! - Licence: <https://londonstrategicedge.com/licence>
//! - Terms: <https://londonstrategicedge.com/terms>
//!
//! This example writes the file to a temporary directory that is deleted when it exits. If you
//! keep a download, keep it out of any repository and anything you share.
//!
//! # Running
//!
//! ```bash
//! export LSE_API_KEY=...   # a free key comes with a free account
//! cargo run --example lse_export --features lse-parquet
//! ```

use chrono::{Datelike, Duration, NaiveTime, Utc, Weekday};
use rustrade_data::event::DataKind;
use rustrade_data::exchange::lse::client::LseClient;
use rustrade_data::exchange::lse::export::{LseExistingFile, LseExportData, LseExportRequest};
use rustrade_data::exchange::lse::historical::LseWindow;
use rustrade_data::exchange::lse::market::LseMarketCode;
use rustrade_data::exchange::lse::parquet::read_export;
use rustrade_data::subscription::candle::CandleInterval;
use rustrade_instrument::{exchange::ExchangeId, instrument::InstrumentIndex};

#[tokio::main]
async fn main() {
    let client = LseClient::from_env().expect("set LSE_API_KEY");

    // An hour of the last weekday's US regular session, two days back so it is fully loaded.
    let mut day = Utc::now().date_naive() - Duration::days(2);
    while matches!(day.weekday(), Weekday::Sat | Weekday::Sun) {
        day -= Duration::days(1);
    }
    let from = day
        .and_time(NaiveTime::from_hms_opt(15, 0, 0).unwrap())
        .and_utc();
    let data = LseExportData::Candles(CandleInterval::Min1);
    let request = LseExportRequest::new(
        LseMarketCode::UsStocks,
        "AAPL",
        data,
        LseWindow::Times {
            from,
            to: from + Duration::hours(1),
        },
    );

    let directory = tempfile::tempdir().unwrap();
    let destination = directory.path().join(request.file_name());
    let Some(file) = client
        .download_export(&request, &destination, LseExistingFile::Refuse)
        .await
        .expect("download failed")
    else {
        println!("nothing stored for {}", request.file_name());
        return;
    };
    println!(
        "downloaded {} ({} bytes, SHA-256 {})",
        file.path.display(),
        file.info.bytes,
        file.info.sha256
    );

    // Decoding blocks; a backtest drives it with `stream_blocking_iter` (see `read_export`).
    // The instrument index here is illustrative: derive it from your registry in real use.
    let events = read_export(
        &file.path,
        data,
        ExchangeId::LseEquities,
        InstrumentIndex(0),
    )
    .expect("not a candle file");
    for event in events.take(5) {
        let event = event.expect("decode failed");
        if let DataKind::Candle(candle) = event.kind {
            println!(
                "{}  O {} H {} L {} C {}  V {:?}",
                candle.close_time,
                candle.open,
                candle.high,
                candle.low,
                candle.close,
                candle.volume
            );
        }
    }
}
