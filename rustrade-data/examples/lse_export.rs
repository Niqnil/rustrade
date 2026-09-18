#![allow(clippy::unwrap_used, clippy::expect_used)] // Example code: panics acceptable for demonstration

//! Bulk export from the London Strategic Edge vault, decoded into market events.
//!
//! # ⚠️ Licensing — the data is NOT redistributable
//!
//! This example's **code** is MIT-licensed like the rest of this repository. **The data it
//! retrieves is not.** London Strategic Edge permits use for your own research, trading and model
//! training — including commercially — but **prohibits redistributing, reselling, or otherwise
//! making the data available to third parties**, in bulk or through any competing feed, download
//! service or interface. Terms: <https://londonstrategicedge.com/terms>
//!
//! This example writes a **file to your disk**, which is trivially easy to commit or share by
//! accident. Do not commit it, do not publish it as a fixture or an example dataset, and do not
//! re-serve it. It is written to a temporary directory here for exactly that reason.
//!
//! # ⚠️ This example spends one of five hourly exports
//!
//! The allowance is **five exports per hour**, shared with streaming, and it is a wall-clock hour
//! bucket that resets at the top of the hour rather than rolling. **A rejected submit still
//! consumes one**, which is why the request is validated before anything is sent.
//!
//! # Running
//!
//! Requires a free API key (no account, no card) from <https://londonstrategicedge.com/data>, in
//! `LSE_API_KEY`:
//!
//! ```bash
//! export LSE_API_KEY=...
//! cargo run --example lse_export --features lse-parquet
//! ```
//!
//! Demonstrates:
//! - **Reaching the raw tick tape**, which no other path serves — neither REST nor the WebSocket.
//! - **Checking `rows` before trusting a `ready` job**, which is the one trap most likely to bite.
//! - **Decoding by columns present**, not by an assumed schema.
//!
//! # Properties worth knowing before you build on this
//!
//! - **Every artifact is single-symbol.** `symbol` is mandatory and `"all"` is a literal that
//!   matches nothing, so there is no multi-symbol spelling. Combining instruments means merging
//!   several files with `merge_time_sorted`.
//! - **A `ready` job with zero rows is not an error.** A request matching nothing returns a valid
//!   Parquet file carrying the complete schema and no rows. Check `rows`.
//! - **The tick schema varies by dataset** — FX is `{ts, symbol, bid, ask}` (a quote tape), while
//!   equities are `{ts, symbol, price, volume}` (a trade tape) — so the decoded `DataKind` differs
//!   per dataset. `price` is the **bid**, which is why `price` beside an `ask` decodes to a quote.
//! - **Range `end` is exclusive**, and the range is date-granular.
//! - **Candles are bid candles for FX, and candle volume is unreliable.** See the module
//!   documentation for `exchange::lse` before backtesting on either.

use chrono::{Duration as ChronoDuration, Utc};
use rustrade_data::event::DataKind;
use rustrade_data::exchange::lse::export::{
    LseExportRange, LseExportRequest, LseExportStatus, LseExportTimeframe,
};
use rustrade_data::exchange::lse::market::{self, LseDataset};
use rustrade_data::exchange::lse::parquet::read_export;
use rustrade_data::exchange::lse::vault::LseVaultClient;
use rustrade_instrument::Underlying;
use rustrade_instrument::asset::Asset;
use rustrade_instrument::index::IndexedInstruments;
use rustrade_instrument::instrument::name::{InstrumentNameExchange, InstrumentNameInternal};
use rustrade_instrument::instrument::quote::InstrumentQuoteAsset;
use rustrade_instrument::instrument::{Instrument, kind::InstrumentKind};
use std::time::Duration;
use tracing::{info, warn};

/// The one symbol this example exports, registers and decodes.
///
/// Named once because all three must agree: `read_export` verifies the symbol on every row against
/// the job it came from, and `instrument_index_for` resolves the same string against the registry.
const SYMBOL: &str = "EUR/USD";

#[tokio::main]
async fn main() {
    init_logging();

    let client = LseVaultClient::from_env()
        .expect("set LSE_API_KEY - get a free key at https://londonstrategicedge.com/data");

    // Check the allowance first: an export is metered, and a rejected submit still spends one.
    let usage = client.usage().await.expect("usage");
    info!(
        exports_remaining_hour = usage.exports_remaining_hour(),
        exports_cap_hour = usage.exports_cap_hour,
        "allowance before submitting"
    );

    // One settled day of tape. `end` is EXCLUSIVE, so this is exactly one day.
    let start = (Utc::now() - ChronoDuration::days(4)).date_naive();
    let range =
        LseExportRange::new(start, start + ChronoDuration::days(1)).expect("a one-day range");

    // Validated before anything is sent: a `400` would cost an export just as a success does. This
    // rejects unknown resolutions, candle resolutions on tick-only datasets, blank symbols, the
    // `"all"` literal, and inverted ranges.
    let request = LseExportRequest::new(LseDataset::Fx, SYMBOL, LseExportTimeframe::Tick, range)
        .expect("a valid export request");

    let job = client
        .submit_export(&request)
        .await
        .expect("submitting the export");
    info!(job_id = %job.job_id, status = %job.status, "export submitted");

    // Poll cadence and deadline are the caller's to choose - the library reports the allowance and
    // takes the timing, rather than baking a retry policy in.
    let status = client
        .await_export(
            &job.job_id,
            Duration::from_secs(3),
            Duration::from_secs(300),
        )
        .await
        .expect("the export job to reach a terminal state");

    assert_eq!(
        status.status,
        LseExportStatus::Ready,
        "export finished in {} ({:?})",
        status.status,
        status.error
    );

    // ⚠️ The trap: `ready` plus a valid file does NOT mean the request matched anything.
    let rows = status.rows.unwrap_or_default();
    if rows == 0 {
        warn!("the export is ready but empty - the request matched no rows");
        return;
    }

    // A temporary directory, deliberately: this file must not be committed. Point it wherever you
    // keep your own research data.
    let directory = tempfile::tempdir().expect("a temporary directory");
    let destination = directory.path().join("eurusd_tick.parquet");

    // Resumes via `Range`, verifies the job's SHA-256, and renames atomically. On a mismatch the
    // destination is left absent, and the partial file is normally kept so a repeated call resumes
    // — unless it turns out to be a stale leftover from a different job at this same destination,
    // which is discarded so the retry can make progress. `IntegrityMismatch` reports which.
    let export = client
        .download_export(&status, &destination, &request)
        .await
        .expect("downloading and verifying the artifact");

    info!(
        path = %export.path().display(),
        rows,
        bytes = ?status.bytes,
        table = ?status.table_name,
        "artifact downloaded and verified"
    );

    // Derived from the registry the engine was built with rather than written as a literal: that
    // makes a fabricated or typo'd index unrepresentable, and `instrument_index_for` additionally
    // rejects a quote-asset disagreement -- a `.L` listing registered in `gbp` rather than `gbx`
    // books pence as pounds, and every notional, fee and PnL downstream is 100x wrong with nothing
    // to object. This example registers the one instrument it decodes; a real system passes the
    // same `IndexedInstruments` it built the engine with.
    let exchange = LseDataset::Fx.exchange_id();
    let name = InstrumentNameExchange::new(SYMBOL);

    // `underlying` splits the display symbol and derives the quote -- the same derivation
    // `instrument_index_for` checks the registration against, so these agree by construction. On a
    // `.L` ticker it yields `gbx`, which is the whole point.
    let underlying = market::underlying(SYMBOL);

    let instruments = IndexedInstruments::new(vec![Instrument::new(
        exchange,
        InstrumentNameInternal::new_from_exchange(exchange, name.clone()),
        name,
        Underlying::new(
            Asset::new_from_exchange(underlying.base),
            Asset::new_from_exchange(underlying.quote),
        ),
        InstrumentQuoteAsset::UnderlyingQuote,
        InstrumentKind::Spot,
        None,
    )]);
    let instrument = market::instrument_index_for(&instruments, exchange, SYMBOL)
        .expect("the symbol to be registered, priced in the asset derived for its venue suffix");

    // Decoding is blocking file I/O, so it runs on a blocking thread rather than on this async
    // one -- `read_export`'s own rustdoc warns against calling it directly from an async context,
    // where it would stall a runtime worker for the whole file.
    //
    // `spawn_blocking` suits a one-shot drain like this. To feed a backtest instead, use
    // `stream_blocking_iter`, which additionally back-pressures the decoder against its consumer;
    // see `engine_backtest_with_lse_candles.rs`.
    let decoded = tokio::task::spawn_blocking(move || {
        // The event type follows the columns present. FX ticks carry `bid` and `ask`, so they
        // decode to an L1 book; an equities tick export would decode to trades from the same call.
        let events = read_export(&export, instrument).expect("the artifact to decode");

        let mut decoded = 0_usize;
        for event in events {
            let event = event.expect("every row to decode");
            decoded += 1;

            if decoded <= 3 {
                match &event.kind {
                    DataKind::OrderBookL1(book) => info!(
                        time = %event.time_exchange,
                        bid = ?book.best_bid.map(|level| level.price),
                        ask = ?book.best_ask.map(|level| level.price),
                        "quote"
                    ),
                    other => info!(time = %event.time_exchange, ?other, "event"),
                }
            }
        }

        decoded
    })
    .await
    .expect("the decode task not to panic");

    info!(decoded, "decode complete");
}

// Initialise an INFO `Subscriber` for `Tracing` Json logs and install it as the global default.
fn init_logging() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::filter::EnvFilter::builder()
                .with_default_directive(tracing_subscriber::filter::LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .with_ansi(cfg!(debug_assertions))
        .json()
        .init()
}
