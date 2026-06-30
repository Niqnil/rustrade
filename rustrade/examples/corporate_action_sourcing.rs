#![allow(clippy::unwrap_used, clippy::expect_used)] // Example code: panics acceptable for demonstration

//! Source stock splits via [`StockSplitSource`] and turn each into an injectable engine event.
//!
//! This is the **sourcing** half of corporate-action handling — the complement to
//! `examples/corporate_action_injection.rs` (which shows construction + injection given a known
//! split). Here a PULL reference-data source supplies the splits; the example then performs the
//! wrapper's job of mapping each sourced fact into an [`EngineEvent::CorporateAction`].
//!
//! The flow, end to end:
//!   1. build a [`CorporateActionFilter`] (symbols + optional effective-date range);
//!   2. fetch a stream of [`CorporateAction<SmolStr>`] facts from a [`StockSplitSource`];
//!   3. for each fact, perform the four wrapper obligations and construct the engine event:
//!      (a) assign a **unique `id`**, (b) resolve the **ticker → `InstrumentKey`**, (c) supply the
//!      broker **rounding policy**, (d) resolve the **effective date → `effective_time`** via
//!      [`split_effective_instant`];
//!   4. inject it (live → `System.feed_tx`; backtest → the aux-event source) — shown but not
//!      executed; *when/whether* to inject stays a wrapper decision, so this is **not** an
//!      auto-injecting driver.
//!
//! Run it against a real provider (each behind its own feature + credentials):
//! ```bash
//! # Alpaca corporate-actions endpoint (needs ALPACA_API_KEY / ALPACA_SECRET_KEY; free/paper tier):
//! cargo run -p rustrade --features alpaca --example corporate_action_sourcing
//! # Massive /v3/reference/splits (needs MASSIVE_API_KEY):
//! cargo run -p rustrade --features massive --example corporate_action_sourcing
//! ```
//! Without any provider feature/credentials it falls back to a small in-example [`DemoSplitSource`]
//! so the mechanics still run offline. Every path flows through the same trait-generic
//! [`collect_splits`].

use chrono::NaiveDate;
use futures::{Stream, StreamExt};
use rust_decimal_macros::dec;
use rustrade::{EngineEvent, SplitRoundingPolicy};
use rustrade_instrument::{
    corporate_action::{CorporateAction, CorporateActionKind, split_effective_instant},
    instrument::InstrumentIndex,
};
use rustrade_integration::corporate_action::{CorporateActionFilter, StockSplitSource};
use smol_str::SmolStr;

#[tokio::main]
async fn main() {
    // A reference-data query: these symbols, splits effective on/after 2020-01-01.
    let filter = CorporateActionFilter::new(
        vec![SmolStr::new("AAPL"), SmolStr::new("NVDA")],
        NaiveDate::from_ymd_opt(2020, 1, 1),
        None,
    );

    // Source the splits from the first available provider (feature-gated + credentialed), falling
    // back to a canned offline source. Every provider goes through the same generic `collect_splits`.
    let actions = source_actions(&filter).await;

    // For each sourced fact, do the wrapper's job and build the injectable engine event.
    for action in &actions {
        match build_event(action) {
            Some(event) => {
                println!("Built injectable event: {event:?}");
                // Inject: live → `system.feed_tx.send(event)`; backtest → add to an
                // `AuxEventsInMemory` source (see `examples/corporate_action_injection.rs`).
                // Sourcing never injects on its own.
            }
            None => println!(
                "Skipping {} (ticker did not resolve, or no effective date).",
                action.instrument
            ),
        }
    }

    println!("Sourced {} split action(s).", actions.len());
}

/// Pick a [`StockSplitSource`] and drain it. Each real provider is feature-gated and used only when
/// its credentials are present; otherwise the canned [`DemoSplitSource`] keeps the example runnable.
async fn source_actions(filter: &CorporateActionFilter) -> Vec<CorporateAction<SmolStr>> {
    #[cfg(feature = "alpaca")]
    {
        use rustrade_data::exchange::alpaca::AlpacaRestClient;
        if let Ok(client) = AlpacaRestClient::from_env() {
            println!("Sourcing splits from Alpaca…");
            return collect_splits(&client, filter).await;
        }
        println!("ALPACA_API_KEY/ALPACA_SECRET_KEY not set — trying the next source.");
    }

    #[cfg(feature = "massive")]
    {
        use rustrade_data::exchange::massive::MassiveRestClient;
        if let Ok(client) = MassiveRestClient::from_env() {
            println!("Sourcing splits from Massive…");
            return collect_splits(&client, filter).await;
        }
        println!("MASSIVE_API_KEY not set — trying the next source.");
    }

    println!("No provider source available — using canned demo splits.");
    collect_splits(&DemoSplitSource, filter).await
}

/// Drain a [`StockSplitSource`] into a `Vec`, demonstrating trait-generic consumption — the same
/// code drives the Alpaca/Massive client or any other implementor.
async fn collect_splits<S>(
    source: &S,
    filter: &CorporateActionFilter,
) -> Vec<CorporateAction<SmolStr>>
where
    S: StockSplitSource,
    S::Error: std::fmt::Display,
{
    let stream = source.fetch_splits(filter);
    futures::pin_mut!(stream);

    let mut actions = Vec::new();
    while let Some(item) = stream.next().await {
        match item {
            Ok(action) => actions.push(action),
            // Per-item error: report and keep going — a partial result set is still useful.
            Err(error) => eprintln!("corporate-action source error: {error}"),
        }
    }
    actions
}

/// Turn a sourced [`CorporateAction<SmolStr>`] into an injectable [`EngineEvent::CorporateAction`],
/// performing the four wrapper obligations. Returns `None` if the ticker does not resolve to an
/// engine key or the action carries no effective date.
fn build_event(action: &CorporateAction<SmolStr>) -> Option<EngineEvent> {
    // (b) resolve the provider ticker to the engine's instrument key.
    let instrument = resolve_ticker(action.instrument.as_str())?;
    // (d) the effective date is required to stamp the event. A compliant `StockSplitSource` always
    //     populates `effective_date` (per the trait contract), so `None` here means the source
    //     misbehaved — this `?` is a defensive guard, not a routine branch.
    let effective_date = action.effective_date?;

    Some(EngineEvent::CorporateAction {
        // (a) a unique, source-stable id — the sole idempotency key. A same-day correction would be
        //     a second event with a *different* id (a reversal followed by the corrected split).
        id: format!("{}-{}-split", action.instrument, effective_date).into(),
        instrument,
        // The market fact carries straight through, ratio and all.
        kind: action.kind.clone(),
        // (c) the broker rounding policy — a fractional-share broker here.
        policy: SplitRoundingPolicy::Fractional,
        // (d) effective *date* → effective *instant* (midnight UTC; also the backtest merge key).
        effective_time: split_effective_instant(effective_date),
    })
}

/// Stand-in for the wrapper's ticker → `InstrumentKey` registry. A real wrapper looks this up in its
/// indexed instruments; here two symbols map to fixed indices and everything else is unknown.
fn resolve_ticker(ticker: &str) -> Option<InstrumentIndex> {
    match ticker {
        "AAPL" => Some(InstrumentIndex::new(0)),
        "NVDA" => Some(InstrumentIndex::new(1)),
        _ => None,
    }
}

/// A canned, offline [`StockSplitSource`] so the example runs without provider credentials.
struct DemoSplitSource;

impl StockSplitSource for DemoSplitSource {
    type Error = std::convert::Infallible;

    fn fetch_splits(
        &self,
        _filter: &CorporateActionFilter,
    ) -> impl Stream<Item = Result<CorporateAction<SmolStr>, Self::Error>> + Send {
        futures::stream::iter([
            // Apple 4-for-1, 2020-08-31. A real source feeds the provider's `split_to`/`split_from`
            // straight into `stock_split`, which derives the validated ratio in one place.
            Ok(CorporateAction::new(
                SmolStr::new("AAPL"),
                CorporateActionKind::stock_split(dec!(4), dec!(1))
                    .expect("4-for-1 is a valid split"),
                NaiveDate::from_ymd_opt(2020, 8, 31),
            )),
            // NVIDIA 10-for-1, 2024-06-10.
            Ok(CorporateAction::new(
                SmolStr::new("NVDA"),
                CorporateActionKind::stock_split(dec!(10), dec!(1))
                    .expect("10-for-1 is a valid split"),
                NaiveDate::from_ymd_opt(2024, 6, 10),
            )),
        ])
    }
}
