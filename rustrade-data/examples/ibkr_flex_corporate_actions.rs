//! IBKR Flex corporate-action reconciliation example.
//!
//! **UNTESTED in CI** — requires IBKR Flex Web Service credentials.
//!
//! Demonstrates fetching an account's corporate-action records from the IBKR Flex Web Service and
//! sketches how a *wrapper* would reconcile them — the library yields faithful raw records and
//! derives **no** split ratio; ratio derivation/verification and reconcile policy live in the
//! caller. See `rustrade_data::exchange::ibkr::flex` for the full contract.
//!
//! # Prerequisites
//!
//! 1. An IBKR account (paper is fine).
//! 2. A Flex Web Service token and a saved Activity Flex query that includes the Corporate Actions
//!    section (Account Management → Reports → Flex Queries).
//! 3. Environment variables (see `.env.template`):
//!    - `IBKR_FLEX_TOKEN`
//!    - `IBKR_FLEX_QUERY_ID`
//!
//! # Usage
//!
//! ```bash
//! source .env
//! cargo run --example ibkr_flex_corporate_actions --features ibkr
//! ```

// Examples use unwrap/expect for brevity — not production code.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use rustrade_data::exchange::ibkr::{IbkrFlexClient, IbkrFlexCorporateAction, IbkrReorgType};
use tracing::{info, warn};

#[tokio::main]
async fn main() {
    init_logging();

    let client = match IbkrFlexClient::from_env() {
        Ok(client) => client,
        Err(e) => {
            warn!("Could not build Flex client: {e}");
            warn!("Set IBKR_FLEX_TOKEN and IBKR_FLEX_QUERY_ID (see .env.template)");
            return;
        }
    };

    info!("Fetching IBKR Flex corporate-action records (SendRequest → poll → GetStatement)...");

    let actions = match client.fetch_corporate_actions().await {
        Ok(actions) => actions,
        Err(e) => {
            warn!("Flex fetch failed: {e}");
            return;
        }
    };

    info!("Fetched {} corporate-action row(s)", actions.len());

    // The library returns ALL reorg rows faithfully; selecting splits is the caller's job.
    let splits: Vec<&IbkrFlexCorporateAction> = actions
        .iter()
        .filter(|a| {
            matches!(
                a.action_type,
                IbkrReorgType::ForwardSplit | IbkrReorgType::ReverseSplit
            )
        })
        .collect();

    if splits.is_empty() {
        info!("No forward/reverse split rows in this statement (a fresh account often has none).");
    }

    for split in splits {
        reconcile_split_sketch(split);
    }
}

/// Sketch of the **wrapper-side** reconciliation a downstream consumer would perform.
///
/// The library deliberately stops at the raw record. To turn a broker-confirmed split into an
/// engine adjustment, the wrapper:
///
/// 1. Resolves `symbol` → its internal instrument key.
/// 2. Derives the split *ratio* from a market-reference source — e.g. a `StockSplitSource`
///    implementation (Alpaca/Massive) cross-referenced by symbol + date. The library does **not**
///    derive a ratio from the Flex record: `quantity_delta` is an account-scoped delta (not
///    `new/old` shares), `action_description` is unstable free text, and `principal_adjust_factor`
///    is a TIPS field, not a split ratio (it is surfaced but must not be trusted as a ratio).
/// 3. Reconciles the broker-confirmed quantity delta against the engine's own post-split position
///    (the broker is the source of truth) under its own reconcile policy.
fn reconcile_split_sketch(split: &IbkrFlexCorporateAction) {
    let symbol = split.symbol.as_deref().unwrap_or("<unknown>");
    info!(
        "Broker-confirmed {:?} for {symbol}: account share delta {} (report_date {:?})",
        split.action_type, split.quantity_delta, split.report_date,
    );

    // The raw TIPS factor is shown only to illustrate it is surfaced — NOT used as a ratio.
    if let Some(factor) = split.principal_adjust_factor {
        info!(
            "  (principalAdjustFactor={factor} is a raw TIPS field — NOT a split ratio; ignore for reconciliation)"
        );
    }

    info!(
        "  → wrapper next: resolve symbol → instrument key, derive ratio from a StockSplitSource, reconcile."
    );
}

fn init_logging() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::filter::EnvFilter::builder()
                .with_default_directive(tracing_subscriber::filter::LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .with_ansi(cfg!(debug_assertions))
        .init()
}
