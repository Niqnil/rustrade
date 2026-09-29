#![allow(clippy::unwrap_used, clippy::expect_used)] // Example code: panics acceptable for demonstration

//! Live option prints for chosen contracts, streamed from the London Strategic Edge WebSocket.
//!
//! # ⚠️ Licensing — the data is NOT redistributable
//!
//! This example's **code** is MIT-licensed like the rest of this repository. **The data it
//! retrieves is not.** London Strategic Edge permits use for your own research, trading and model
//! training — including commercially — but **prohibits redistributing, reselling, or otherwise
//! making the data available to third parties**, in bulk or through any competing feed, download
//! service or interface. Terms: <https://londonstrategicedge.com/terms>
//!
//! In practice: do not commit what this prints to a public repository, do not publish it as
//! fixtures or an example dataset, and do not re-serve it.
//!
//! # Running
//!
//! Requires a free API key from <https://londonstrategicedge.com/data>, in `LSE_API_KEY`:
//!
//! ```bash
//! export LSE_API_KEY=...
//! cargo run --example lse_options_stream --features lse
//! ```
//!
//! Prints arrive only during the US options session: 13:30–20:00 UTC in US summer time, 14:30–21:00
//! in winter, weekdays. Outside it the subscription is confirmed and then quiet.
//!
//! Demonstrates:
//! - **Choosing contracts from the REST print tape**, then registering them as ordinary
//!   subscriptions — one per contract, as option instruments.
//! - **One subscription slot per underlying.** The ten contracts below share SPY, so they cost one
//!   of the connection's slots, however many there are.
//! - **The rest of the chain is counted, not delivered.** Every SPY contract ticks on the socket;
//!   only the registered ones reach the stream. After a minute an `info` line reports how many
//!   prints were dropped, and for how many contracts.
//!
//! # Properties worth knowing before you build on this
//!
//! - **Every event is a real print, and there is no quote.** Option ticks carry no bid or ask, so
//!   this dataset serves `PublicTrades` only — and an L1 subscription on it does not compile.
//! - **No greeks on the socket.** They come from the REST print tape only; see the `lse_options`
//!   example.
//! - **Timestamps are whole-second.** Dozens of prints share one; arrival order is the sequence.
//! - **A contract that starts trading mid-session is missed** unless registered up front. The
//!   REST print tape carries every contract.
//! - **One connection per key, shared.** Options stream over the same connection as every other
//!   London Strategic Edge dataset, so contracts can be subscribed alongside equities or crypto by
//!   handing each `subscribe` a clone of one subscriber. A separately built subscriber for the
//!   same key would need a second connection, which a free key is refused.
//! - **No resumption.** A reconnect leaves a gap: the provider replays nothing on this channel.

use chrono::{Datelike, Duration, NaiveTime, Utc, Weekday};
use futures::StreamExt;
use rustrade_data::{
    exchange::lse::{
        LseOptions, live::LseSubscriber, options::LseOptionContract, vault::LseVaultClient,
    },
    streams::{Streams, reconnect::stream::ReconnectingStream},
    subscription::{Subscription, trade::PublicTrades},
};
use rustrade_instrument::instrument::{
    kind::option::OptionExercise,
    market_data::{
        MarketDataInstrument,
        kind::{MarketDataInstrumentKind, MarketDataOptionContract},
    },
};
use std::collections::HashMap;
use tracing::{info, warn};

/// How long to stream for: past the one-minute report of dropped prints.
const RUN_FOR: std::time::Duration = std::time::Duration::from_secs(75);

#[tokio::main]
async fn main() {
    init_logging();

    let vault = LseVaultClient::from_env()
        .expect("set LSE_API_KEY - get a free key at https://londonstrategicedge.com/data");

    let contracts = busiest_spy_contracts(&vault, 10).await;
    for contract in &contracts {
        info!(ticker = %contract.ticker, "registering");
    }

    let streams = Streams::<PublicTrades>::builder()
        .subscribe(
            LseSubscriber::from_env().expect("LSE_API_KEY"),
            contracts.iter().map(|contract| {
                Subscription::<LseOptions, MarketDataInstrument, PublicTrades>::new(
                    LseOptions::default(),
                    instrument(contract),
                    PublicTrades,
                )
            }),
        )
        .init()
        .await
        .expect("subscribe");

    let stream = streams
        .select_all()
        .with_error_handler(|error| warn!(?error, "market stream error"));
    futures::pin_mut!(stream);

    let _ = tokio::time::timeout(RUN_FOR, async {
        while let Some(event) = stream.next().await {
            info!(?event, "option print");
        }
    })
    .await;
}

/// The instrument a caller registers for a contract.
///
/// The connector spells it as the OSI symbol the provider ticks under. The expiry is read as a UTC
/// date, so 20:00 UTC — the US close in summer — names the right one. Exercise style plays no part
/// in the symbol.
fn instrument(contract: &LseOptionContract) -> MarketDataInstrument {
    MarketDataInstrument::from((
        contract.underlying.as_str(),
        "usd",
        MarketDataInstrumentKind::Option(MarketDataOptionContract {
            kind: contract.kind,
            exercise: OptionExercise::American,
            expiry: contract.expiry.and_hms_opt(20, 0, 0).unwrap().and_utc(),
            strike: contract.strike,
        }),
    ))
}

/// The `count` SPY contracts that printed most in the last settled minute, or — with the market
/// shut — in a minute of the previous weekday's session.
async fn busiest_spy_contracts(vault: &LseVaultClient, count: usize) -> Vec<LseOptionContract> {
    // A range ending within a minute of now is refused: the provider can serve it incomplete.
    let end = Utc::now() - Duration::seconds(90);
    let mut prints = vault
        .collect_option_flow(Some("SPY"), end - Duration::minutes(1), end)
        .await
        .expect("option flow");

    if prints.is_empty() {
        info!("no SPY prints in the last settled minute - the market is shut, so expect none live");

        let mut day = Utc::now().date_naive() - Duration::days(1);
        while matches!(day.weekday(), Weekday::Sat | Weekday::Sun) {
            day -= Duration::days(1);
        }
        let start = day
            .and_time(NaiveTime::from_hms_opt(15, 0, 0).unwrap())
            .and_utc();

        prints = vault
            .collect_option_flow(Some("SPY"), start, start + Duration::minutes(1))
            .await
            .expect("option flow");
    }

    let mut per_contract = HashMap::<LseOptionContract, usize>::new();
    for print in prints {
        *per_contract.entry(print.contract).or_default() += 1;
    }

    let mut busiest = per_contract.into_iter().collect::<Vec<_>>();
    busiest.sort_by(|(_, a), (_, b)| b.cmp(a));
    busiest
        .into_iter()
        .take(count)
        .map(|(contract, _)| contract)
        .collect()
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
