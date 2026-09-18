#![allow(clippy::unwrap_used, clippy::expect_used)] // Example code: panics acceptable for demonstration

//! Live London Strategic Edge ticks pricing instruments that are executed somewhere else.
//!
//! # ⚠️ Licensing — the data is NOT redistributable
//!
//! This example's **code** is MIT-licensed like the rest of this repository. **The data it
//! retrieves is not.** London Strategic Edge permits use for your own research, trading and model
//! training — including commercially — but **prohibits redistributing, reselling, or otherwise
//! making the data available to third parties**, in bulk or through any competing feed, download
//! service or interface. Terms: <https://londonstrategicedge.com/terms>
//!
//! In practice: do not commit what this consumes to a public repository, do not publish it as
//! fixtures or an example dataset, and do not re-serve it.
//!
//! # Running
//!
//! Requires a free API key (no account, no card) from <https://londonstrategicedge.com/data>, in
//! `LSE_API_KEY`:
//!
//! ```bash
//! export LSE_API_KEY=...
//! cargo run --example engine_sync_with_lse_market_data_and_mock_execution --features lse
//! ```
//!
//! # What this demonstrates
//!
//! 1. **A data venue that is not the execution venue.** This provider publishes prices and offers
//!    no execution at all, so every live use of it is necessarily "priced here, traded there". Each
//!    instrument in the config carries a `data_venue`, which keeps both roles on **one**
//!    `Instrument` — and so on one position and one `pnl_unrealised`. Registering the two roles as
//!    two instruments instead would leave the traded one's PnL permanently stale, because price
//!    updates would land on the other index.
//! 2. **The data venue spells symbols differently, and that is not cosmetic.** The provider quotes
//!    `BTC/USD` where the execution venue lists `BTC-USD`, so the `data_venue` carries its own
//!    `name_exchange`. Subscribing under the execution venue's spelling would silently receive
//!    nothing.
//! 3. **Execution clients are registered only for the venue actually traded.** A venue that only
//!    supplies prices has no account to connect to, and connectivity is derived from the registered
//!    execution clients — so a data-only venue does not hold global connectivity at `Reconnecting`.
//! 4. **Subscriptions are built from the registry, not from literals.** The subscription list below
//!    is derived from `IndexedInstruments`, so each event arrives already tagged with the
//!    `InstrumentIndex` the engine keys its state on. A hand-written index would silently attribute
//!    one symbol's prices to a different instrument.
//!
//! # What a working run looks like
//!
//! Three lines are worth watching for, because they are the whole point of the wiring:
//!
//! ```text
//! EngineState tracking exchange connectivity exchange=coinbase role=ExecutionOnly
//! EngineState tracking exchange connectivity exchange=lse_crypto role=DataOnly
//! EngineState updating global connectivity previous=Reconnecting global=Healthy
//! ```
//!
//! Each venue is tracked in the role it actually plays, and global connectivity reaches `Healthy`
//! without waiting on an account the data venue does not have. The subscription log alongside them
//! names the **data** venue's symbols (`BTC/USD`, `ETH/USD`) against the engine's own instrument
//! indices, which is the `data_venue` `name_exchange` doing its job.
//!
//! # ⚠️ Suitability — the decision price is not the fill price
//!
//! Prices come from an aggregated tape with no venue attribution, and fills come from
//! `MockExchange`. Those are two different markets, and the basis between them is real. That is
//! sound for research and paper trading; before risking capital it is a property to opt into
//! knowingly, because nothing in the library can detect it.
//!
//! # ⚠️ The tick is a QUOTE, not a print
//!
//! The provider publishes one data frame, and its `price` equals its `bid` on every sample taken —
//! 3,966 of 3,966 ticks across every dataset family. This example subscribes to **both** kinds the
//! frame serves, so `DefaultInstrumentMarketData` prices from the top-of-book mid and also records
//! a last-traded price; the trade-shaped events are bid-side quotes, not evidence that a
//! transaction occurred. Both book levels carry a **zero size** — the feed publishes prices only —
//! which prices correctly because the volume-weighted mid falls back to the plain mid, but must not
//! be read as available quantity. See the `rustrade_data::exchange::lse` module documentation.
//!
//! Resumption across a reconnect is available and deliberately **not** enabled here: a resumed
//! subscription is served its whole historical window before a single live tick, and one hour of a
//! busy crypto symbol replays over a hundred thousand ticks. A live engine wants to be current, so
//! it takes the gap. See `LseSubscriber::with_resume` for the other choice.

use futures::Stream;
use rust_decimal::Decimal;
use rustrade::{
    engine::{
        clock::LiveClock,
        state::{
            global::DefaultGlobalData,
            instrument::{data::DefaultInstrumentMarketData, filter::InstrumentFilter},
            trading::TradingState,
        },
    },
    logging::init_logging,
    risk::DefaultRiskManager,
    statistic::time::Daily,
    strategy::DefaultStrategy,
    system::{
        builder::{EngineFeedMode, SystemArgs, SystemBuilder},
        config::SystemConfig,
    },
};
use rustrade_data::{
    error::DataError,
    event::DataKind,
    exchange::lse::{LseCrypto, live::LseSubscriber},
    instrument::MarketInstrumentData,
    streams::{
        Streams,
        consumer::{MarketStreamEvent, MarketStreamResult},
        reconnect::stream::ReconnectingStream,
    },
    subscription::{Subscription, book::OrderBooksL1, trade::PublicTrades},
};
use rustrade_instrument::{
    exchange::ExchangeId, index::IndexedInstruments, instrument::InstrumentIndex,
};
use serde::Deserialize;
use std::{fs::File, io::BufReader, time::Duration};
use tracing::warn;

const CONFIG_PATH: &str = "rustrade/examples/config/lse_live_config.json";

/// The dataset family the config prices its instruments from.
///
/// One `ExchangeId` per dataset family, so this both selects the instruments to subscribe for and
/// names the connector that serves them. An instrument priced by a different family — equities, FX,
/// futures or CFDs — needs its own connector and its own subscription batch, because each family
/// spells symbols its own way.
const DATA_VENUE: ExchangeId = ExchangeId::LseCrypto;

/// How long to let the live system run before shutting down and summarising.
const RUN_DURATION: Duration = Duration::from_secs(30);

#[derive(Deserialize)]
pub struct Config {
    pub risk_free_return: Decimal,
    pub system: SystemConfig,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_logging();

    let Config {
        risk_free_return,
        system: SystemConfig {
            instruments,
            executions,
        },
    } = load_config();

    // Indices are assigned here, in config order, and every market event below is tagged with the
    // index of the instrument it prices rather than with a symbol.
    let instruments = IndexedInstruments::new(instruments);

    let market_stream = init_lse_market_stream(&instruments).await?;

    let args = SystemArgs::new(
        &instruments,
        executions,
        LiveClock,
        DefaultStrategy::default(),
        DefaultRiskManager::default(),
        market_stream,
        DefaultGlobalData,
        // No custom state needed: `DefaultInstrumentMarketData` consumes both kinds this feed
        // serves, pricing from the top-of-book mid and falling back to the last traded price.
        |_| DefaultInstrumentMarketData::default(),
    );

    let system = SystemBuilder::new(args)
        .engine_feed_mode(EngineFeedMode::Iterator)
        .trading_state(TradingState::Disabled)
        .build()?
        .init_with_runtime(tokio::runtime::Handle::current())
        .await?;

    system.trading_state(TradingState::Enabled);

    tokio::time::sleep(RUN_DURATION).await;

    // `DefaultStrategy` and `DefaultRiskManager` are no-ops — this example demonstrates the live
    // market-data wiring, not a strategy — so there is nothing outstanding to cancel or close. Both
    // calls are made anyway, because that is the shutdown order a real system needs.
    system.cancel_orders(InstrumentFilter::None);
    system.close_positions(InstrumentFilter::None);

    let (engine, _shutdown_audit) = system.shutdown().await?;

    engine
        .trading_summary_generator(risk_free_return)
        .generate(Daily)
        .print_summary();

    Ok(())
}

/// Subscribe every registered instrument that this provider prices, for both kinds its tick serves.
///
/// The instrument list is read from the registry rather than written out again, so the two cannot
/// drift: an instrument added to the config is subscribed by adding nothing here.
async fn init_lse_market_stream(
    instruments: &IndexedInstruments,
) -> Result<impl Stream<Item = MarketStreamEvent<InstrumentIndex, DataKind>> + use<>, DataError> {
    // The subscriber holds the credential and performs the authenticating handshake. It is cloned
    // into each connection, and again into every reconnect attempt, so one instance serves all of
    // them.
    let subscriber = LseSubscriber::from_env()
        .expect("set LSE_API_KEY - get a free key at https://londonstrategicedge.com/data");

    // `data_exchange()` falls back to the execution venue when an instrument carries no
    // `DataVenue`, so this reads correctly for both kinds of instrument and selects exactly those
    // this provider prices. `MarketInstrumentData::from` takes the *data* venue's symbol for the
    // same reason.
    let subscribed = || {
        instruments
            .instruments()
            .iter()
            .filter(|keyed| keyed.value.data_exchange().value == DATA_VENUE)
            .map(MarketInstrumentData::from)
    };

    // The instrument type is named explicitly: `Subscription::new` accepts anything convertible
    // into it, and three instrument shapes can address this provider's symbols, so the conversion
    // is ambiguous without it.
    let trades = subscribed()
        .map(|instrument| {
            Subscription::<LseCrypto, MarketInstrumentData<InstrumentIndex>, PublicTrades>::new(
                LseCrypto::default(),
                instrument,
                PublicTrades,
            )
        })
        .collect::<Vec<_>>();

    let quotes = subscribed()
        .map(|instrument| {
            Subscription::<LseCrypto, MarketInstrumentData<InstrumentIndex>, OrderBooksL1>::new(
                LseCrypto::default(),
                instrument,
                OrderBooksL1,
            )
        })
        .collect::<Vec<_>>();

    // One connection per kind, each capped at 16 symbols by the provider. Both decode the same tick
    // frame — the kind decides what it becomes, not what is asked for on the wire.
    let streams: Streams<MarketStreamResult<InstrumentIndex, DataKind>> = Streams::builder_multi()
        .add(Streams::<PublicTrades>::builder().subscribe(subscriber.clone(), trades))
        .add(Streams::<OrderBooksL1>::builder().subscribe(subscriber, quotes))
        .init()
        .await?;

    Ok(streams
        .select_all()
        .with_error_handler(|error| warn!(?error, "MarketStream generated error")))
}

pub fn load_config() -> Config {
    let file = File::open(CONFIG_PATH).expect("Failed to open config file");
    let reader = BufReader::new(file);
    serde_json::from_reader(reader).expect("Failed to parse config file")
}
