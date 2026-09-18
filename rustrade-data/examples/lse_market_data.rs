#![allow(clippy::unwrap_used, clippy::expect_used)] // Example code: panics acceptable for demonstration

//! Live ticks from the London Strategic Edge WebSocket.
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
//! Requires a free API key (no account, no card) from <https://londonstrategicedge.com/data>, in
//! `LSE_API_KEY`:
//!
//! ```bash
//! export LSE_API_KEY=...
//! cargo run --example lse_market_data --features lse
//! ```
//!
//! Demonstrates:
//! - **One frame serving two subscription kinds.** The provider publishes a single data frame
//!   carrying a price, a bid, an ask and a size, so the same tick decodes as a `PublicTrade` or as
//!   an `OrderBookL1` depending only on which kind was subscribed.
//! - **Per-dataset provenance.** Each dataset family is its own connector, so `MarketEvent.exchange`
//!   says which one an event came from — worth having, because two of the five fabricate `volume`.
//! - **Opt-in resumption across a reconnect**, with one state shared across every stream.
//!
//! # Properties worth knowing before you build on this
//!
//! - **The tick is a QUOTE, not a print.** Its `price` equals its `bid` on every sample taken —
//!   3,966 of 3,966 ticks across every dataset family. A `PublicTrade` decoded from it is a
//!   bid-side quote wearing a trade's shape, and its arrival is not evidence that a transaction
//!   occurred.
//! - **`volume` is real on two venues and fabricated on two others**, with no in-band signal
//!   separating them. `LseCrypto` and `LseEquities` carry a genuine per-tick size that reconciles
//!   exactly against the provider's own one-minute candles. **`LseFx` and `LseCfd` carry a
//!   hard-coded `1.0`** — a placeholder that aggregates into a legitimate-looking total, so
//!   volume-weighted prices and size filters there are meaningless rather than imprecise. Watch the
//!   `EUR/USD` line below print `1` forever while `BTC/USD` varies.
//! - **Identical consecutive ticks are genuine and are never de-duplicated.** Barely a third of a
//!   sampled run was unique on `(ts, price, bid, ask, volume)`, yet removing the repeats destroyed
//!   volume that otherwise reconciles exactly. Do not add a filter.
//! - **Both book levels carry a zero size.** The feed publishes bid and ask *prices* only.
//! - **Each `subscribe` call opens its own connection, and a connection accepts 16 symbols.** A
//!   batch that exceeds the cap, or that names a symbol the key cannot subscribe to, is rejected
//!   before anything reaches the wire — so a typo costs no subscription slot and never presents as
//!   a symbol that is confirmed and then silently never ticks.

use futures::StreamExt;
use rustrade_data::{
    event::DataKind,
    exchange::lse::{LseCrypto, LseFx, live::LseSubscriber, resume::LseResumeState},
    streams::{
        Streams,
        consumer::MarketStreamResult,
        reconnect::{Event, stream::ReconnectingStream},
    },
    subscription::{book::OrderBooksL1, trade::PublicTrades},
};
use rustrade_instrument::instrument::market_data::{
    MarketDataInstrument, kind::MarketDataInstrumentKind,
};
use std::sync::Arc;
use tracing::{info, warn};

#[tokio::main]
async fn main() {
    init_logging();

    let subscriber = LseSubscriber::from_env()
        .expect("set LSE_API_KEY - get a free key at https://londonstrategicedge.com/data");

    // Resumption is opt-in, and one state serves every stream below — including the top-of-book
    // connection, which carries the *same* crypto symbols as the crypto trade connection.
    // Watermarks are filed per subscription *and* kind, so those two advance independently and
    // neither can set the other's resume point; nothing about sharing needs thinking about here.
    //
    // On a first connection there is nothing to resume from, so nothing replays here. The state
    // matters after a drop: the reconnect re-subscribes from the last event each stream actually
    // delivered instead of leaving a gap.
    let resume = Arc::new(LseResumeState::new());

    let streams: Streams<MarketStreamResult<MarketDataInstrument, DataKind>> =
        Streams::builder_multi()
            .add(
                Streams::<PublicTrades>::builder()
                    .subscribe(
                        subscriber.clone().with_resume(Arc::clone(&resume)),
                        [
                            (
                                LseCrypto::default(),
                                "btc",
                                "usd",
                                MarketDataInstrumentKind::Spot,
                                PublicTrades,
                            ),
                            (
                                LseCrypto::default(),
                                "eth",
                                "usd",
                                MarketDataInstrumentKind::Spot,
                                PublicTrades,
                            ),
                        ],
                    )
                    // A second dataset family, on its own connection: same frame, same decoder,
                    // different `MarketEvent.exchange` — and a `volume` the provider invents.
                    .subscribe(
                        subscriber.clone().with_resume(Arc::clone(&resume)),
                        [(
                            LseFx::default(),
                            "eur",
                            "usd",
                            MarketDataInstrumentKind::Spot,
                            PublicTrades,
                        )],
                    ),
            )
            .add(Streams::<OrderBooksL1>::builder().subscribe(
                subscriber.with_resume(resume),
                [
                    (
                        LseCrypto::default(),
                        "btc",
                        "usd",
                        MarketDataInstrumentKind::Spot,
                        OrderBooksL1,
                    ),
                    (
                        LseCrypto::default(),
                        "eth",
                        "usd",
                        MarketDataInstrumentKind::Spot,
                        OrderBooksL1,
                    ),
                ],
            ))
            .init()
            .await
            .unwrap();

    let mut joined_stream = streams
        .select_all()
        .with_error_handler(|error| warn!(?error, "MarketStream generated error"));

    while let Some(event) = joined_stream.next().await {
        // A reconnect is reported rather than hidden, so a consumer can tell a quiet market from a
        // dropped connection. What follows it is a fresh subscription, resumed from the watermark.
        let event = match event {
            Event::Reconnecting(exchange) => {
                warn!(%exchange, "MarketStream reconnecting");
                continue;
            }
            Event::Item(event) => event,
        };

        match &event.kind {
            DataKind::Trade(trade) => info!(
                exchange = %event.exchange,
                instrument = %event.instrument,
                time_exchange = %event.time_exchange,
                price = %trade.price,
                // Genuine on LseCrypto, a hard-coded 1.0 on LseFx. Same field, same type, no signal.
                amount = %trade.amount,
                // Always empty, and always `None`: the feed publishes neither a trade identifier
                // nor an aggressor side, and neither is inferable from a quote.
                side = ?trade.side,
                "tick as trade"
            ),
            DataKind::OrderBookL1(l1) => info!(
                exchange = %event.exchange,
                instrument = %event.instrument,
                time_exchange = %event.time_exchange,
                best_bid = ?l1.best_bid.as_ref().map(|level| level.price),
                best_ask = ?l1.best_ask.as_ref().map(|level| level.price),
                // Both zero: the feed publishes no resting size. Do not read them as quantity.
                "tick as top-of-book"
            ),
            // Unreachable: this provider's WebSocket serves no other kind. There is no candle
            // channel at all — its candles are a REST-only product.
            other => warn!(kind = other.kind_name(), "unexpected DataKind"),
        }
    }
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
