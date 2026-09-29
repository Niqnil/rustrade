//! London Strategic Edge WebSocket shape canary (network + credential gated).
//!
//! # Why this exists
//!
//! `lse_ws_handshake.rs` drives this integration against a synthetic server, which proves the client
//! behaves correctly *given* the protocol as this repository understands it. Nothing in it can
//! notice that the provider's understanding has changed. That is this file's job, and it is the only
//! test in the crate that ever reaches the real socket — the provider prohibits redistributing its
//! data (<https://londonstrategicedge.com/terms>), so no recorded frame may be committed here and
//! every other test is necessarily synthetic.
//!
//! The restriction reaches this file's output too. `lse-weekly.yml` runs it with `--nocapture` in a
//! public repository, so whatever a passing run prints is published in the workflow log. That is
//! why the `CANARY_OK` lines carry symbols, timestamps and counts, and never a price or a size.
//!
//! # ⚠️ What it deliberately does NOT assert
//!
//! **No provider-side inventory counts.** The published symbol list moved from 7,940 to 8,516
//! entries inside a week, and the subscription cap is a plan attribute the provider may revise. A
//! canary that pinned either would fail for a reason that is not a defect, and would then be muted
//! — which costs more than it ever caught. Support is asserted structurally instead: the guards
//! either work against the real list or they do not.
//!
//! Signal 5 counts *ticks over a window*, which is a different quantity: it is a property of the
//! feed this connection is being served rather than of the provider's published inventory, and it
//! is held to a floor set more than an order of magnitude below every rate ever measured here,
//! rather than pinned to a figure.
//!
//! # The seven signals it does assert
//!
//! 1. **Every subscribed symbol actually ticks.** This surface's quietest failure is a subscription
//!    that is *confirmed and then silent* — the provider answers `subscribed` for a symbol it does
//!    not serve, never errors, and holds the slot for the life of the connection. A shape check on
//!    whichever frames happen to arrive cannot see it, because the frames that arrive are fine; it
//!    is the absent ones that matter. So delivery is required per symbol, not in aggregate.
//! 2. **A tick's decoded instant is plausible — and on a 24/7 venue, recent.** The provider spells
//!    `ts` three ways and changes spelling per dataset and between live and replayed frames. Every
//!    one of those spellings decodes to *a* `DateTime` — a seconds-vs-milliseconds epoch misread
//!    lands in 1970 or in the fifty-third century, and a dropped timezone lands hours out — so the
//!    decode cannot be checked by whether it succeeded, only by whether the answer is plausible.
//!    The *tight* recency window is held only against continuously-traded crypto, because a venue
//!    that keeps market hours serves a stale instant legitimately; see the note below.
//! 3. **A symbol the provider does not offer is rejected before subscribing.** This passes only if
//!    the `authenticated` frame still carries a usable symbol list: were the list to disappear or be
//!    renamed, the guard degrades to a warning by design, the provider confirms the bogus symbol,
//!    and the batch would succeed. That silent degradation is exactly what this catches.
//! 4. **A resumed reconnect leaves no gap and rolls nothing back.** Resumption is the one part of
//!    this integration whose correctness rests on the *provider's* half of a bargain — that `start`
//!    is inclusive, is honoured at the resolution it was sent at, and replays from exactly where it
//!    is pointed. Every other test asserts that against fixtures this repository wrote, and a
//!    fixture cannot notice the provider changing its mind. So one test disconnects deliberately,
//!    stays away long enough that a resubscribe ignoring the watermark would be unmistakable, and
//!    reconnects: the resumed stream must begin at the watermark rather than at the reconnect, and
//!    never before it.
//! 5. **The crypto tape delivers at a rate, not merely at all.** A subscription can be confirmed,
//!    tick, decode cleanly and still carry a small fraction of the feed. Whole sessions have been
//!    reported running at a couple of percent of normal throughput with the socket up, no error
//!    frame and no close code — and signals 1 and 2 are both satisfied by such a stream, because
//!    every symbol does deliver *a* tick and every `ts` on it is genuinely fresh. Throughput is
//!    the only thing separating it from a healthy connection, so it is the only thing that can
//!    detect it. It is asserted as a floor rather than a band, because a fast feed is not a
//!    defect, and only against crypto; see the note below on what that leaves uncovered.
//! 6. **Option contracts spell, subscribe and print as this integration expects.** Every contract
//!    on a slice of the provider's own REST print tape must rebuild, through the connector, to the
//!    exact ticker the provider printed it under; the busiest of them must subscribe (one
//!    underlying, confirmed once); and, **while the US options session is open**, at least one must
//!    print over the socket, with nothing from the rest of the chain surfacing as an error. Whether
//!    the session is open is read from the REST tape rather than a clock, which knows nothing of
//!    holidays: a settled minute holding prints is an open market. Outside the session the print
//!    half reports `CANARY_SKIP`, which is why `lse-weekly.yml` is scheduled inside it.
//! 7. **One connection serves several batches and both kinds.** Clones of one subscriber share a
//!    socket, and every stream on it must deliver: a batch the provider confirmed and then stopped
//!    serving on a shared socket, or a frame no longer carrying the key the connection routes by,
//!    would leave every synthetic test green and a shared stream silent.
//!
//! # Skip vs. fail contract
//!
//! - `LSE_API_KEY` **unset** → **SKIP** (logged, test passes), so CI without secrets stays green.
//! - `LSE_API_KEY` set but unusable → **FAIL**. A skip here would be indistinguishable from "no
//!   secrets configured", so a mistyped key would report green forever.
//! - Key present but an assertion fails → **FAIL** (the real signal).
//!
//! ## ⚠️ The contract above is invisible without `--nocapture`
//!
//! Every skip and every `CANARY_OK` is a `println!`, and libtest **discards stdout for a passing
//! test**. Run without `--nocapture` — as the command in *Running* below does not — and a skip is
//! reported as `ok`, identical to a run that exercised every signal. That is why `--nocapture` is
//! part of the invocation rather than a debugging nicety, and why nothing this file *decides* is
//! left to a printed line: what must not pass silently is asserted.
//!
//! # Why every test here is `#[serial]`
//!
//! The provider permits **one** WebSocket connection per API key, and says so plainly when it
//! refuses a second: `TOO_MANY_CONNECTIONS: Max 1 concurrent websocket connection(s) for this API
//! key; 1 already open from this same address.` Every test below opens a socket, each through a
//! subscriber of its own, and Rust's harness runs the tests in a file in parallel, so unserialised
//! they contend for the one slot: whichever connects first proceeds and the rest fail inside their
//! `expect`, before asserting anything at all.
//!
//! That failure is worse than an ordinary flake, for the reason the vault canary gives about its
//! own cap: it presents as four red tests on the surface this file exists to watch, it is not a
//! provider-side change, and it reads exactly like one. A drift detector that cries wolf is one
//! people learn to ignore.
//!
//! Serialising holds the binary to a single socket at a time — which the resume test already
//! assumed, since it drops one connection before opening the next. It is also what makes signal 5
//! well defined: a rate is only meaningful as a measurement of the whole of what this key is being
//! served, and one connection per key is what guarantees that is what it counts.
//!
//! # ⚠️ A closed venue is not a silent one — measured
//!
//! One test covers the only venue that reaches the space-separated timestamp spelling, and that
//! venue keeps market hours. It was first written to **skip on silence**, on the assumption that a
//! closed market delivers nothing. That assumption is false: subscribing on a Saturday delivered a
//! tick stamped at the previous session's close — well-formed, correctly decoded, and five hours
//! old. Holding it to a ten-minute window failed the canary for a market simply being shut, which
//! is the failure-for-a-non-defect this file's own reasoning rules out.
//!
//! So that test holds the wide plausibility band unconditionally and reports whether the tight
//! recency signal was reached at all. A stale-but-plausible instant and a `CANARY_SKIP` line mean
//! the same thing — rerun while that market is open. Neither is a pass for the signal it names.
//!
//! ## ⚠️ Silence must be proved innocent before it is read as a closure
//!
//! Tolerating silence opens a hole, and it is the one this whole file is built to close. The error
//! handler these tests install is a `filter_map`: an item it is called for is **removed** from the
//! stream. A connection on which every frame failed to decode therefore looks, from below the
//! handler, exactly like a connection that delivered nothing — so a shape change that broke the
//! decoder outright would be read as "the market is shut" and reported as a *pass*.
//!
//! So decode failures are counted, and the count is asserted zero **before** any silence is
//! interpreted. Silence is only allowed to mean a closed venue once it is known that nothing
//! arrived and failed to be read.
//!
//! # ⚠️ The rate floor covers ONE venue — a green run is not "the stream is healthy"
//!
//! Signal 5 is asserted against crypto alone, and that is a limit chosen rather than one
//! overlooked. A rate floor needs a denominator that means something, and on a venue keeping
//! market hours the same low count is produced by a shut market and by a throttled feed alike. A
//! floor there would fail for a closure — the failure-for-a-non-defect this file's own reasoning
//! rules out — and would then be muted, taking the signal with it. Crypto never closes, so it is
//! the one venue where a low count has exactly one explanation.
//!
//! The consequence has to be said plainly, because it is the misreading the floor exists to
//! prevent: **a green run here says the crypto tape is flowing, and says nothing whatever about
//! throughput on the equities, ETF, FX and CFD venues.** Those are covered by signals 1–4 only. On
//! them this file still catches a subscription that never ticks and a frame it cannot read, and
//! still cannot catch one delivering a fraction of the feed — which is, as it happens, where
//! degraded throughput has actually been reported, rather than on crypto. Covering that surface
//! needs a different instrument: a comparison against a second source over the same window, or a
//! measurement taken during known trading hours. It does not need a looser floor here, which would
//! only trade the one venue this file can speak for against a figure it cannot defend on any.
//!
//! # Running
//!
//! ```bash
//! set -a && . ./.env && set +a
//! cargo test --test lse_ws_canary --features lse -- --ignored --nocapture
//! ```
//!
//! Marked `#[ignore]` so a default test run never opens a connection or spends the shared
//! allowance.
//!
//! ⚠️ If a run reports `TOO_MANY_CONNECTIONS` despite the serialisation above, a socket from an
//! interrupted earlier run is still open against the same key. The provider closes it on its own
//! timeout; wait rather than re-running immediately.

#![cfg(feature = "lse")]
#![allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable

use chrono::{DateTime, Datelike, Duration as ChronoDuration, Utc};
use futures_util::{Stream, StreamExt};
use rustrade_data::{
    Identifier, MarketStream, NoInitialSnapshots,
    error::DataError,
    event::{DataKind, MarketEvent},
    exchange::lse::{
        LseCfd, LseCrypto, LseOptions,
        live::LseSubscriber,
        market::LseMarket,
        options::{LseOptionContract, LseOptionPrint},
        resume::LseResumeState,
        stream::LseStream,
        vault::LseVaultClient,
    },
    streams::{
        Streams,
        consumer::MarketStreamResult,
        reconnect::{Event, stream::ReconnectingStream},
    },
    subscriber::Subscriber,
    subscription::{
        Subscription,
        book::OrderBooksL1,
        trade::{PublicTrade, PublicTrades},
    },
};
use rustrade_instrument::{
    exchange::ExchangeId,
    instrument::{
        kind::option::OptionExercise,
        market_data::{
            MarketDataInstrument,
            kind::{MarketDataInstrumentKind, MarketDataOptionContract},
        },
    },
};
use rustrade_integration::error::SocketError;
use serial_test::serial;
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::time::Instant;

const KEY_ENV: &str = "LSE_API_KEY";

/// How long to wait for every subscribed symbol to tick.
///
/// Generous against the measured rates — one busy crypto symbol replayed six figures of ticks per
/// hour — so a timeout here means silence, not slowness.
const DELIVERY_TIMEOUT: Duration = Duration::from_secs(45);

/// How long the rate signal counts ticks for, once the first one has arrived.
///
/// Long enough that what it measures is a rate rather than a coin toss — at the slowest throughput
/// ever recorded on this feed the window carries several hundred ticks — and short enough to sit
/// inside the same run as everything else here. The clock starts at the first tick rather than at
/// the subscribe, so connection and handshake latency are not charged to the provider's throughput.
const RATE_WINDOW: Duration = Duration::from_secs(30);

/// The slowest aggregate rate a healthy **continuously-traded** subscription may deliver at.
///
/// `BTC/USD` and `ETH/USD` together delivered 51, 170, 225 and 293 ticks a second across four
/// measurements, and `BTC/USD` alone has sustained 21–58 a second on every earlier probe of this
/// feed. **Note the spread**: the same pair, measured the same way within the hour, varied by
/// nearly six times. That is the argument for this floor rather than a proportional one — a bound
/// set at some fraction of "normal" has no stable figure to take a fraction of, and would be
/// calibrated against whichever hour it was written in.
///
/// One a second sits beneath the slowest of those by more than an order of magnitude, so it is
/// clear of the variation rather than tracking it. The throttling it exists to catch has been
/// reported at a couple of percent of normal throughput, which lands well below even the quietest
/// figure above, so the two are cleanly separated. A floor nearer normal would buy no additional
/// detection and would spend this canary's credibility to get it.
const MIN_TICKS_PER_SECOND: usize = 1;

/// How far behind now a tick may be stamped on a **continuously-traded** venue.
///
/// Wide enough to absorb clock skew and a quiet minute on a thin symbol, and far narrower than any
/// epoch or timezone misread: those miss by decades or by whole hours. Only crypto is held to this,
/// because only crypto never closes — see the module header.
const MAX_TICK_AGE: ChronoDuration = ChronoDuration::minutes(10);

/// How far behind now a tick may be stamped on a venue that **keeps market hours**.
///
/// A closed venue serves its last session print rather than going silent, so the age of a perfectly
/// decoded instant there is bounded by the closure, not by the feed: a Friday-evening close read on
/// a Sunday is two days old and entirely correct. This band contains any weekend or holiday closure
/// while still being narrower than an epoch-scale misread by decades, so it keeps the signal that
/// survives a closed market and drops the one that cannot.
const MAX_CLOSED_VENUE_TICK_AGE: ChronoDuration = ChronoDuration::days(7);

/// How far ahead of now a live tick may be stamped.
///
/// Non-zero only for clock skew — the provider stamps in the past by construction.
const MAX_TICK_LEAD: ChronoDuration = ChronoDuration::minutes(2);

/// How long the resume canary's two connections stay apart.
///
/// This is the whole measurement: a reconnect that ignores the watermark begins at *now*, this far
/// past it, while a resumed one begins *at* it. The two are only distinguishable if the pause is
/// comfortably wider than the tick spacing, and it is — a busy crypto symbol prints roughly thirty
/// ticks a second, so this is hundreds of ticks of separation. It is also short enough that the
/// replay it triggers drains in a second or two rather than the minutes a whole-window resume takes.
const RESUME_PAUSE: Duration = Duration::from_secs(20);

/// How far past the watermark the resumed stream's earliest event may be stamped.
///
/// A correct resume begins *at* the watermark, so this is slack for a quiet stretch at that instant
/// — not for the reconnect. It has to stay well under [`RESUME_PAUSE`] or it stops distinguishing a
/// replay from a fresh live subscription, which is the only thing this bound is for.
const RESUME_GAP_TOLERANCE: ChronoDuration = ChronoDuration::seconds(5);

/// How many events the resume canary's first connection takes before disconnecting.
///
/// Only has to be enough to seed a watermark. The arithmetic being exercised is per instant, not
/// per batch, so a larger sample would buy nothing but wall-clock.
const TICKS_BEFORE_DISCONNECT: usize = 25;

/// Build a subscriber, or `None` when the key is **absent** (skip rather than fail).
///
/// Only an unset variable skips. A key that is *set but unusable* — a stray newline from a `.env`
/// edit, a mis-encoded paste — is a misconfiguration, and reporting it as a skip would let this
/// canary pass green while never once reaching the provider, which is exactly the state it exists
/// to detect. The error is safe to print: the credential type redacts the key from every message.
fn subscriber() -> Option<LseSubscriber> {
    if std::env::var_os(KEY_ENV).is_none() {
        println!("CANARY_SKIP: {KEY_ENV} is not set - skipping");
        return None;
    }

    Some(
        LseSubscriber::from_env()
            .unwrap_or_else(|error| panic!("{KEY_ENV} is set but unusable: {error}")),
    )
}

fn spot(base: &str) -> MarketDataInstrument {
    MarketDataInstrument::from((base, "usd", MarketDataInstrumentKind::Spot))
}

fn cfd(base: &str) -> MarketDataInstrument {
    MarketDataInstrument::from((base, "usd", MarketDataInstrumentKind::Cfd))
}

/// Fail if a tick's decoded instant is not plausible, where `max_age` is what "plausible" means for
/// the venue under test.
///
/// See signal 2 in the module header: every spelling the provider uses decodes to *some* instant,
/// so plausibility is the only available check on whether it decoded to the right one. How much
/// staleness is plausible is a property of the venue's trading hours, not of the decoder, which is
/// why the bound is a parameter rather than a constant.
fn assert_stamped_plausibly(
    instrument: &MarketDataInstrument,
    time_exchange: DateTime<Utc>,
    max_age: ChronoDuration,
) {
    let now = Utc::now();
    let age = now - time_exchange;

    assert!(
        age <= max_age,
        "{instrument} ticked at {time_exchange}, {age} behind now (limit {max_age}) - the timestamp \
         decode has probably misread the provider's spelling",
    );
    assert!(
        -age <= MAX_TICK_LEAD,
        "{instrument} ticked at {time_exchange}, {} ahead of now - a live tick cannot be stamped in \
         the future beyond clock skew, so the timestamp decode has probably misread the provider's \
         spelling",
        -age,
    );
}

/// A decode-failure counter, and the error handler that feeds it.
///
/// # ⚠️ Printing a decode failure is not reporting it
/// [`with_error_handler`](ReconnectingStream::with_error_handler) is a `filter_map`: the item it is
/// called for is **removed** from the stream. So a connection on which every frame failed to decode
/// looks, from below the handler, exactly like a connection that delivered nothing — and a test
/// that reads silence as "the market is closed" would report `CANARY_SKIP` and pass, on precisely
/// the shape change this file exists to catch. A `println!` cannot close that: libtest discards
/// stdout for a passing test, so the evidence would only exist in a run that already knew to look.
///
/// The counter turns it into an assertion. The handler still prints, because the message names
/// *what* changed and the count only says that something did.
fn decode_failures() -> (Arc<AtomicUsize>, impl Fn(DataError)) {
    let failures = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&failures);

    let handler = move |error: DataError| {
        counter.fetch_add(1, Ordering::Relaxed);
        println!("CANARY: market stream error: {}", without_payload(&error));
    };

    (failures, handler)
}

/// `error` with any frame it quotes cut off.
///
/// A decode failure's message ends `for payload: <the raw frame>`, and this line reaches a public
/// GitHub Actions log, where one live frame is this provider's data, which may not be
/// redistributed (see the module header). The parser's own message comes first and is kept: it
/// names the field that no longer decodes, which is the diagnostic, and quotes at most one value.
///
/// The cut matches `SocketError`'s wording, so
/// `a_decode_failure_is_reported_without_its_frame` pins it on every CI run: if that wording
/// changes, the test fails rather than the frame reaching the log.
fn without_payload(error: &DataError) -> String {
    let message = error.to_string();
    match message
        .find(" for payload: ")
        .or_else(|| message.find(" for binary payload: "))
    {
        Some(end) => format!("{} [frame withheld]", &message[..end]),
        None => message,
    }
}

/// Not a canary: no network and no key, so it runs with the ordinary test suite.
#[test]
fn a_decode_failure_is_reported_without_its_frame() {
    let payload = r#"{"symbol":"BTC/USD","price":"not-a-number"}"#;
    let error = DataError::from(SocketError::Deserialise {
        error: serde_json::from_str::<u64>(payload).unwrap_err(),
        payload: payload.to_owned(),
    });

    let reported = without_payload(&error);

    assert!(!reported.contains("BTC/USD"), "{reported}");
    assert!(reported.contains("Deserialising JSON error"), "{reported}");
    assert!(reported.ends_with("[frame withheld]"), "{reported}");
}

/// Fail if any frame failed to decode, whatever else the stream did.
///
/// Called **before** any silence is interpreted: a decode failure explains silence, and reading it
/// as a closed market instead is how this canary would go quiet on a real shape change.
fn assert_every_frame_decoded(failures: &Arc<AtomicUsize>) {
    let failed = failures.load(Ordering::Relaxed);

    assert_eq!(
        failed, 0,
        "{failed} frame(s) failed to decode - the error handler filters them out of the stream, so \
         this would otherwise present as silence rather than as a failure. The provider's frame \
         shape has changed; see the CANARY lines above for what it now sends",
    );
}

/// Drive `stream` until every instrument in `expected` has delivered a tick, or the deadline.
///
/// Returns the instruments that never ticked **and** the newest instant seen. The caller decides
/// what both mean for its venue: whether silence is a failure (a 24/7 venue) or a skip (a venue that
/// closes), and whether the newest instant was recent enough to have exercised the tight window.
async fn collect_first_tick_per_instrument<S>(
    mut stream: S,
    expected: &[MarketDataInstrument],
    max_age: ChronoDuration,
) -> (Vec<MarketDataInstrument>, Option<DateTime<Utc>>)
where
    S: Stream<Item = Event<ExchangeId, MarketEvent<MarketDataInstrument, PublicTrade>>> + Unpin,
{
    let mut pending = expected.to_vec();
    let mut newest: Option<DateTime<Utc>> = None;
    let deadline = Instant::now() + DELIVERY_TIMEOUT;

    // One deadline governs the whole wait, so there is no per-iteration clock arithmetic and no
    // chance of spinning once the stream goes quiet.
    let _ = tokio::time::timeout_at(deadline, async {
        while !pending.is_empty() {
            match stream.next().await {
                Some(Event::Item(event)) => {
                    // Checked on every tick rather than only the first: the provider changes `ts`
                    // spelling between datasets, and a stream that begins well can still carry a
                    // frame this integration reads wrongly.
                    assert_stamped_plausibly(&event.instrument, event.time_exchange, max_age);

                    newest = newest.max(Some(event.time_exchange));

                    if let Some(index) = pending.iter().position(|i| *i == event.instrument) {
                        let delivered = pending.swap_remove(index);
                        println!("CANARY_OK: {delivered} delivered a live tick");
                    }
                }
                // Logged rather than ignored: a reconnect mid-window is the likeliest innocent
                // cause of a timeout, and distinguishing it from silence matters when reading a
                // failure.
                Some(Event::Reconnecting(origin)) => {
                    println!("CANARY: {origin} is reconnecting mid-test");
                }
                // The reconnecting stream should never exhaust; if it has, the consumer task died.
                None => panic!("the market stream terminated before delivering every symbol"),
            }
        }
    })
    .await;

    (pending, newest)
}

/// Count the ticks delivered over [`RATE_WINDOW`], starting the clock at the first one.
///
/// Returns the count and the number of reconnects that intervened, or `None` if no tick arrived at
/// all within [`DELIVERY_TIMEOUT`] — a distinction the caller needs, because "nothing in 45 s" and
/// "a handful in 30 s" have the same verdict but not the same diagnosis.
///
/// The tick that starts the clock is counted: the window runs from the instant it arrived, so it
/// sits on the opening boundary rather than before it. Reconnects are returned rather than merely
/// logged because a reconnect mid-window costs both time and ticks, and is the likeliest innocent
/// explanation of a count below the floor.
async fn count_ticks_over_rate_window<S>(mut stream: S) -> Option<(usize, usize)>
where
    S: Stream<Item = Event<ExchangeId, MarketEvent<MarketDataInstrument, PublicTrade>>> + Unpin,
{
    let mut reconnects = 0usize;

    // Phase one: wait for the first tick, which opens the window. Nothing is counted here, so a
    // slow connect cannot depress the rate measured below.
    let opened = tokio::time::timeout(DELIVERY_TIMEOUT, async {
        loop {
            match stream.next().await {
                Some(Event::Item(_)) => return,
                Some(Event::Reconnecting(origin)) => {
                    reconnects += 1;
                    println!("CANARY: {origin} is reconnecting before the rate window opened");
                }
                None => panic!("the market stream terminated before delivering a single tick"),
            }
        }
    })
    .await;

    if opened.is_err() {
        return None;
    }

    // Phase two: count everything that arrives over exactly the window. The timeout ends it, so
    // the interval is the window by construction rather than by arithmetic on arrival instants —
    // which matters, because a stream that goes silent partway through must still be measured over
    // the whole window rather than over the part of it that had ticks in it.
    let mut ticks = 1usize;
    let _ = tokio::time::timeout(RATE_WINDOW, async {
        loop {
            match stream.next().await {
                Some(Event::Item(_)) => ticks += 1,
                Some(Event::Reconnecting(origin)) => {
                    reconnects += 1;
                    println!("CANARY: {origin} is reconnecting mid-window");
                }
                None => panic!("the market stream terminated before the rate window elapsed"),
            }
        }
    })
    .await;

    Some((ticks, reconnects))
}

#[tokio::test]
#[ignore = "opens a live connection and spends the shared provider allowance; run on demand"]
#[serial]
async fn every_subscribed_crypto_symbol_delivers_a_live_tick() {
    let Some(subscriber) = subscriber() else {
        return;
    };

    // Crypto because it is the one dataset family that trades continuously: on any other, silence
    // is ambiguous between "closed" and "confirmed but never served", and this test exists to make
    // that distinction unambiguous.
    let expected = [spot("btc"), spot("eth")];

    let streams = Streams::<PublicTrades>::builder()
        .subscribe(
            subscriber,
            expected.clone().map(|instrument| {
                Subscription::<LseCrypto, MarketDataInstrument, PublicTrades>::new(
                    LseCrypto::default(),
                    instrument,
                    PublicTrades,
                )
            }),
        )
        .init()
        .await
        .expect("subscribing to continuously-traded crypto symbols should succeed");

    // Decode failures are counted as well as printed: the handler filters them out of the stream,
    // so an undecodable frame arrives below as nothing at all.
    let (failures, on_error) = decode_failures();
    let stream = streams.select_all().with_error_handler(on_error);

    // Crypto is the one venue whose staleness is unambiguous, so it carries the tight window.
    let (silent, _newest) =
        collect_first_tick_per_instrument(Box::pin(stream), &expected, MAX_TICK_AGE).await;

    // Before the silence below is read as a missing subscription: a frame that failed to decode
    // produces the same silence and has a different cause, so diagnosing it as the other would send
    // a reader after the pre-subscribe guard for a problem in the decoder.
    assert_every_frame_decoded(&failures);

    assert!(
        silent.is_empty(),
        "{silent:?} were confirmed but delivered no tick in {DELIVERY_TIMEOUT:?}, and every frame \
         that did arrive decoded - on a continuously-traded venue that is the confirmed-then-silent \
         failure this integration's pre-subscribe guard exists to prevent, which means the guard's \
         symbol list no longer reflects what the provider actually serves",
    );
}

/// A confirmed, ticking, cleanly-decoding subscription can still be carrying a fraction of the
/// feed, and only throughput separates it from a healthy one.
///
/// # What this catches that the test above cannot
///
/// [`every_subscribed_crypto_symbol_delivers_a_live_tick`] stops the moment each symbol has
/// delivered once, which is the right shape for the failure it covers and blind to this one: at a
/// fiftieth of normal throughput every symbol still delivers inside the window, every `ts` on
/// what arrives is genuinely fresh, and every frame decodes. That connection passes all three of
/// the signals above while carrying almost nothing, and it does so with the socket up, no error
/// frame and no close code, so there is nothing else in band to notice it by.
///
/// # Why a floor, why crypto, and why this far down
///
/// The upper end is not a defect — a busy tape is the healthy case — so the only defensible bound
/// is a lower one. It is held against crypto because crypto never closes, which is what makes a
/// low count mean one thing here and two things on any venue that keeps hours; the module header
/// sets out what that leaves uncovered, and it is not a small thing. The floor itself sits far
/// below normal on purpose: see [`MIN_TICKS_PER_SECOND`] for the measurements it is placed
/// against. Separating normal from degraded needs only a figure somewhere between them, and every
/// step closer to normal is bought with flakiness that would eventually get this muted.
///
/// # ⚠️ The rate is asserted aggregate, not per symbol
///
/// Per-symbol floors would need a per-symbol denominator, and the two symbols here differ in
/// liquidity by enough that the thinner one would set the flakiest bound in the file while adding
/// nothing: the throttling reported on this feed suppressed a whole connection rather than one
/// subscription on it. Aggregate keeps the bound on the quantity that actually moved.
#[tokio::test]
#[ignore = "opens a live connection and spends the shared provider allowance; run on demand"]
#[serial]
async fn the_crypto_tape_delivers_at_a_rate_rather_than_a_trickle() {
    let Some(subscriber) = subscriber() else {
        return;
    };

    let expected = [spot("btc"), spot("eth")];

    let streams = Streams::<PublicTrades>::builder()
        .subscribe(
            subscriber,
            expected.clone().map(|instrument| {
                Subscription::<LseCrypto, MarketDataInstrument, PublicTrades>::new(
                    LseCrypto::default(),
                    instrument,
                    PublicTrades,
                )
            }),
        )
        .init()
        .await
        .expect("subscribing to continuously-traded crypto symbols should succeed");

    // Counted as well as printed for the reason given on `decode_failures`, and it bites harder
    // here than anywhere else in this file: the handler filters an undecodable frame out of the
    // stream, so a partial shape change presents below as exactly what this test is measuring — a
    // reduced tick count. Without the assertion that follows, a decoder broken on some fraction of
    // frames would be reported as the provider throttling us.
    let (failures, on_error) = decode_failures();
    let stream = streams.select_all().with_error_handler(on_error);

    let counted = count_ticks_over_rate_window(Box::pin(stream)).await;

    // Before the count below is read as throttling, and that ordering is the whole point of the
    // paragraph above.
    assert_every_frame_decoded(&failures);

    let Some((ticks, reconnects)) = counted else {
        panic!(
            "no tick arrived in {DELIVERY_TIMEOUT:?} on a continuously-traded venue, and every \
             frame that did arrive decoded - a rate of zero is below any floor, but the diagnosis \
             is the confirmed-then-silent subscription rather than a throttled one"
        );
    };

    let floor = MIN_TICKS_PER_SECOND * RATE_WINDOW.as_secs() as usize;
    let per_second = ticks as f64 / RATE_WINDOW.as_secs_f64();
    // Display rather than Debug: the failure message below is what this test produces, and a
    // two-element Debug dump of the instruments buries the numbers that matter in it.
    let symbols = expected
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ");

    assert!(
        ticks >= floor,
        "{ticks} tick(s) in {RATE_WINDOW:?} ({per_second:.2}/s) across {symbols}, below the \
         floor of {floor} ({MIN_TICKS_PER_SECOND}/s), with {reconnects} reconnect(s) and every \
         frame decoding - the socket is up and serving a fraction of the feed. If the reconnect \
         count is non-zero this may be that rather than the provider; rerun before reading it as \
         throttling",
    );

    println!(
        "CANARY_OK: {ticks} tick(s) in {RATE_WINDOW:?} ({per_second:.2}/s, floor \
         {MIN_TICKS_PER_SECOND}/s), {reconnects} reconnect(s)",
    );
}

/// Covers the space-separated timestamp spelling, which the crypto test above cannot reach: the
/// provider sends `2026-01-02 09:37:21.690159+00:00` on this venue and the `T`-separated form on
/// crypto, and [`DateTime::parse_from_rfc3339`] rejects the former outright.
#[tokio::test]
#[ignore = "opens a live connection and spends the shared provider allowance; run on demand"]
#[serial]
async fn a_cfd_tick_decodes_to_a_plausible_instant() {
    let Some(subscriber) = subscriber() else {
        return;
    };

    let expected = [cfd("xau")];

    let streams = Streams::<PublicTrades>::builder()
        .subscribe(
            subscriber,
            expected.clone().map(|instrument| {
                Subscription::<LseCfd, MarketDataInstrument, PublicTrades>::new(
                    LseCfd::default(),
                    instrument,
                    PublicTrades,
                )
            }),
        )
        .init()
        .await
        .expect("subscribing to a published CFD symbol should succeed");

    // Decode failures are counted as well as printed: the handler filters them out of the stream,
    // so an undecodable frame arrives below as nothing at all.
    let (failures, on_error) = decode_failures();
    let stream = streams.select_all().with_error_handler(on_error);

    // The wide band, because this venue closes. A tick arriving here is not evidence the market is
    // open: a closed session serves its last print, correctly stamped hours or days ago.
    let (silent, newest) =
        collect_first_tick_per_instrument(Box::pin(stream), &expected, MAX_CLOSED_VENUE_TICK_AGE)
            .await;

    // ⚠️ BEFORE the match below, and that ordering is the whole point. This is the one test whose
    // silence is a legitimate SKIP, so it is the one place where a total decode failure -- the exact
    // shape change this file exists to catch -- would otherwise be read as "the market is shut" and
    // reported as a pass.
    assert_every_frame_decoded(&failures);

    // The plausibility assertion already ran inside the collector, on every tick that arrived. What
    // is left is to say which signal was actually reached, because neither outcome below is a pass
    // for the recency check, and a silent green here would be the muted canary this file warns
    // about.
    match newest {
        None => println!(
            "CANARY_SKIP: {silent:?} delivered no tick in {DELIVERY_TIMEOUT:?} - this venue keeps \
             market hours, so this is most likely closed rather than broken. The space-separated \
             timestamp spelling was NOT exercised; rerun while the market is open.",
        ),
        Some(newest) if Utc::now() - newest > MAX_TICK_AGE => println!(
            "CANARY_SKIP: the newest tick is stamped {newest}, older than {MAX_TICK_AGE} - the \
             session has closed and the provider is serving its last print. The spelling decoded \
             plausibly, but recency was NOT exercised; rerun while the market is open.",
        ),
        Some(newest) => println!(
            "CANARY_OK: the space-separated timestamp spelling decoded to {newest}, live and recent",
        ),
    }
}

/// The pre-subscribe guard is only as good as the list it checks against. If the provider stopped
/// publishing symbols in its `authenticated` frame, the guard would degrade to a warning by design,
/// the bogus symbol below would be *confirmed*, and this would return `Ok` — so a passing assertion
/// here is what proves the list is still both present and honoured.
#[tokio::test]
#[ignore = "opens a live connection; run on demand"]
#[serial]
async fn a_symbol_the_provider_does_not_offer_is_rejected_before_subscribing() {
    let Some(subscriber) = subscriber() else {
        return;
    };

    let subscription = Subscription::<LseCrypto, MarketDataInstrument, PublicTrades>::new(
        LseCrypto::default(),
        spot("nope-xyz"),
        PublicTrades,
    );

    let error = subscriber
        .subscribe(&[subscription])
        .await
        .expect_err(
            "the provider confirms symbols it does not serve, so a bogus symbol must be rejected \
             by the pre-subscribe guard rather than by the provider",
        )
        .to_string();

    assert!(error.contains("NOPE-XYZ/USD"), "{error}");
    println!("CANARY_OK: an unoffered symbol was rejected before any subscribe was sent");
}

/// One connection to the provider, resume state and all, with nothing wrapped around it.
///
/// The reconnecting stream the other tests build is deliberately avoided here: it decides for itself
/// when to reconnect, and this test's entire signal is *what a reconnect does*, which means owning
/// both connections explicitly. What is driven is still production code — this is the same
/// [`LseStream`] that [`StreamSelector`](rustrade_data::exchange::StreamSelector) resolves to.
async fn connection(
    subscriber: &LseSubscriber,
    subscriptions: &[Subscription<LseCrypto, MarketDataInstrument, PublicTrades>],
) -> LseStream<LseCrypto, MarketDataInstrument, PublicTrades> {
    <LseStream<_, _, _> as MarketStream<LseCrypto, MarketDataInstrument, PublicTrades>>::init::<
        NoInitialSnapshots,
    >(subscriber, subscriptions)
    .await
    .expect("subscribing to a continuously-traded crypto symbol should succeed")
}

/// Drive `stream` until `is_last` accepts an event or the deadline passes, returning what arrived.
///
/// A decode failure fails the test rather than being skipped: on this file's surface it is itself a
/// shape-change signal, and silently dropping it would let a resumed stream that delivers nothing
/// *readable* look the same as one that delivers nothing at all.
async fn collect_until<S, F>(
    stream: &mut S,
    deadline: Instant,
    mut is_last: F,
) -> Vec<MarketEvent<MarketDataInstrument, PublicTrade>>
where
    S: Stream<Item = Result<MarketEvent<MarketDataInstrument, PublicTrade>, DataError>> + Unpin,
    F: FnMut(&MarketEvent<MarketDataInstrument, PublicTrade>) -> bool,
{
    let mut collected = Vec::new();

    let _ = tokio::time::timeout_at(deadline, async {
        while let Some(item) = stream.next().await {
            let event = item.expect("the market stream yielded an error instead of an event");
            let last = is_last(&event);
            collected.push(event);

            if last {
                break;
            }
        }
    })
    .await;

    collected
}

/// Resumption, end to end against the real provider: disconnect, wait, reconnect, and check the
/// seam.
///
/// # Why this cannot be a synthetic test
///
/// Every other resume test in this repository asserts the *client* half of the contract — that the
/// watermark advances on delivery, that `start` carries the stored instant to the microsecond, that
/// the replayed prefix is skipped by position. All of them are checked against frames this
/// repository wrote, so all of them would keep passing if the provider changed the *server* half:
/// if `start` stopped being inclusive, stopped being honoured at microsecond resolution, or began
/// replaying from somewhere other than where it was pointed. That half has only ever been verified
/// by hand. This makes it repeatable.
///
/// # What the two bounds actually separate
///
/// After the pause, a stream that consulted the watermark and one that ignored it look identical in
/// every respect but one: where their first event is *stamped*. A live-only resubscribe begins at
/// the reconnect — a whole [`RESUME_PAUSE`] past the watermark. A resumed one begins at the
/// watermark itself. So the earliest resumed instant is required to sit **at or after** the
/// watermark (nothing already delivered is served a second time, which is what a mis-scaled or
/// timezone-shifted `start` would cause) and **no later than** a small tolerance past it (the
/// window was really replayed). Reaching a live instant afterwards is required too, so the check
/// covers the whole pause rather than only its first tick.
///
/// # ⚠️ What it deliberately does NOT decide
///
/// **The tie-group prefix.** `start` is inclusive, so the provider re-serves every tick sharing the
/// watermark instant and the transformer drops the ones already delivered by position. Deciding
/// whether it dropped *exactly* the right number needs the provider's own total at that instant,
/// which is not observable — and inferring it from the tick signatures is not available either,
/// because identical consecutive ticks are genuine on this feed and are never de-duplicated, so a
/// legitimate repeat and a re-delivered duplicate are the same bytes. The counts on both sides of
/// the seam are therefore printed rather than asserted; the positional arithmetic itself is covered
/// exactly, against known totals, by the transformer's unit tests. Crypto also quantises to the
/// millisecond, which is why a rounding error in `start` cannot be caught here — that is what
/// `the_epoch_form_round_trips_an_instant_to_the_microsecond` is for.
#[tokio::test]
#[ignore = "opens two live connections and pauses between them; run on demand"]
#[serial]
async fn a_resumed_reconnect_replays_from_the_watermark_rather_than_from_now() {
    let Some(subscriber) = subscriber() else {
        return;
    };

    // Crypto for the same reason the delivery test uses it: it never closes, so a pause of a known
    // length is a window of known content rather than possibly a shut market.
    let state = Arc::new(LseResumeState::new());
    let subscriber = subscriber.with_resume(Arc::clone(&state));
    let subscriptions = [
        Subscription::<LseCrypto, MarketDataInstrument, PublicTrades>::new(
            LseCrypto::default(),
            spot("btc"),
            PublicTrades,
        ),
    ];

    // First connection: take enough ticks to seed a watermark, then hang up.
    let mut opened = connection(&subscriber, &subscriptions).await;
    let mut taken = 0;
    let delivered = collect_until(&mut opened, Instant::now() + DELIVERY_TIMEOUT, |_| {
        taken += 1;
        taken >= TICKS_BEFORE_DISCONNECT
    })
    .await;
    drop(opened);

    let watermark = delivered
        .last()
        .map(|event| event.time_exchange)
        .expect("a continuously-traded symbol delivered no tick to resume from");
    for event in &delivered {
        assert_stamped_plausibly(&event.instrument, event.time_exchange, MAX_TICK_AGE);
    }
    println!(
        "CANARY: {} tick(s) taken, watermark {watermark}",
        delivered.len()
    );

    tokio::time::sleep(RESUME_PAUSE).await;

    // Second connection: the same subscriber, so the same resume state, which is exactly what the
    // reconnecting stream does when it re-runs the subscribe.
    let mut resumed_stream = connection(&subscriber, &subscriptions).await;
    let reopened_at = Utc::now();
    let resumed = collect_until(
        &mut resumed_stream,
        Instant::now() + DELIVERY_TIMEOUT,
        |event| event.time_exchange >= reopened_at,
    )
    .await;

    for event in &resumed {
        assert_stamped_plausibly(&event.instrument, event.time_exchange, MAX_TICK_AGE);
    }

    let earliest = resumed
        .iter()
        .map(|event| event.time_exchange)
        .min()
        .expect("the resumed subscription delivered no tick at all");

    assert!(
        earliest >= watermark,
        "the resumed stream served {earliest}, which is before the watermark {watermark} - the \
         provider replayed further back than it was asked to, or `start` no longer means what this \
         integration sends it as, and events already delivered are being served again",
    );
    assert!(
        earliest - watermark <= RESUME_GAP_TOLERANCE,
        "the resumed stream began at {earliest}, {} past the watermark {watermark} after a \
         {RESUME_PAUSE:?} pause - the historical window was not replayed, so the reconnect left \
         the gap resumption exists to close",
        earliest - watermark,
    );
    assert!(
        resumed
            .last()
            .is_some_and(|event| event.time_exchange >= reopened_at),
        "the resumed stream never reached a live instant within {DELIVERY_TIMEOUT:?} - it began at \
         the watermark but did not deliver the whole pause, so the window is covered at its start \
         and not at its end",
    );

    // Reported, not asserted -- see the note on the tie-group prefix above.
    println!(
        "CANARY_OK: resumed at {earliest} from watermark {watermark} after a {RESUME_PAUSE:?} \
         pause, {} tick(s) replayed to catch up ({} delivered at the watermark instant before the \
         disconnect, {} served at it after)",
        resumed.len(),
        delivered
            .iter()
            .filter(|event| event.time_exchange == watermark)
            .count(),
        resumed
            .iter()
            .filter(|event| event.time_exchange == watermark)
            .count(),
    );
}

/// The underlying the options signal subscribes, and whose tape decides whether the session is open.
///
/// The busiest the provider carries, so a settled minute of it is empty only when the market is shut.
const OPTIONS_UNDERLYING: &str = "SPY";

/// How many of the underlying's busiest contracts to register.
///
/// Enough that one printing inside [`DELIVERY_TIMEOUT`] is near-certain in session, and all on one
/// underlying, so one subscription slot.
const OPTION_CONTRACTS: usize = 10;

/// How far before now the open-market probe window ends.
///
/// Past the vault's settle margin, which refuses a range ending closer to now than that because the
/// provider can serve one incomplete, with a margin on top for clock skew.
const OPEN_PROBE_END: ChronoDuration = ChronoDuration::seconds(90);

/// 15:00 UTC is inside the US options session on both sides of the DST change, so a minute there
/// on a recent weekday supplies contracts to spell and subscribe when the market is shut now.
const CLOSED_PROBE_TIME: chrono::NaiveTime = chrono::NaiveTime::from_hms_opt(15, 0, 0).unwrap();

/// The instrument a caller would register for `contract`.
///
/// Exercise style plays no part in the OSI symbol, and the provider does not report it; American is
/// what a caller registering a listed equity option would say.
fn option_instrument(contract: &LseOptionContract) -> MarketDataInstrument {
    MarketDataInstrument::from((
        contract.underlying.as_str(),
        "usd",
        MarketDataInstrumentKind::Option(MarketDataOptionContract {
            kind: contract.kind,
            exercise: OptionExercise::American,
            expiry: contract
                .expiry
                .and_hms_opt(20, 0, 0)
                .expect("20:00 is a valid time")
                .and_utc(),
            strike: contract.strike,
        }),
    ))
}

/// A slice of the provider's own print tape, and whether it shows the session open *now*.
///
/// A settled minute ending just before now holding prints means the market is open. An empty one
/// means it is shut — or that ingestion is lagging, which the provider does episodically — and either
/// way the print signal cannot be judged, so contracts are taken from a recent session instead.
async fn option_tape(vault: &LseVaultClient) -> (Vec<LseOptionPrint>, bool) {
    let end = Utc::now() - OPEN_PROBE_END;
    let recent = vault
        .collect_option_flow(
            Some(OPTIONS_UNDERLYING),
            end - ChronoDuration::minutes(1),
            end,
        )
        .await
        .expect("option flow fetch");

    if !recent.is_empty() {
        return (recent, true);
    }

    let today = Utc::now().date_naive();
    for days_back in 1..=7 {
        let day = today - ChronoDuration::days(days_back);
        if matches!(day.weekday(), chrono::Weekday::Sat | chrono::Weekday::Sun) {
            continue;
        }

        let start = day.and_time(CLOSED_PROBE_TIME).and_utc();
        let prints = vault
            .collect_option_flow(
                Some(OPTIONS_UNDERLYING),
                start,
                start + ChronoDuration::minutes(1),
            )
            .await
            .expect("option flow fetch");

        if !prints.is_empty() {
            return (prints, false);
        }
    }

    panic!(
        "no {OPTIONS_UNDERLYING} option prints now or at {CLOSED_PROBE_TIME} UTC on any weekday in \
         the last week - the print tape the contracts are chosen from appears to have stopped"
    );
}

/// Signal 6. The spelling half holds at any hour; the delivery half only in session.
#[tokio::test]
#[ignore = "opens a live connection and spends the shared provider allowance; run on demand"]
#[serial]
async fn option_contracts_spell_subscribe_and_print_as_expected() {
    let Some(subscriber) = subscriber() else {
        return;
    };
    let vault = LseVaultClient::from_env()
        .unwrap_or_else(|error| panic!("{KEY_ENV} is set but unusable: {error}"));

    let (tape, open) = option_tape(&vault).await;

    // Spelling: every contract on the tape, rebuilt from its parts, must name the provider's ticker.
    // A wrong spelling is the quietest failure this dataset has: the contract is accepted, its prints
    // arrive under the real ticker, and the stream counts them as someone else's and drops them.
    let mut prints_per_contract = HashMap::<&LseOptionContract, usize>::new();
    for print in &tape {
        *prints_per_contract.entry(&print.contract).or_default() += 1;
    }

    let misspelt = prints_per_contract
        .keys()
        .filter_map(|contract| {
            let subscription = Subscription::<LseOptions, MarketDataInstrument, PublicTrades>::new(
                LseOptions::default(),
                option_instrument(contract),
                PublicTrades,
            );
            let spelt: LseMarket = subscription.id();

            (spelt.as_ref() != contract.ticker.as_str())
                .then(|| format!("{} spelt {spelt}", contract.ticker))
        })
        .collect::<Vec<_>>();

    assert!(
        misspelt.is_empty(),
        "{} of {} contracts rebuild to a symbol other than the provider's own ticker: {misspelt:?} - \
         a subscription to any of them would be confirmed and then never deliver",
        misspelt.len(),
        prints_per_contract.len(),
    );
    println!(
        "CANARY_OK: all {} contracts on the tape spell back to the provider's ticker",
        prints_per_contract.len()
    );

    // Subscription: the busiest contracts, which all share one underlying and so one slot.
    let mut busiest = prints_per_contract.into_iter().collect::<Vec<_>>();
    busiest.sort_by(|(a, a_prints), (b, b_prints)| {
        b_prints.cmp(a_prints).then_with(|| a.ticker.cmp(&b.ticker))
    });
    let expected = busiest
        .into_iter()
        .take(OPTION_CONTRACTS)
        .map(|(contract, _)| option_instrument(contract))
        .collect::<Vec<_>>();

    let streams = Streams::<PublicTrades>::builder()
        .subscribe(
            subscriber,
            expected.iter().cloned().map(|instrument| {
                Subscription::<LseOptions, MarketDataInstrument, PublicTrades>::new(
                    LseOptions::default(),
                    instrument,
                    PublicTrades,
                )
            }),
        )
        .init()
        .await
        .expect(
            "subscribing option contracts on one underlying should be confirmed once - a failure \
             here with a validation timeout means the confirmation frame changed shape",
        );
    println!(
        "CANARY_OK: {} contracts on {OPTIONS_UNDERLYING} subscribed and confirmed",
        expected.len()
    );

    // Delivery. Every error reaching the handler is counted, which here covers more than a decode:
    // the rest of the chain arrives too, and must be dropped as counted rather than raised.
    let (failures, on_error) = decode_failures();
    let mut stream = Box::pin(streams.select_all().with_error_handler(on_error));

    let deadline = Instant::now() + DELIVERY_TIMEOUT;
    let delivered = tokio::time::timeout_at(deadline, async {
        loop {
            match stream.next().await {
                Some(Event::Item(event)) => return Some(event),
                Some(Event::Reconnecting(origin)) => {
                    println!("CANARY: {origin} is reconnecting mid-test");
                }
                None => return None,
            }
        }
    })
    .await
    .ok()
    .flatten();

    // Before silence is read as a closed market, for the reason given on `decode_failures` - and on
    // this dataset it also proves the unregistered chain is being dropped rather than raised.
    assert_every_frame_decoded(&failures);

    match (delivered, open) {
        (Some(event), _) => {
            assert!(
                expected.contains(&event.instrument),
                "{} was delivered but never registered - the stream should drop the rest of the \
                 chain",
                event.instrument,
            );
            assert_stamped_plausibly(&event.instrument, event.time_exchange, MAX_TICK_AGE);
            assert!(
                event.kind.price > rust_decimal::Decimal::ZERO
                    && event.kind.amount > rust_decimal::Decimal::ZERO,
                "{} printed without a positive premium and size",
                event.instrument,
            );
            // No premium or size: the log is public (see the module docs).
            println!(
                "CANARY_OK: {} printed at {}",
                event.instrument, event.time_exchange,
            );
        }
        (None, true) => panic!(
            "the REST tape shows the session open, yet none of the {OPTION_CONTRACTS} busiest \
             contracts printed over the socket in {DELIVERY_TIMEOUT:?}, and every frame decoded - \
             the options channel is confirmed but silent, or delivering under symbols the \
             connector does not spell"
        ),
        (None, false) => println!(
            "CANARY_SKIP: the US options session is shut, so option delivery was NOT exercised; \
             spelling and subscription were. Rerun during the session (13:30-20:00 UTC in US \
             summer time, 14:30-21:00 in winter)."
        ),
    }
}

/// Signal 7. Several batches and both kinds from clones of one subscriber, all over one socket.
///
/// # What only the provider can answer
///
/// `lse_ws_handshake.rs` proves the connection task routes, de-duplicates and fans out correctly,
/// against a server this repository wrote. What that cannot notice is the provider changing the
/// premises underneath it: that one socket serves several subscribe batches at once, that a symbol
/// subscribed once ticks for every kind reading it, and that the frames still carry the routing key
/// the task sorts them by. Any of those failing leaves the handshake green and a shared stream
/// silent, so each stream here must deliver on its own.
///
/// # ⚠️ What it deliberately does NOT assert
///
/// **That the streams share exactly one socket.** That is a property of this crate's code, not of
/// the provider, and `lse_ws_handshake.rs` asserts it exactly, on every CI run, by counting the
/// connections its synthetic provider accepts. Proving it again here would mean opening a second
/// socket to watch the provider refuse it, which tests the provider's connection policy rather than
/// any premise this integration rests on: a provider that lifted the cap would fail this for a
/// non-defect, which is the failure this file's own reasoning rules out. While the cap stands, a
/// batch that opened a socket of its own would fail `init` here with `TOO_MANY_CONNECTIONS` anyway.
///
/// # Why this shape
///
/// Three batches, crypto only, so a scheduled run with every other market shut still exercises it.
/// The two trade batches are separate `subscribe` calls, so they are separate attaches to the
/// connection. The quote batch re-reads both symbols the trade batches already hold: on the wire
/// that is two slots and no further subscribe, and each frame must reach both kinds.
#[tokio::test]
#[ignore = "opens a live connection and spends the shared provider allowance; run on demand"]
#[serial]
async fn clones_of_one_subscriber_share_one_socket_across_batches_and_kinds() {
    let Some(subscriber) = subscriber() else {
        return;
    };

    let symbols = [spot("btc"), spot("eth")];
    let trades = |instrument: &MarketDataInstrument| {
        Subscription::<LseCrypto, MarketDataInstrument, PublicTrades>::new(
            LseCrypto::default(),
            instrument.clone(),
            PublicTrades,
        )
    };

    let streams: Streams<MarketStreamResult<MarketDataInstrument, DataKind>> =
        Streams::builder_multi()
            .add(
                Streams::<PublicTrades>::builder()
                    .subscribe(subscriber.clone(), [trades(&symbols[0])])
                    .subscribe(subscriber.clone(), [trades(&symbols[1])]),
            )
            .add(Streams::<OrderBooksL1>::builder().subscribe(
                subscriber,
                symbols.clone().map(|instrument| {
                    Subscription::<LseCrypto, MarketDataInstrument, OrderBooksL1>::new(
                        LseCrypto::default(),
                        instrument,
                        OrderBooksL1,
                    )
                }),
            ))
            .init()
            .await
            .expect(
                "every batch from clones of one subscriber should attach to one shared connection - \
                 a TOO_MANY_CONNECTIONS here means a batch opened a socket of its own",
            );

    // Decode failures are counted as well as printed: the handler filters them out of the stream,
    // so an undecodable frame arrives below as nothing at all.
    let (failures, on_error) = decode_failures();
    let mut stream = Box::pin(streams.select_all().with_error_handler(on_error));

    // One entry per stream the connection must feed: a kind and a symbol. A symbol held by both
    // kinds is one subscription on the wire and two entries here, which is the point.
    let mut pending = ["public_trade", "l1"]
        .into_iter()
        .flat_map(|kind| symbols.iter().map(move |symbol| (kind, symbol.clone())))
        .collect::<Vec<_>>();
    let expected = pending.len();
    let mut reconnects = 0usize;

    let deadline = Instant::now() + DELIVERY_TIMEOUT;
    let _ = tokio::time::timeout_at(deadline, async {
        while !pending.is_empty() {
            match stream.next().await {
                Some(Event::Item(event)) => {
                    assert_stamped_plausibly(&event.instrument, event.time_exchange, MAX_TICK_AGE);

                    let kind = event.kind.kind_name();
                    if let Some(index) = pending
                        .iter()
                        .position(|(k, symbol)| *k == kind && *symbol == event.instrument)
                    {
                        let (kind, symbol) = pending.swap_remove(index);
                        println!("CANARY_OK: {symbol} delivered {kind} over the shared connection");
                    }
                }
                // Counted, not asserted: a reconnect is the likeliest innocent cause of a timeout,
                // and the report below needs it to be read correctly.
                Some(Event::Reconnecting(origin)) => {
                    reconnects += 1;
                    println!("CANARY: {origin} is reconnecting mid-test");
                }
                None => panic!("the market stream terminated before every stream delivered"),
            }
        }
    })
    .await;

    // Before the silence below is read as a routing failure, for the reason given on
    // `decode_failures`.
    assert_every_frame_decoded(&failures);

    assert!(
        pending.is_empty(),
        "{pending:?} delivered nothing in {DELIVERY_TIMEOUT:?} on a continuously-traded venue, with \
         {reconnects} reconnect(s) and every frame decoding - the shared connection confirmed these \
         streams and then never routed them a frame",
    );

    println!(
        "CANARY_OK: {expected} streams across 3 batches and 2 kinds delivered over one connection, \
         {reconnects} reconnect(s)",
    );
}
