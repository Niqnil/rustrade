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
//! # ⚠️ What it deliberately does NOT assert
//!
//! **No counts.** The published symbol list moved from 7,940 to 8,516 entries inside a week, and
//! the subscription cap is a plan attribute the provider may revise. A canary that pinned either
//! would fail for a reason that is not a defect, and would then be muted — which costs more than it
//! ever caught. Support is asserted structurally instead: the guards either work against the real
//! list or they do not.
//!
//! # The four signals it does assert
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
//! # Running
//!
//! ```bash
//! set -a && . ./.env && set +a
//! cargo test --test lse_ws_canary --features lse -- --ignored --nocapture
//! ```
//!
//! Marked `#[ignore]` so a default test run never opens a connection or spends the shared
//! allowance.

#![cfg(feature = "lse")]
#![allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use futures_util::{Stream, StreamExt};
use rustrade_data::{
    MarketStream, NoInitialSnapshots,
    error::DataError,
    event::MarketEvent,
    exchange::lse::{
        LseCfd, LseCrypto, live::LseSubscriber, resume::LseResumeState, stream::LseStream,
    },
    streams::{
        Streams,
        reconnect::{Event, stream::ReconnectingStream},
    },
    subscriber::Subscriber,
    subscription::{Subscription, trade::PublicTrade, trade::PublicTrades},
};
use rustrade_instrument::{
    exchange::ExchangeId,
    instrument::market_data::{MarketDataInstrument, kind::MarketDataInstrumentKind},
};
use std::{
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
        println!("CANARY: market stream error: {error:?}");
    };

    (failures, handler)
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

#[tokio::test]
#[ignore = "opens a live connection and spends the shared provider allowance; run on demand"]
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

/// Covers the space-separated timestamp spelling, which the crypto test above cannot reach: the
/// provider sends `2026-01-02 09:37:21.690159+00:00` on this venue and the `T`-separated form on
/// crypto, and [`DateTime::parse_from_rfc3339`] rejects the former outright.
#[tokio::test]
#[ignore = "opens a live connection and spends the shared provider allowance; run on demand"]
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
