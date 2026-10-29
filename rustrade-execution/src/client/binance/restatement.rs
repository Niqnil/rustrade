//! Restating an account stream's balances after it reconnects by itself.
//!
//! A balance or debt event the venue sends while the stream is disconnected is lost, and nothing
//! later on the stream restates it. So after each reconnect the stream reads the balances over
//! REST and emits them, once the reconnect's fill recovery has run. This module holds what that
//! read needs, independent of the venue's account kind:
//!
//! - [`StreamClock`] follows the venue's clock and the latest balance or debt time the stream
//!   delivered, from the events it forwards.
//! - [`BalanceRestatement`] is the restatement a reconnect owes, with its retry schedule.
//! - [`restatement_time`] is the time the read's balances are stamped with.
//! - [`restate`] runs the restatement owed once it is due, with the account's own read.

use super::shared::{REST_BALANCE_LAG, balance_read_time};
use crate::{
    AccountEventKind, UnindexedAccountEvent, client::venue_clock::VenueClock,
    error::UnindexedClientError, trade::TimeSource,
};
use chrono::{DateTime, Utc};
use std::{fmt::Display, future::Future, sync::Arc, time::Duration};
use tokio::{sync::mpsc, time::Instant};
use tracing::{info, warn};

/// The first wait before a failed restatement is read again. Each failure in a row doubles it, up
/// to [`RESTATEMENT_RETRY_MAX`].
pub(crate) const RESTATEMENT_RETRY_BASE: Duration = Duration::from_secs(5);

/// The longest wait between reads of a restatement that keeps failing.
pub(crate) const RESTATEMENT_RETRY_MAX: Duration = Duration::from_secs(120);

/// How long one restatement read may take before it counts as failed.
pub(crate) const RESTATEMENT_TIMEOUT: Duration = Duration::from_secs(30);

/// A [`StreamClock`] shared between the code that forwards a stream's events and the code that
/// restates its balances. A new stream starts with the default, which has seen nothing.
pub(crate) type SharedStreamClock = Arc<parking_lot::Mutex<StreamClock>>;

/// The venue's clock as an account stream's events show it, and the latest balance or debt time
/// the stream delivered.
#[derive(Debug, Default)]
pub(crate) struct StreamClock {
    venue: VenueClock,
    /// The latest `time_exchange` of a balance or debt event the stream delivered.
    last_balance: Option<DateTime<Utc>>,
}

impl StreamClock {
    /// Record a frame the stream received at `arrived` in `clock`: `sent`, the time `E` Binance
    /// stamped on it, and the `events` converted from it. The lock is taken once, and only when
    /// the clock learns something.
    ///
    /// Every event Binance pushes carries an `E`, an order's too, so the clock is sampled before
    /// the stream's first fill. A fill's, balance update's or debt change's own time is the
    /// venue's too and samples the clock as well; a fill whose time is this host's
    /// ([`TimeSource::Host`]) is skipped. A balance update's or debt change's time is also the
    /// latest balance time when it is later than the one held: the engine orders balance state by
    /// it, so a restatement stamped before it would be dropped.
    ///
    /// Record a frame before sending its events: a restatement read that starts after one is sent
    /// must see its balance time.
    pub(crate) fn observe_frame(
        clock: &SharedStreamClock,
        sent: Option<DateTime<Utc>>,
        events: &[UnindexedAccountEvent],
        arrived: Instant,
    ) {
        let mut samples = events.iter().filter_map(Self::sample_of).peekable();
        if sent.is_none() && samples.peek().is_none() {
            return;
        }
        let mut clock = clock.lock();
        if let Some(sent) = sent {
            clock.venue.observe(sent, arrived);
        }
        for (time, balance) in samples {
            clock.sample(time, balance, arrived);
        }
    }

    /// [`observe_frame`](Self::observe_frame) for one event, with no frame time.
    #[cfg(test)]
    pub(crate) fn observe(
        clock: &SharedStreamClock,
        event: &UnindexedAccountEvent,
        arrived: Instant,
    ) {
        Self::observe_frame(clock, None, std::slice::from_ref(event), arrived);
    }

    /// The venue's time `event` carries, and whether it is a balance or debt time, or `None` for
    /// an event the clock does not learn from.
    fn sample_of(event: &UnindexedAccountEvent) -> Option<(DateTime<Utc>, bool)> {
        match &event.kind {
            AccountEventKind::Trade(trade) if trade.time_source == TimeSource::Venue => {
                Some((trade.time_exchange, false))
            }
            AccountEventKind::BalanceStreamUpdate(update) => Some((update.0.time_exchange, true)),
            AccountEventKind::InstrumentBalanceUpdate(update) => {
                Some((update.balance.time_exchange, true))
            }
            AccountEventKind::MarginDebt(update) => Some((update.time_exchange, true)),
            _ => None,
        }
    }

    /// The venue's clock, as the stream's events show it.
    pub(crate) fn venue(&self) -> &VenueClock {
        &self.venue
    }

    /// Record the venue's `time`, seen at `arrived`, which is a balance or debt time if `balance`.
    fn sample(&mut self, time: DateTime<Utc>, balance: bool, arrived: Instant) {
        self.venue.observe(time, arrived);
        if balance {
            // `None` orders before any time.
            self.last_balance = self.last_balance.max(Some(time));
        }
    }

    /// When a restatement read may start, at `now` or later, and the latest balance time
    /// delivered by then, which its stamp is raised to (see [`restatement_time`]).
    ///
    /// The read waits until the venue's clock reads at least [`REST_BALANCE_LAG`] past that
    /// time. A read that starts then shows the state at that time or later, even when the venue's
    /// REST view trails its stream by the whole allowance. The venue's clock is sampled by the
    /// same events, so the wait ends at most [`REST_BALANCE_LAG`] after the latest of them
    /// arrived, and has usually passed by the time a reconnect is made.
    ///
    /// Take the time to raise the stamp to here, at the read's start: a balance event delivered
    /// during the read can be newer than what the read shows.
    pub(crate) fn read_start(&self, now: Instant) -> ReadStart {
        let Some(last) = self.last_balance else {
            return ReadStart::Now { floor: None };
        };
        let target = last + REST_BALANCE_LAG;
        let short = self
            .venue
            .earliest_at(now)
            .map_or(Duration::ZERO, |earliest| {
                (target - earliest).to_std().unwrap_or(Duration::ZERO)
            });
        if short.is_zero() {
            ReadStart::Now { floor: Some(last) }
        } else {
            // The venue's clock is counted on with a drift allowance of 100 ppm: a 1 ms margin
            // covers it for any wait shorter than 10 s, and the check when the wait ends catches
            // a longer one.
            ReadStart::At(now + short + Duration::from_millis(1))
        }
    }
}

/// When a restatement read may start ([`StreamClock::read_start`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReadStart {
    /// Now, with the latest balance time delivered, if any, to raise its stamp to.
    Now { floor: Option<DateTime<Utc>> },
    /// Not before this instant.
    At(Instant),
}

/// The time a restatement read's balances are stamped with, for a read whose request was sent at
/// `sent` by this host's clock, no earlier than `sent_at` by tokio's, and started when
/// [`StreamClock::read_start`] gave `floor`.
///
/// The earlier of the host's time and the venue's estimated time at the read's start, less the
/// venue's REST lag, so a host clock ahead of the venue's does not carry into the stamp. Raised to
/// `floor`, the latest balance time delivered before the read: the read shows that state or
/// later, and a stamp earlier than it would have the engine drop the restatement for the assets
/// it updated.
pub(crate) fn restatement_time(
    clock: &StreamClock,
    sent: DateTime<Utc>,
    sent_at: Instant,
    floor: Option<DateTime<Utc>>,
) -> DateTime<Utc> {
    let earliest = clock
        .venue
        .earliest_at(sent_at)
        .map_or(sent, |venue| venue.min(sent));
    let time = balance_read_time(earliest);
    floor.map_or(time, |floor| time.max(floor))
}

/// Restate the balances if a reconnect owes it and it is due: run `read` and send the events it
/// returns, which state the `account`'s balances.
///
/// `read` is started with the floor [`StreamClock::read_start`] gave, to stamp its balances by
/// [`restatement_time`]; a read not yet allowed to start defers the restatement instead. A read
/// that fails, or does not finish within [`RESTATEMENT_TIMEOUT`] (rate-limit waits included), is
/// logged at `warn` and retried after a backoff ([`BalanceRestatement::failed`]).
///
/// The restatement stays owed when this future is dropped before the read ends, or when the
/// consumer has gone and the events cannot be sent.
pub(crate) async fn restate<Read>(
    account: &'static str,
    clock: &SharedStreamClock,
    restatement: &mut BalanceRestatement,
    tx: &mpsc::UnboundedSender<UnindexedAccountEvent>,
    read: impl FnOnce(Option<DateTime<Utc>>) -> Read,
) where
    Read: Future<Output = Result<Vec<UnindexedAccountEvent>, UnindexedClientError>>,
{
    let now = Instant::now();
    if !restatement.is_due(now) {
        return;
    }
    let read_start = clock.lock().read_start(now);
    let floor = match read_start {
        ReadStart::Now { floor } => floor,
        ReadStart::At(start) => {
            restatement.defer(start);
            return;
        }
    };
    let events = match tokio::time::timeout(RESTATEMENT_TIMEOUT, read(floor)).await {
        Ok(Ok(events)) => events,
        Ok(Err(error)) => return retry(account, restatement, &error),
        Err(_) => {
            let secs = RESTATEMENT_TIMEOUT.as_secs();
            return retry(
                account,
                restatement,
                &format_args!("unfinished after {secs}s"),
            );
        }
    };
    let count = events.len();
    for event in events {
        if tx.send(event).is_err() {
            return;
        }
    }
    restatement.restated();
    info!(
        account,
        count, "Binance balances restated after a reconnect"
    );
}

/// Record that the `account`'s restatement read failed with `error`, to retry after a backoff.
fn retry(account: &'static str, restatement: &mut BalanceRestatement, error: &dyn Display) {
    let retry_in = restatement.failed().unwrap_or_default();
    warn!(
        account,
        %error,
        retry_in_secs = retry_in.as_secs(),
        "Binance balance restatement after a reconnect failed, will retry"
    );
}

/// The balance restatement a reconnect owes the consumer, if any, and when it is next due.
///
/// A reconnect [`open`](Self::open)s one, due at once, replacing one still owed: one read restates
/// everything, so a restatement never queues behind another. A read that fails is retried after
/// a backoff ([`failed`](Self::failed)), connected or across reconnects, until one succeeds
/// ([`restated`](Self::restated)). It is never given up: a read costs one account request.
#[derive(Debug, Default)]
pub(crate) struct BalanceRestatement(Option<Pending>);

#[derive(Debug, Clone, Copy)]
struct Pending {
    due: Instant,
    failures: u32,
}

impl BalanceRestatement {
    /// Owe a restatement, due now, in place of one still owed.
    pub(crate) fn open(&mut self) {
        self.0 = Some(Pending {
            due: Instant::now(),
            failures: 0,
        });
    }

    /// When the restatement owed is next due, or `None` when none is owed.
    pub(crate) fn next_due(&self) -> Option<Instant> {
        self.0.map(|pending| pending.due)
    }

    /// Whether a restatement is owed and due by `now`.
    pub(crate) fn is_due(&self, now: Instant) -> bool {
        self.0.is_some_and(|pending| pending.due <= now)
    }

    /// Hold the restatement owed until `due`, when its read may start.
    pub(crate) fn defer(&mut self, due: Instant) {
        if let Some(pending) = &mut self.0 {
            pending.due = pending.due.max(due);
        }
    }

    /// Record that the restatement's read failed: it is due again after a backoff, which is
    /// returned.
    pub(crate) fn failed(&mut self) -> Option<Duration> {
        let pending = self.0.as_mut()?;
        let delay = RESTATEMENT_RETRY_BASE
            .saturating_mul(1 << pending.failures.min(16))
            .min(RESTATEMENT_RETRY_MAX);
        pending.failures = pending.failures.saturating_add(1);
        pending.due = Instant::now() + delay;
        Some(delay)
    }

    /// Record that the balances were restated.
    pub(crate) fn restated(&mut self) {
        self.0 = None;
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use crate::{
        AccountEventKind, InstrumentBalanceUpdate, UnindexedAccountEvent,
        balance::{AssetBalance, AssetBalanceUpdate, Balance, BalanceUpdate, MarginDetails},
        error::ConnectivityError,
        margin_debt::{MarginDebtKind, MarginDebtUpdate},
    };
    use chrono::TimeZone;
    use rust_decimal::Decimal;
    use rustrade_instrument::{asset::name::AssetNameExchange, exchange::ExchangeId};
    use rustrade_integration::collection::snapshot::Snapshot;

    fn at(secs: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(1_700_000_000 + secs, 0).unwrap()
    }

    fn balance_update(time: DateTime<Utc>) -> UnindexedAccountEvent {
        UnindexedAccountEvent::new(
            ExchangeId::BinanceMargin,
            AccountEventKind::BalanceStreamUpdate(Snapshot(AssetBalanceUpdate::new(
                AssetNameExchange::new("BTC"),
                BalanceUpdate::new(Decimal::ONE, Decimal::ZERO),
                time,
            ))),
        )
    }

    /// A frame's event time samples the stream's clock, so a restatement stamped after it takes
    /// Binance's time when this host's clock is ahead, though no fill or balance was delivered.
    #[tokio::test(start_paused = true)]
    async fn a_frames_event_time_samples_the_clock_the_stamp_is_taken_from() {
        let clock = SharedStreamClock::default();
        let arrived = Instant::now();
        StreamClock::observe_frame(&clock, Some(at(100)), &[], arrived);

        assert_eq!(clock.lock().venue().earliest_at(arrived), Some(at(100)));
        // The host reads 60 s ahead of Binance.
        assert_eq!(
            restatement_time(&clock.lock(), at(160), arrived, None),
            at(100) - REST_BALANCE_LAG
        );
    }

    /// With nothing delivered, a read may start at once, and is stamped the REST lag before the
    /// host's time.
    #[tokio::test(start_paused = true)]
    async fn a_read_with_nothing_delivered_starts_at_once_stamped_the_lag_before_the_host() {
        let clock = SharedStreamClock::default();
        let now = Instant::now();
        assert_eq!(clock.lock().read_start(now), ReadStart::Now { floor: None });
        assert_eq!(
            restatement_time(&clock.lock(), at(100), now, None),
            at(100) - REST_BALANCE_LAG
        );
    }

    /// A read waits until the venue's clock is the REST lag past the latest balance time
    /// delivered, and is stamped no earlier than that time, though this host's clock is behind.
    #[tokio::test(start_paused = true)]
    async fn a_read_waits_out_the_lag_after_the_last_balance_and_is_stamped_no_earlier() {
        let clock = SharedStreamClock::default();
        let arrived = Instant::now();
        StreamClock::observe(&clock, &balance_update(at(100)), arrived);

        let ReadStart::At(start) = clock.lock().read_start(arrived + Duration::from_secs(2)) else {
            panic!("a read 2 s after the balance must wait");
        };
        assert!(
            start >= arrived + REST_BALANCE_LAG.to_std().unwrap(),
            "{start:?}"
        );
        assert!(start <= arrived + Duration::from_secs(5) + Duration::from_millis(2));
        let ReadStart::Now { floor } = clock.lock().read_start(start) else {
            panic!("the wait must have ended at {start:?}");
        };
        assert_eq!(floor, Some(at(100)));

        // This host reads 60 s behind the venue: the stamp is the delivered balance's time.
        assert_eq!(
            restatement_time(&clock.lock(), at(50), start, floor),
            at(100)
        );

        // A balance delivered during the read does not raise its stamp: the floor was taken at
        // the read's start.
        StreamClock::observe(&clock, &balance_update(at(200)), start);
        assert_eq!(
            restatement_time(&clock.lock(), at(50), start, floor),
            at(100)
        );
    }

    /// A host clock ahead of the venue's does not carry into the stamp: the venue's estimate,
    /// less the lag, is used.
    #[tokio::test(start_paused = true)]
    async fn a_host_clock_ahead_of_the_venue_does_not_carry_into_the_stamp() {
        let clock = SharedStreamClock::default();
        let arrived = Instant::now();
        StreamClock::observe(&clock, &balance_update(at(100)), arrived);

        let sent_at = arrived + Duration::from_secs(20);
        let stamp = restatement_time(&clock.lock(), at(500), sent_at, Some(at(100)));
        // The venue reads about 120 s then; the host's 500 s is ignored.
        assert!(stamp <= at(120) - REST_BALANCE_LAG, "{stamp}");
        assert!(stamp >= at(119) - REST_BALANCE_LAG, "{stamp}");
    }

    /// Only a later balance or debt time moves the latest balance time, whether a cross balance's,
    /// an isolated pair's, or a debt's; a fill samples the clock only, unless its time is this
    /// host's, and a REST read neither.
    #[tokio::test(start_paused = true)]
    async fn the_latest_balance_time_is_the_latest_balance_or_debt_event() {
        let clock = SharedStreamClock::default();
        let now = Instant::now();
        StreamClock::observe(&clock, &balance_update(at(100)), now);
        StreamClock::observe(&clock, &balance_update(at(90)), now);
        assert_eq!(clock.lock().last_balance, Some(at(100)));
        StreamClock::observe(&clock, &balance_update(at(105)), now);
        assert_eq!(clock.lock().last_balance, Some(at(105)));

        let pair = || rustrade_instrument::instrument::name::InstrumentNameExchange::new("BTCUSDT");
        let AccountEventKind::BalanceStreamUpdate(Snapshot(update)) = balance_update(at(110)).kind
        else {
            unreachable!()
        };
        StreamClock::observe(
            &clock,
            &UnindexedAccountEvent::new(
                ExchangeId::BinanceMargin,
                AccountEventKind::InstrumentBalanceUpdate(InstrumentBalanceUpdate::new(
                    pair(),
                    update,
                )),
            ),
            now,
        );
        assert_eq!(
            clock.lock().last_balance,
            Some(at(110)),
            "an isolated pair's"
        );

        StreamClock::observe(
            &clock,
            &UnindexedAccountEvent::new(
                ExchangeId::BinanceMargin,
                AccountEventKind::MarginDebt(MarginDebtUpdate::new(
                    AssetNameExchange::new("BTC"),
                    Some(pair()),
                    MarginDetails::new(Decimal::ONE, Decimal::ZERO),
                    MarginDebtKind::Borrow,
                    "BORROW".into(),
                    at(120),
                )),
            ),
            now,
        );
        assert_eq!(clock.lock().last_balance, Some(at(120)), "a debt's");

        let fill = |time| {
            crate::trade::Trade::new(
                crate::trade::TradeId::new("t-1"),
                crate::order::id::OrderId::new("o-1"),
                rustrade_instrument::instrument::name::InstrumentNameExchange::new("BTCUSDT"),
                crate::order::id::StrategyId::unknown(),
                time,
                rustrade_instrument::Side::Buy,
                Decimal::ONE,
                Decimal::ONE,
                None,
                crate::trade::AssetFees::new(AssetNameExchange::new("BTC"), Decimal::ZERO, None),
            )
        };
        let host_stamped = fill(at(300)).with_time_source(TimeSource::Host);
        StreamClock::observe(
            &clock,
            &UnindexedAccountEvent::new(
                ExchangeId::BinanceMargin,
                AccountEventKind::Trade(host_stamped),
            ),
            now,
        );
        assert_eq!(
            clock.lock().venue.earliest_at(now),
            Some(at(120)),
            "a fill stamped with this host's time does not sample the clock"
        );

        StreamClock::observe(
            &clock,
            &UnindexedAccountEvent::new(
                ExchangeId::BinanceMargin,
                AccountEventKind::Trade(fill(at(150))),
            ),
            now,
        );
        assert_eq!(
            clock.lock().last_balance,
            Some(at(120)),
            "a fill is no balance"
        );
        assert_eq!(
            clock.lock().venue.earliest_at(now),
            Some(at(150)),
            "but it samples the clock"
        );

        StreamClock::observe(
            &clock,
            &UnindexedAccountEvent::new(
                ExchangeId::BinanceMargin,
                AccountEventKind::BalanceSnapshot(Snapshot(AssetBalance::new(
                    AssetNameExchange::new("BTC"),
                    Balance::new(Decimal::ONE, Decimal::ONE),
                    at(200),
                ))),
            ),
            now,
        );
        assert_eq!(
            clock.lock().last_balance,
            Some(at(120)),
            "a REST read is ignored"
        );
        assert_eq!(clock.lock().venue.earliest_at(now), Some(at(150)));
    }

    /// A failed restatement backs off from the base, doubling up to the cap; a reconnect makes a
    /// new one due at once.
    #[tokio::test(start_paused = true)]
    async fn a_failed_restatement_backs_off_and_a_reconnect_makes_it_due_again() {
        let mut restatement = BalanceRestatement::default();
        assert_eq!(restatement.next_due(), None);
        assert_eq!(restatement.failed(), None);

        restatement.open();
        assert!(restatement.is_due(Instant::now()));
        let delays: Vec<_> = (0..7).map(|_| restatement.failed().unwrap()).collect();
        assert_eq!(
            delays,
            [5, 10, 20, 40, 80, 120, 120].map(Duration::from_secs)
        );
        assert!(!restatement.is_due(Instant::now()));

        restatement.open();
        assert!(restatement.is_due(Instant::now()));
        assert_eq!(restatement.failed(), Some(RESTATEMENT_RETRY_BASE));

        restatement.restated();
        assert_eq!(restatement.next_due(), None);
    }

    /// A deferral only moves the due time later.
    #[tokio::test(start_paused = true)]
    async fn a_deferral_only_moves_the_due_time_later() {
        let mut restatement = BalanceRestatement::default();
        restatement.open();
        let now = Instant::now();
        restatement.defer(now + Duration::from_secs(3));
        restatement.defer(now + Duration::from_secs(1));
        assert_eq!(restatement.next_due(), Some(now + Duration::from_secs(3)));
    }

    /// What a fake restatement read returns: one balance, stamped at its floor.
    fn restated_btc(floor: Option<DateTime<Utc>>) -> Vec<UnindexedAccountEvent> {
        vec![balance_update(floor.unwrap_or(at(0)))]
    }

    /// A due restatement runs the read and sends its events, and is settled; one not owed, or not
    /// due, reads nothing.
    #[tokio::test(start_paused = true)]
    async fn a_due_restatement_sends_the_read_and_is_settled() {
        let clock = SharedStreamClock::default();
        let mut restatement = BalanceRestatement::default();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let unread = |_| async { panic!("nothing is due") };

        restate("test", &clock, &mut restatement, &tx, unread).await;
        restatement.open();
        restatement.failed();
        restate("test", &clock, &mut restatement, &tx, unread).await;

        restatement.open();
        StreamClock::observe(&clock, &balance_update(at(100)), Instant::now());
        tokio::time::advance(REST_BALANCE_LAG.to_std().unwrap() + Duration::from_secs(1)).await;
        restate("test", &clock, &mut restatement, &tx, |floor| async move {
            assert_eq!(floor, Some(at(100)));
            Ok(restated_btc(floor))
        })
        .await;
        assert_eq!(rx.try_recv().unwrap(), restated_btc(Some(at(100)))[0]);
        assert!(rx.try_recv().is_err());
        assert_eq!(restatement.next_due(), None);
    }

    /// A restatement right after a balance event is deferred until its read may start, reading
    /// nothing before then.
    #[tokio::test(start_paused = true)]
    async fn a_restatement_not_yet_allowed_to_start_is_deferred() {
        let clock = SharedStreamClock::default();
        StreamClock::observe(&clock, &balance_update(at(100)), Instant::now());
        let mut restatement = BalanceRestatement::default();
        restatement.open();
        let (tx, _rx) = mpsc::unbounded_channel();

        restate("test", &clock, &mut restatement, &tx, |_| async {
            panic!("the read may not start yet")
        })
        .await;
        let ReadStart::At(start) = clock.lock().read_start(Instant::now()) else {
            panic!("the read must wait");
        };
        assert_eq!(restatement.next_due(), Some(start));
    }

    /// A read that fails, or does not finish in time, sends nothing and is due again after the
    /// backoff.
    #[tokio::test(start_paused = true)]
    async fn a_failed_or_unfinished_read_is_retried_after_the_backoff() {
        let clock = SharedStreamClock::default();
        let mut restatement = BalanceRestatement::default();
        let (tx, mut rx) = mpsc::unbounded_channel();

        restatement.open();
        restate("test", &clock, &mut restatement, &tx, |_| async {
            Err(UnindexedClientError::Connectivity(
                ConnectivityError::Timeout,
            ))
        })
        .await;
        assert_eq!(
            restatement.next_due(),
            Some(Instant::now() + RESTATEMENT_RETRY_BASE)
        );

        tokio::time::advance(RESTATEMENT_RETRY_BASE).await;
        let started = Instant::now();
        restate("test", &clock, &mut restatement, &tx, |_| {
            std::future::pending()
        })
        .await;
        assert_eq!(Instant::now(), started + RESTATEMENT_TIMEOUT);
        assert_eq!(
            restatement.next_due(),
            Some(Instant::now() + 2 * RESTATEMENT_RETRY_BASE)
        );
        assert!(rx.try_recv().is_err());
    }

    /// A restatement stays owed when its read is dropped part-way, and when the consumer has gone.
    #[tokio::test(start_paused = true)]
    async fn a_restatement_not_delivered_stays_owed() {
        let clock = SharedStreamClock::default();
        let mut restatement = BalanceRestatement::default();
        let (tx, rx) = mpsc::unbounded_channel();
        restatement.open();
        let due = restatement.next_due();

        let dropped = tokio::time::timeout(
            Duration::from_secs(1),
            restate("test", &clock, &mut restatement, &tx, |_| {
                std::future::pending()
            }),
        )
        .await;
        assert!(dropped.is_err());
        assert_eq!(restatement.next_due(), due);

        drop(rx);
        restate("test", &clock, &mut restatement, &tx, |floor| async move {
            Ok(restated_btc(floor))
        })
        .await;
        assert_eq!(restatement.next_due(), due);
    }
}
