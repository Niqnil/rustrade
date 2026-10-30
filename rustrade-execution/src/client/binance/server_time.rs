//! The clock a client stamps its signed requests with: Binance's, as read from its server time.
//!
//! Binance refuses a signed request whose `timestamp` is 1 s or more ahead of its own clock, or
//! more than `recvWindow` (5 s) behind it, with `-1021`. A host clock that stays that far off
//! would fail every signed request. So each request is stamped with the earliest Binance's clock
//! can read, as a [`VenueClock`] bounds it from `GET /api/v3/time`: never ahead of Binance's
//! clock, and behind it by about the time the answer took to arrive.
//!
//! The estimate counts on from its sample on tokio's monotonic clock, so a step of this host's
//! wall clock between reads does not move the stamps. That clock does not advance while the host
//! is suspended, which would leave the stamps behind by the time it was; a request that finds the
//! wall clock gone further than the monotonic one since the last read waits for a new read first.

use crate::client::venue_clock::{HostMoment, VenueClock};
use binance_sdk::{common::config::TimestampProvider, spot::rest_api::RestApi};
use chrono::{DateTime, TimeDelta, Utc};
use futures::future::BoxFuture;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::time::Instant;
use tracing::{debug, warn};

/// How old the last server-time read may be before a request starts another, in the background.
///
/// The estimate's drift allowance (100 ppm) lets it fall behind Binance's clock by about 30 ms in
/// this time, against the 5 s Binance accepts.
pub(crate) const RESYNC_AFTER: Duration = Duration::from_secs(300);

/// The most a server-time read may take. A request waits on a read only before the client's
/// first signed request, after a suspend (see [`PAUSE_ALLOWANCE`]) and after a `-1021`.
pub(crate) const READ_TIMEOUT: Duration = Duration::from_secs(5);

/// How far this host's clock may lead or trail Binance's before a read warns: Binance refuses a
/// request stamped this far ahead of its clock.
const LEAD_WARN: TimeDelta = TimeDelta::seconds(1);

/// How much further this host's wall clock may have gone than its monotonic clock since the last
/// read before a request waits for a new one: a suspend of the host, or a forward step of its wall
/// clock. Each leaves the monotonic estimate behind Binance's clock by the difference. NTP slewing
/// the wall clock (up to 500 ppm, 1.8 s an hour) can also reach it, but only after half an hour
/// or more without a read, which the refresh a request starts normally prevents while requests
/// keep arriving; after a longer idle, the next request waits for a read, as after a suspend.
const PAUSE_ALLOWANCE: TimeDelta = TimeDelta::seconds(1);

/// Serde default for each Binance config's `server_time_sync`: on.
pub(crate) const fn default_server_time_sync() -> bool {
    true
}

/// Reads Binance's server time, in milliseconds since the Unix epoch.
pub(crate) type ReadServerTime =
    Arc<dyn Fn() -> BoxFuture<'static, anyhow::Result<i64>> + Send + Sync>;

/// [`ReadServerTime`] from `GET /api/v3/time`, on `rest`.
///
/// The read is unsigned, so `rest` may be the client that signs with this clock.
pub(crate) fn read_server_time(rest: Arc<RestApi>) -> ReadServerTime {
    Arc::new(move || {
        let rest = Arc::clone(&rest);
        Box::pin(async move {
            let time = rest.time().await?.data().await?;
            time.server_time
                .ok_or_else(|| anyhow::anyhow!("Binance's server time answer has no serverTime"))
        })
    })
}

/// The estimate of Binance's clock that signed requests are stamped with.
///
/// Shared by the SDK configurations, through [`provider`](Self::provider), and the
/// [`ServerTime`] that reads it. Before its first read it stamps with this host's clock.
#[derive(Debug, Clone, Default)]
pub(crate) struct SigningClock {
    estimate: Arc<parking_lot::Mutex<Estimate>>,
}

#[derive(Debug, Default)]
struct Estimate {
    clock: VenueClock,
    /// When the last read finished, whether or not it succeeded.
    last_read: Option<HostMoment>,
}

impl SigningClock {
    /// The provider the SDK calls for each signed request's `timestamp`.
    pub(crate) fn provider(&self) -> TimestampProvider {
        let this = self.clone();
        Arc::new(move || this.stamp_ms())
    }

    /// Now, on Binance's clock as estimated, or on this host's before any read has succeeded.
    fn stamp(&self) -> DateTime<Utc> {
        let venue = self.estimate.lock().clock.earliest_at(Instant::now());
        venue.unwrap_or_else(Utc::now)
    }

    /// A clock whose one read answered `stamp`, arriving at `received`.
    #[cfg(test)]
    pub(crate) fn sampled(stamp: DateTime<Utc>, received: Instant) -> Self {
        let clock = Self::default();
        clock.estimate.lock().clock.observe(stamp, received);
        clock
    }

    /// The estimate, as a [`VenueClock`] sampled by the reads that succeeded.
    pub(crate) fn venue(&self) -> VenueClock {
        self.estimate.lock().clock
    }

    fn stamp_ms(&self) -> u64 {
        // A time before the epoch cannot be stamped; 0 is refused like any stale stamp.
        u64::try_from(self.stamp().timestamp_millis()).unwrap_or(0)
    }
}

/// Keeps a [`SigningClock`] read from Binance's server time.
///
/// Cheap to clone; clones share the estimate.
///
/// - **First request.** [`ready`](Self::ready) before the client's first request waits for one
///   read. If it fails, requests are stamped with this host's clock until a read succeeds.
/// - **Refresh.** A request that finds the last read [`RESYNC_AFTER`] old or older starts one
///   read in the background, and is sent without waiting for it. A read whose answer took longer
///   than the one held does not replace it (see [`VenueClock::observe`]).
/// - **Suspend.** A request that finds this host's wall clock gone more than [`PAUSE_ALLOWANCE`]
///   further than its monotonic clock since the last read waits for a new read. If that read
///   fails, the next is the background refresh's or a `-1021`'s.
/// - **`-1021`.** [`after_rejection`](Self::after_rejection) waits for a new read, whose answer
///   replaces the estimate rather than adding to it, as the estimate itself may be what Binance
///   refused. Until the answer arrives, and if the read fails, the estimate held stays.
///
/// The reads go through no rate-limit wait of their own: each is started by a request that has
/// passed its own, and weighs 1. Their weight is not counted, and a refusal of one starts no
/// cooldown.
#[derive(Clone)]
pub(crate) struct ServerTime {
    inner: Arc<Inner>,
}

struct Inner {
    clock: SigningClock,
    read: ReadServerTime,
    /// Held across a read, so that requests waiting on one share it.
    reading: tokio::sync::Mutex<()>,
    /// Whether a background read is in flight.
    refreshing: AtomicBool,
}

impl ServerTime {
    /// Keeps `clock` read by `read`.
    pub(crate) fn new(clock: SigningClock, read: ReadServerTime) -> Self {
        Self {
            inner: Arc::new(Inner {
                clock,
                read,
                reading: tokio::sync::Mutex::new(()),
                refreshing: AtomicBool::new(false),
            }),
        }
    }

    /// Called before each request: waits for the first read, or for a new one after a suspend
    /// (see the type's docs), and starts a background read when the last one is [`RESYNC_AFTER`]
    /// old.
    pub(crate) async fn ready(&self) {
        self.ready_at(HostMoment::now()).await;
    }

    async fn ready_at(&self, asked: HostMoment) {
        let Some(last) = self.last_read() else {
            self.read_unless_read_since(asked.instant, false).await;
            return;
        };
        let monotonic = asked.instant.saturating_duration_since(last.instant);
        let wall = asked.time.signed_duration_since(last.time);
        let paused =
            TimeDelta::from_std(monotonic).map_or(TimeDelta::zero(), |monotonic| wall - monotonic);
        if paused > PAUSE_ALLOWANCE {
            debug!(
                paused_ms = paused.num_milliseconds(),
                "This host's wall clock has gone further than its monotonic clock since the last \
                 Binance server time read; reading it again before the request"
            );
            self.read_unless_read_since(asked.instant, false).await;
        } else if monotonic >= RESYNC_AFTER {
            self.refresh_in_background();
        }
    }

    /// Reads Binance's server time afresh, replacing the estimate with the answer, after Binance
    /// refused a request's timestamp (`-1021`). Returns once the read has finished or failed.
    /// Dropped before then, it leaves the estimate as it was.
    ///
    /// A read that finished after this was called serves it, so concurrent refusals share one.
    pub(crate) async fn after_rejection(&self) {
        warn!("Binance refused a request's timestamp (-1021); reading its server time afresh");
        self.read_unless_read_since(Instant::now(), true).await;
    }

    /// The most this host's clock leads Binance's now, from the reads so far (see
    /// [`VenueClock::host_lead`]); negative when it trails. `None` before a read has succeeded.
    pub(crate) fn host_lead(&self) -> Option<TimeDelta> {
        self.inner
            .clock
            .estimate
            .lock()
            .clock
            .host_lead(Utc::now(), Instant::now())
    }

    /// A clock read, at once, from a server whose time is always `lead` behind this host's.
    #[cfg(test)]
    pub(crate) fn trailing_host(lead: TimeDelta) -> Self {
        let lead_ms = lead.num_milliseconds();
        let read: ReadServerTime =
            Arc::new(move || Box::pin(async move { Ok(Utc::now().timestamp_millis() - lead_ms) }));
        Self::new(SigningClock::default(), read)
    }

    /// The clock this keeps read.
    pub(crate) fn clock(&self) -> &SigningClock {
        &self.inner.clock
    }

    fn last_read(&self) -> Option<HostMoment> {
        self.inner.clock.estimate.lock().last_read
    }

    /// Reads, unless a read finished at or after `since` while this waited for the one before.
    /// With `replace`, a successful answer replaces the estimate held.
    async fn read_unless_read_since(&self, since: Instant, replace: bool) {
        let _reading = self.inner.reading.lock().await;
        if self.last_read().is_some_and(|last| last.instant >= since) {
            return;
        }
        self.read(replace).await;
    }

    /// Starts a read in the background, unless one is already in flight.
    fn refresh_in_background(&self) {
        if self.inner.refreshing.swap(true, Ordering::AcqRel) {
            return;
        }
        let this = self.clone();
        tokio::spawn(async move {
            // Cleared however the task ends, so a later request can start another.
            let _refreshing = ClearOnDrop(&this.inner.refreshing);
            let since = Instant::now();
            this.read_unless_read_since(since, false).await;
        });
    }

    /// One read, recorded in the estimate, whose answer replaces the estimate held with
    /// `replace`. The caller holds `reading`.
    async fn read(&self, replace: bool) {
        let sent = Instant::now();
        let result = tokio::time::timeout(READ_TIMEOUT, (self.inner.read)()).await;
        let received = HostMoment::now();
        let round_trip = received.instant.saturating_duration_since(sent);
        let mut estimate = self.inner.clock.estimate.lock();
        estimate.last_read = Some(received);
        let received = received.instant;
        let millis = match result {
            Ok(Ok(millis)) => millis,
            Ok(Err(error)) => {
                let held = estimate.clock.earliest_at(received).is_some();
                drop(estimate);
                warn!(
                    error = %format_args!("{error:#}"),
                    signing_on = if held { "the previous estimate" } else { "this host's clock" },
                    "Binance server time read failed"
                );
                return;
            }
            Err(_elapsed) => {
                let held = estimate.clock.earliest_at(received).is_some();
                drop(estimate);
                warn!(
                    timeout_ms = READ_TIMEOUT.as_millis(),
                    signing_on = if held {
                        "the previous estimate"
                    } else {
                        "this host's clock"
                    },
                    "Binance server time read timed out"
                );
                return;
            }
        };
        let Some(server_time) = DateTime::<Utc>::from_timestamp_millis(millis) else {
            drop(estimate);
            warn!(millis, "Binance server time is out of range; ignored");
            return;
        };
        if replace {
            estimate.clock = VenueClock::new();
        }
        // Stamped before the answer arrived, so a bound on Binance's clock at its arrival.
        estimate.clock.observe(server_time, received);
        let lead = estimate.clock.host_lead(Utc::now(), received);
        drop(estimate);
        let host_lead_ms = lead.map(|lead| lead.num_milliseconds());
        let round_trip_ms = round_trip.as_millis();
        if lead.is_some_and(clocks_apart) {
            warn!(
                host_lead_ms,
                round_trip_ms,
                "This host's clock is off Binance's by 1 s or more; signed requests are stamped \
                 on Binance's clock as read"
            );
        } else {
            debug!(host_lead_ms, round_trip_ms, "Binance server time read");
        }
    }
}

/// Whether a host clock leading Binance's by `lead` (trailing when negative) is far enough off it
/// for a read to warn: as far as Binance refuses a stamp ahead of its clock.
fn clocks_apart(lead: TimeDelta) -> bool {
    lead.abs() >= LEAD_WARN
}

/// Clears its flag when dropped.
struct ClearOnDrop<'a>(&'a AtomicBool);

impl Drop for ClearOnDrop<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use crate::client::binance::shared::{
        RateLimitTracker, RequestKind, WeightPool, is_timestamp_rejection, rest_read_with_retry,
    };
    use binance_sdk::{
        common::{config::ConfigurationRestApi, errors::ConnectorError},
        spot::{SpotRestApi, rest_api::GetAccountParams},
    };
    use serde_json::json;
    use std::sync::atomic::AtomicUsize;
    use wiremock::{
        Mock, MockServer, Request, ResponseTemplate,
        matchers::{method, path},
    };

    const SERVER_MS: i64 = 1_800_000_000_000;

    /// A source answering `SERVER_MS` plus the time elapsed on tokio's clock since `base`, after
    /// `delay`, counting its reads.
    fn source(
        base: Instant,
        delay: Duration,
        reads: Arc<AtomicUsize>,
    ) -> (SigningClock, ServerTime) {
        let clock = SigningClock::default();
        let read: ReadServerTime = Arc::new(move || {
            let reads = Arc::clone(&reads);
            Box::pin(async move {
                reads.fetch_add(1, Ordering::SeqCst);
                let elapsed = Instant::now().saturating_duration_since(base);
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
                Ok(SERVER_MS + i64::try_from(elapsed.as_millis()).unwrap())
            })
        });
        (clock.clone(), ServerTime::new(clock, read))
    }

    fn failing(reads: Arc<AtomicUsize>) -> (SigningClock, ServerTime) {
        let clock = SigningClock::default();
        let read: ReadServerTime = Arc::new(move || {
            let reads = Arc::clone(&reads);
            Box::pin(async move {
                reads.fetch_add(1, Ordering::SeqCst);
                Err(anyhow::anyhow!("unreachable"))
            })
        });
        (clock.clone(), ServerTime::new(clock, read))
    }

    /// The module docs, the CHANGELOG and the clients' docs state these as numbers. Change them
    /// only together with those docs.
    #[test]
    fn the_resync_interval_and_warning_threshold_are_as_documented() {
        assert_eq!(RESYNC_AFTER, Duration::from_secs(300));
        assert_eq!(LEAD_WARN, TimeDelta::seconds(1));
        assert_eq!(PAUSE_ALLOWANCE, TimeDelta::seconds(1));
    }

    #[tokio::test(start_paused = true)]
    async fn the_first_request_waits_for_a_read_and_is_stamped_on_the_servers_clock() {
        let reads = Arc::new(AtomicUsize::new(0));
        let base = Instant::now();
        let (clock, server_time) = source(base, Duration::from_millis(40), Arc::clone(&reads));

        server_time.ready().await;

        assert_eq!(reads.load(Ordering::SeqCst), 1);
        // Read at `base`, answered 40 ms later: the stamp is the server's time when it answered.
        assert_eq!(clock.stamp_ms(), u64::try_from(SERVER_MS).unwrap());
    }

    #[tokio::test(start_paused = true)]
    async fn the_stamp_counts_on_from_the_read_on_the_monotonic_clock() {
        let reads = Arc::new(AtomicUsize::new(0));
        let (clock, server_time) = source(Instant::now(), Duration::ZERO, reads);
        server_time.ready().await;

        tokio::time::advance(Duration::from_secs(10)).await;

        // 10 s on, less 1 ms of drift allowance at 100 ppm.
        assert_eq!(
            clock.stamp_ms(),
            u64::try_from(SERVER_MS + 10_000 - 1).unwrap()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn before_a_read_the_stamp_is_this_hosts_clock() {
        let clock = SigningClock::default();
        let before = Utc::now().timestamp_millis();

        let stamp = i64::try_from(clock.stamp_ms()).unwrap();

        assert!(stamp >= before && stamp <= Utc::now().timestamp_millis());
    }

    #[tokio::test(start_paused = true)]
    async fn concurrent_first_requests_share_one_read() {
        let reads = Arc::new(AtomicUsize::new(0));
        let (_, server_time) = source(
            Instant::now(),
            Duration::from_millis(40),
            Arc::clone(&reads),
        );

        tokio::join!(
            server_time.ready(),
            server_time.ready(),
            server_time.ready()
        );

        assert_eq!(reads.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_first_read_is_not_retried_by_every_request() {
        let reads = Arc::new(AtomicUsize::new(0));
        let (clock, server_time) = failing(Arc::clone(&reads));

        server_time.ready().await;
        server_time.ready().await;

        assert_eq!(reads.load(Ordering::SeqCst), 1);
        assert_eq!(server_time.host_lead(), None);
        // Still this host's clock.
        let now = Utc::now().timestamp_millis();
        assert!(i64::try_from(clock.stamp_ms()).unwrap() <= now);
    }

    #[tokio::test(start_paused = true)]
    async fn a_read_older_than_the_resync_interval_refreshes_in_the_background() {
        let reads = Arc::new(AtomicUsize::new(0));
        let (_, server_time) = source(Instant::now(), Duration::ZERO, Arc::clone(&reads));
        server_time.ready().await;

        tokio::time::advance(RESYNC_AFTER - Duration::from_millis(1)).await;
        server_time.ready().await;
        tokio::task::yield_now().await;
        assert_eq!(reads.load(Ordering::SeqCst), 1);

        tokio::time::advance(Duration::from_millis(1)).await;
        server_time.ready().await;
        server_time.ready().await;
        // The background read runs once the request has gone ahead.
        assert_eq!(reads.load(Ordering::SeqCst), 1);
        for _ in 0..4 {
            tokio::task::yield_now().await;
        }
        assert_eq!(reads.load(Ordering::SeqCst), 2);
        assert!(!server_time.inner.refreshing.load(Ordering::Acquire));
    }

    #[tokio::test(start_paused = true)]
    async fn a_rejection_drops_the_estimate_and_reads_afresh() {
        let reads = Arc::new(AtomicUsize::new(0));
        let clock = SigningClock::default();
        let answers = Arc::new(parking_lot::Mutex::new(vec![SERVER_MS - 60_000, SERVER_MS]));
        let read: ReadServerTime = {
            let reads = Arc::clone(&reads);
            Arc::new(move || {
                reads.fetch_add(1, Ordering::SeqCst);
                let answer = answers.lock().pop().unwrap();
                Box::pin(async move { Ok(answer) })
            })
        };
        let server_time = ServerTime::new(clock.clone(), read);
        // The first read answers a time a minute ahead of the second's.
        server_time.ready().await;
        assert_eq!(clock.stamp_ms(), u64::try_from(SERVER_MS).unwrap());

        tokio::time::advance(Duration::from_millis(1)).await;
        server_time.after_rejection().await;

        // A max-filter would keep the earlier, later-bounding sample; the rejection replaced it.
        assert_eq!(reads.load(Ordering::SeqCst), 2);
        assert_eq!(clock.stamp_ms(), u64::try_from(SERVER_MS - 60_000).unwrap());
    }

    /// A rejection whose read is dropped before its answer, or fails, leaves the estimate held:
    /// it is replaced only by an answer.
    #[tokio::test(start_paused = true)]
    async fn a_rejection_keeps_the_estimate_until_a_new_answer_replaces_it() {
        let clock = SigningClock::default();
        let answers = Arc::new(parking_lot::Mutex::new(vec![
            Err(anyhow::anyhow!("unreachable")),
            Ok(SERVER_MS + 60_000),
            Ok(SERVER_MS),
        ]));
        let read: ReadServerTime = Arc::new(move || {
            let answer = answers.lock().pop().unwrap();
            Box::pin(async move {
                tokio::time::sleep(Duration::from_millis(40)).await;
                answer
            })
        });
        let server_time = ServerTime::new(clock.clone(), read);
        server_time.ready().await;
        let held = clock.stamp_ms();

        // Dropped mid-read: the read's answer, a minute ahead, never arrives.
        tokio::time::advance(Duration::from_millis(1)).await;
        let dropped =
            tokio::time::timeout(Duration::from_millis(10), server_time.after_rejection()).await;
        assert!(dropped.is_err());
        // 11 ms on, less the drift allowance, in whole milliseconds.
        assert_eq!(clock.stamp_ms(), held + 10);

        // A read that fails.
        let reads_before = server_time.last_read();
        tokio::time::advance(Duration::from_millis(1)).await;
        server_time.after_rejection().await;
        assert_ne!(server_time.last_read(), reads_before);
        assert_eq!(clock.stamp_ms(), held + 51);
    }

    /// The monotonic clock stops while the host is suspended: a request that finds the wall clock
    /// gone more than a second further since the last read waits for a new one.
    #[tokio::test(start_paused = true)]
    async fn a_request_after_a_suspend_waits_for_a_new_read() {
        let reads = Arc::new(AtomicUsize::new(0));
        let (_, server_time) = source(Instant::now(), Duration::ZERO, Arc::clone(&reads));
        server_time.ready().await;
        let last = server_time.last_read().unwrap();
        let after = |wall_ms: i64| {
            HostMoment::new(
                last.time + TimeDelta::milliseconds(wall_ms),
                last.instant + Duration::from_secs(1),
            )
        };

        // 1 s on the monotonic clock, and 2 s on the wall clock: within the allowance.
        server_time.ready_at(after(2_000)).await;
        assert_eq!(reads.load(Ordering::SeqCst), 1);

        // A suspend of a minute.
        tokio::time::advance(Duration::from_secs(1)).await;
        server_time.ready_at(after(61_000)).await;
        assert_eq!(reads.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn concurrent_rejections_share_one_read() {
        let reads = Arc::new(AtomicUsize::new(0));
        let (_, server_time) = source(
            Instant::now(),
            Duration::from_millis(40),
            Arc::clone(&reads),
        );
        server_time.ready().await;
        tokio::time::advance(Duration::from_millis(1)).await;

        tokio::join!(server_time.after_rejection(), server_time.after_rejection());

        assert_eq!(reads.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn a_read_that_does_not_answer_in_time_leaves_the_estimate_held() {
        let reads = Arc::new(AtomicUsize::new(0));
        let base = Instant::now();
        let (clock, server_time) = source(base, Duration::ZERO, Arc::clone(&reads));
        server_time.ready().await;
        let held = clock.stamp_ms();
        // Every later read hangs past the timeout.
        let slow = ServerTime::new(
            clock.clone(),
            Arc::new(|| Box::pin(std::future::pending::<anyhow::Result<i64>>())),
        );

        tokio::time::advance(RESYNC_AFTER).await;
        slow.read_unless_read_since(Instant::now(), false).await;

        // Counted on from the held sample through the 5 s timeout: 305 s less 30.5 ms of drift
        // allowance, in whole milliseconds.
        assert_eq!(clock.stamp_ms(), held + 305_000 - 31);
        assert!(slow.last_read().is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn the_host_lead_compares_this_hosts_clock_with_the_estimate() {
        let clock = SigningClock::default();
        let host_ms = Utc::now().timestamp_millis();
        let read: ReadServerTime = Arc::new(move || Box::pin(async move { Ok(host_ms - 2_000) }));
        let server_time = ServerTime::new(clock, read);

        server_time.ready().await;

        let lead = server_time.host_lead().unwrap();
        assert!(lead >= TimeDelta::seconds(2) && lead < TimeDelta::seconds(3));
    }

    /// A spot REST client on `server` that stamps its signed requests with `clock`, and a tracker
    /// that keeps `clock` read from `server`.
    fn signing_client_at(server: &MockServer) -> (Arc<RestApi>, RateLimitTracker) {
        let clock = SigningClock::default();
        let rest = Arc::new(SpotRestApi::from_config(
            ConfigurationRestApi::builder()
                .api_key("key")
                .api_secret("secret")
                .base_path(server.uri())
                .timestamp_provider(clock.provider())
                .build()
                .unwrap(),
        ));
        let server_time = ServerTime::new(clock, read_server_time(Arc::clone(&rest)));
        let tracker = RateLimitTracker::new(WeightPool::Spot).with_server_time(Some(server_time));
        (rest, tracker)
    }

    async fn mount_server_time(server: &MockServer, server_ms: i64) {
        Mock::given(method("GET"))
            .and(path("/api/v3/time"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(json!({ "serverTime": server_ms })),
            )
            .mount(server)
            .await;
    }

    async fn read_account(rest: &Arc<RestApi>, tracker: &RateLimitTracker) -> anyhow::Result<()> {
        rest_read_with_retry(rest, tracker, RequestKind::Query, |rest| {
            Box::pin(async move { rest.get_account(GetAccountParams::builder().build()?).await })
        })
        .await
        .map(|_| ())
    }

    async fn requests_to(server: &MockServer, to: &str) -> Vec<Request> {
        let requests = server.received_requests().await.unwrap();
        requests
            .into_iter()
            .filter(|r| r.url.path() == to)
            .collect()
    }

    fn timestamp_of(request: &Request) -> i64 {
        let (_, stamp) = request
            .url
            .query_pairs()
            .find(|(key, _)| key == "timestamp")
            .unwrap();
        stamp.parse().unwrap()
    }

    /// The SDK takes each signed request's timestamp from the provider: a request sent after the
    /// first read is stamped on the server's clock, not on this host's an hour ahead of it.
    #[tokio::test]
    async fn a_signed_request_is_stamped_on_the_server_time_read_before_it() {
        let server = MockServer::start().await;
        let server_ms = Utc::now().timestamp_millis() - 3_600_000;
        mount_server_time(&server, server_ms).await;
        Mock::given(method("GET"))
            .and(path("/api/v3/account"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
            .mount(&server)
            .await;
        let (rest, tracker) = signing_client_at(&server);

        read_account(&rest, &tracker).await.unwrap();

        let requests = server.received_requests().await.unwrap();
        assert_eq!(requests[0].url.path(), "/api/v3/time");
        let stamp = timestamp_of(&requests_to(&server, "/api/v3/account").await[0]);
        assert!(
            (server_ms..server_ms + 5_000).contains(&stamp),
            "stamped {stamp}, server time read {server_ms}"
        );
        assert!(tracker.host_clock_lead().unwrap() > TimeDelta::minutes(59));
    }

    /// A `-1021` reads the server time again before the error returns, so a retry is stamped on
    /// the clock as read again.
    #[tokio::test]
    async fn a_refused_timestamp_reads_the_server_time_again_before_the_error_returns() {
        let server = MockServer::start().await;
        mount_server_time(&server, Utc::now().timestamp_millis()).await;
        Mock::given(method("GET"))
            .and(path("/api/v3/account"))
            .respond_with(ResponseTemplate::new(400).set_body_json(json!({
                "code": -1021,
                "msg": "Timestamp for this request was 1000ms ahead of the server's time."
            })))
            .mount(&server)
            .await;
        let (rest, tracker) = signing_client_at(&server);

        let error = read_account(&rest, &tracker).await.unwrap_err();

        assert!(is_timestamp_rejection(&error), "{error:#}");
        assert_eq!(requests_to(&server, "/api/v3/time").await.len(), 2);
    }

    /// Any other refusal leaves the estimate alone.
    #[tokio::test]
    async fn another_refusal_does_not_read_the_server_time_again() {
        let server = MockServer::start().await;
        mount_server_time(&server, Utc::now().timestamp_millis()).await;
        Mock::given(method("GET"))
            .and(path("/api/v3/account"))
            .respond_with(ResponseTemplate::new(400).set_body_json(json!({
                "code": -1022,
                "msg": "Signature for this request is not valid."
            })))
            .mount(&server)
            .await;
        let (rest, tracker) = signing_client_at(&server);

        let error = read_account(&rest, &tracker).await.unwrap_err();

        assert!(!is_timestamp_rejection(&error), "{error:#}");
        assert_eq!(requests_to(&server, "/api/v3/time").await.len(), 1);
    }

    /// A server answering this host's time less `behind_ms`, 40 ms after it is asked.
    fn behind_host(behind_ms: i64) -> ServerTime {
        let read: ReadServerTime = Arc::new(move || {
            Box::pin(async move {
                tokio::time::sleep(Duration::from_millis(40)).await;
                Ok(Utc::now().timestamp_millis() - behind_ms)
            })
        });
        ServerTime::new(SigningClock::default(), read)
    }

    #[test]
    fn a_read_warns_from_a_second_apart_either_way() {
        for (lead_ms, apart) in [
            (0, false),
            (999, false),
            (1_000, true),
            (-999, false),
            (-1_000, true),
        ] {
            assert_eq!(
                clocks_apart(TimeDelta::milliseconds(lead_ms)),
                apart,
                "{lead_ms} ms"
            );
        }
    }

    /// A `-1021` during a rate-limit cooldown reads nothing: the read would be refused too.
    #[tokio::test(start_paused = true)]
    async fn a_refused_timestamp_during_a_cooldown_reads_nothing() {
        let reads = Arc::new(AtomicUsize::new(0));
        let (_, server_time) = source(Instant::now(), Duration::ZERO, Arc::clone(&reads));
        let tracker = RateLimitTracker::new(WeightPool::Spot).with_server_time(Some(server_time));
        tracker.wait_if_blocked(RequestKind::Order).await.unwrap();
        let refused = anyhow::Error::new(ConnectorError::BadRequestError {
            msg: "Timestamp for this request is outside of the recvWindow.".to_owned(),
            code: Some(-1021),
        });

        tracker.on_rate_limited(Some(Duration::from_secs(60)));
        tokio::time::advance(Duration::from_millis(1)).await;
        tracker.after_failure(&refused).await;
        assert_eq!(reads.load(Ordering::SeqCst), 1);

        tokio::time::advance(Duration::from_secs(60)).await;
        tracker.after_failure(&refused).await;
        assert_eq!(reads.load(Ordering::SeqCst), 2);
    }

    /// The tracker's signing clock is the one its server time reads into, which the account
    /// streams bound the venue's clock by.
    #[tokio::test(start_paused = true)]
    async fn the_trackers_signing_clock_is_the_one_read() {
        let tracker =
            RateLimitTracker::new(WeightPool::Spot).with_server_time(Some(behind_host(0)));
        assert_eq!(tracker.host_clock_lead(), None);

        tracker.wait_if_blocked(RequestKind::Order).await.unwrap();

        let signing = tracker.signing_clock().unwrap();
        assert!(signing.venue().earliest_at(Instant::now()).is_some());
        assert!(tracker.host_clock_lead().unwrap().abs() < TimeDelta::seconds(1));
        assert!(
            RateLimitTracker::new(WeightPool::Spot)
                .signing_clock()
                .is_none()
        );
    }
}
