//! Live candles from IB: the request for a candle subscription on
//! [`IbkrMarketStream`](crate::exchange::ibkr::IbkrMarketStream).
//!
//! A candle subscription asks IB for its historical bars kept up to date, the same bars
//! [`IbkrHistoricalData::fetch_candles`](crate::exchange::ibkr::historical::IbkrHistoricalData::fetch_candles)
//! returns, so that each live candle matches the one a later read of history gives for that
//! period. It yields **completed bars only**, each once, never revised.
//!
//! # When a bar is complete
//!
//! IB marks no bar final. It sends the forming bar again every few seconds, and a bar is taken as
//! complete on whichever comes first:
//! - an update for it that arrives after its end (IB's closing update);
//! - the first update of a later bar;
//! - [`BAR_SETTLE`] after its end, for a bar whose last seconds had no update.
//!
//! An update for a bar already emitted is dropped with a warning: the emitted candle missed the
//! trades it adds. The end of a bar is measured on IB's clock, estimated from IB's server time;
//! see [`IbkrMarketStream`](crate::exchange::ibkr::IbkrMarketStream) § "Candles".

use chrono::{DateTime, TimeDelta, Utc};
use ibapi::market_data::historical::{BarSize, Duration, WhatToShow};
use serde::{Deserialize, Serialize};

/// How long after a bar's end it is emitted when IB has sent no closing update for it.
///
/// IB sends a bar's closing update shortly after its end, and none when the bar's last seconds
/// had no trades. That update is expected about half a second after the bar's end; until a live
/// check measures it, this leaves a wide margin.
pub const BAR_SETTLE: TimeDelta = TimeDelta::seconds(2);

/// The length of a live candle.
///
/// Only intraday sizes whose bars line up with a session starting on the half hour: IB's
/// keepUpToDate bars need at least 5 seconds, and a daily or longer bar has no end the library
/// could time (its [`close_time`](crate::subscription::candle::Candle::close_time) is the next
/// UTC midnight, not the session close). Longer intraday sizes may follow once it is known how IB
/// aligns them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub enum IbkrLiveBarSize {
    /// 5 seconds.
    Sec5,
    /// 10 seconds.
    Sec10,
    /// 15 seconds.
    Sec15,
    /// 30 seconds.
    Sec30,
    /// 1 minute.
    Min1,
    /// 2 minutes.
    Min2,
    /// 3 minutes.
    Min3,
    /// 5 minutes.
    Min5,
    /// 10 minutes.
    Min10,
    /// 15 minutes.
    Min15,
    /// 30 minutes.
    Min30,
}

impl IbkrLiveBarSize {
    /// The length of one bar.
    pub fn duration(self) -> TimeDelta {
        TimeDelta::seconds(self.seconds())
    }

    fn seconds(self) -> i64 {
        match self {
            Self::Sec5 => 5,
            Self::Sec10 => 10,
            Self::Sec15 => 15,
            Self::Sec30 => 30,
            Self::Min1 => 60,
            Self::Min2 => 120,
            Self::Min3 => 180,
            Self::Min5 => 300,
            Self::Min10 => 600,
            Self::Min15 => 900,
            Self::Min30 => 1800,
        }
    }

    /// The open of the bar `time` falls in: `time` floored to a multiple of the bar's length.
    pub(super) fn bar_open(self, time: DateTime<Utc>) -> DateTime<Utc> {
        let seconds = time.timestamp();
        let floored = seconds - seconds.rem_euclid(self.seconds());
        // A whole second at or before `time`, so within the range `time` is in.
        DateTime::from_timestamp(floored, 0).unwrap_or(time)
    }
}

impl From<IbkrLiveBarSize> for BarSize {
    fn from(value: IbkrLiveBarSize) -> Self {
        match value {
            IbkrLiveBarSize::Sec5 => BarSize::Sec5,
            IbkrLiveBarSize::Sec10 => BarSize::Sec10,
            IbkrLiveBarSize::Sec15 => BarSize::Sec15,
            IbkrLiveBarSize::Sec30 => BarSize::Sec30,
            IbkrLiveBarSize::Min1 => BarSize::Min,
            IbkrLiveBarSize::Min2 => BarSize::Min2,
            IbkrLiveBarSize::Min3 => BarSize::Min3,
            IbkrLiveBarSize::Min5 => BarSize::Min5,
            IbkrLiveBarSize::Min10 => BarSize::Min10,
            IbkrLiveBarSize::Min15 => BarSize::Min15,
            IbkrLiveBarSize::Min30 => BarSize::Min30,
        }
    }
}

/// Which prices a live candle is built from.
///
/// Only the series that are price bars. IB's other kinds are not: its trading schedule, fee rates
/// and split-adjusted series, and its volatility series. IB's combined bid-and-ask series is left
/// out too: its high comes from the ask and its low from the bid, and it carries no volume. The
/// [`Bid`](Self::Bid) and [`Ask`](Self::Ask) series each take one side alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub enum IbkrBarSource {
    /// Trade prices, with volume and trade count.
    Trades,
    /// The bid/ask midpoint. No volume or trade count.
    MidPoint,
    /// Bid prices. No volume or trade count.
    Bid,
    /// Ask prices. No volume or trade count.
    Ask,
    /// Aggregated trades, which IB requires for crypto contracts in place of
    /// [`Trades`](Self::Trades).
    AggTrades,
}

impl From<IbkrBarSource> for WhatToShow {
    fn from(value: IbkrBarSource) -> Self {
        match value {
            IbkrBarSource::Trades => WhatToShow::Trades,
            IbkrBarSource::MidPoint => WhatToShow::MidPoint,
            IbkrBarSource::Bid => WhatToShow::Bid,
            IbkrBarSource::Ask => WhatToShow::Ask,
            IbkrBarSource::AggTrades => WhatToShow::AggTrades,
        }
    }
}

/// What a candle subscription asks for. Build one with [`new`](Self::new) and the `with_*`
/// methods.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub struct IbkrCandleRequest {
    /// The length of each candle.
    pub bar_size: IbkrLiveBarSize,
    /// Which prices the candles are built from.
    pub source: IbkrBarSource,
    /// Whether the candles cover regular trading hours only, rather than the extended session as
    /// well. Default `true`, as in a history read.
    #[serde(default = "default_regular_trading_hours_only")]
    pub regular_trading_hours_only: bool,
    /// The first candle wanted is the first opening at or after this time, to continue from a
    /// history read: pass the
    /// [`close_time`](crate::subscription::candle::Candle::close_time) of the last completed
    /// candle that read returned. Default `None`: start with the bar forming when the stream
    /// connects, which is the first candle emitted.
    ///
    /// A history read's last candle may still be forming: drop it before taking its successor's
    /// open, or keep it and pass its own open (`close_time` minus the bar's length) to have it
    /// emitted again once complete.
    ///
    /// It must not be later than one bar from the time the stream is made
    /// ([`DataError::InvalidSubscription`](crate::error::DataError::InvalidSubscription)). IB limits
    /// how far back a request may reach for each bar size; a `since` past that is refused by IB,
    /// which ends the subscription.
    #[serde(default)]
    pub since: Option<DateTime<Utc>>,
}

fn default_regular_trading_hours_only() -> bool {
    true
}

impl IbkrCandleRequest {
    /// Candles of `bar_size` built from `source`, in regular trading hours, starting with the bar
    /// forming when the stream connects.
    pub fn new(bar_size: IbkrLiveBarSize, source: IbkrBarSource) -> Self {
        Self {
            bar_size,
            source,
            regular_trading_hours_only: default_regular_trading_hours_only(),
            since: None,
        }
    }

    /// Cover regular trading hours only, or the extended session as well; see
    /// [`regular_trading_hours_only`](Self::regular_trading_hours_only).
    pub fn with_regular_trading_hours_only(mut self, regular_trading_hours_only: bool) -> Self {
        self.regular_trading_hours_only = regular_trading_hours_only;
        self
    }

    /// Start with the first candle opening at or after `since`; see [`since`](Self::since).
    pub fn with_since(mut self, since: DateTime<Utc>) -> Self {
        self.since = Some(since);
        self
    }

    /// Why the request cannot be made at `now`, if it cannot.
    pub(super) fn refusal(&self, now: DateTime<Utc>) -> Option<String> {
        let since = self.since?;
        let latest = now
            .checked_add_signed(self.bar_size.duration())
            .unwrap_or(DateTime::<Utc>::MAX_UTC);
        (since > latest)
            .then(|| format!("candle `since` {since} is later than one bar from now ({now})"))
    }
}

/// The IB duration of a keepUpToDate request whose history must reach back to the bar opening at
/// `first_open`, made at `now`: the span from `first_open` to the end of the bar forming at
/// `now`, and at least two bars.
///
/// IB counts seconds (`S`, at most a day) in trading time and days (`D`) in whole sessions, and
/// trading time never exceeds wall-clock time, so a wall-clock span over-fetches rather than falls
/// short; one more day covers a span starting partway through a session. The bars before
/// `first_open` are dropped on arrival.
pub(super) fn history_duration(
    first_open: DateTime<Utc>,
    now: DateTime<Utc>,
    bar_size: IbkrLiveBarSize,
) -> Duration {
    const SECONDS_PER_DAY: i64 = 86_400;
    let bar = bar_size.seconds();
    let span = now
        .signed_duration_since(first_open)
        .num_seconds()
        .saturating_add(bar)
        .max(2 * bar);
    if span <= SECONDS_PER_DAY {
        // At most a day, so within `i32`.
        return Duration::seconds(i32::try_from(span).unwrap_or(i32::MAX));
    }
    let days = (span / SECONDS_PER_DAY)
        .saturating_add(i64::from(span % SECONDS_PER_DAY != 0))
        .saturating_add(1);
    Duration::days(i32::try_from(days).unwrap_or(i32::MAX))
}

#[cfg(test)]
// Test code: a panic is the failure report.
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn at(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    const ALL: [IbkrLiveBarSize; 11] = [
        IbkrLiveBarSize::Sec5,
        IbkrLiveBarSize::Sec10,
        IbkrLiveBarSize::Sec15,
        IbkrLiveBarSize::Sec30,
        IbkrLiveBarSize::Min1,
        IbkrLiveBarSize::Min2,
        IbkrLiveBarSize::Min3,
        IbkrLiveBarSize::Min5,
        IbkrLiveBarSize::Min10,
        IbkrLiveBarSize::Min15,
        IbkrLiveBarSize::Min30,
    ];

    #[test]
    fn each_size_maps_to_ibapis_bar_size_of_the_same_length() {
        let expected = [
            (BarSize::Sec5, 5),
            (BarSize::Sec10, 10),
            (BarSize::Sec15, 15),
            (BarSize::Sec30, 30),
            (BarSize::Min, 60),
            (BarSize::Min2, 120),
            (BarSize::Min3, 180),
            (BarSize::Min5, 300),
            (BarSize::Min10, 600),
            (BarSize::Min15, 900),
            (BarSize::Min30, 1800),
        ];
        for (size, (bar_size, seconds)) in ALL.into_iter().zip(expected) {
            assert_eq!(BarSize::from(size), bar_size);
            assert_eq!(size.duration(), TimeDelta::seconds(seconds), "{size:?}");
        }
    }

    #[test]
    fn every_size_divides_half_an_hour() {
        for size in ALL {
            assert_eq!(1800 % size.seconds(), 0, "{size:?}");
        }
    }

    #[test]
    fn each_source_maps_to_ibapis_what_to_show() {
        let pairs = [
            (IbkrBarSource::Trades, WhatToShow::Trades),
            (IbkrBarSource::MidPoint, WhatToShow::MidPoint),
            (IbkrBarSource::Bid, WhatToShow::Bid),
            (IbkrBarSource::Ask, WhatToShow::Ask),
            (IbkrBarSource::AggTrades, WhatToShow::AggTrades),
        ];
        for (source, what_to_show) in pairs {
            assert_eq!(WhatToShow::from(source), what_to_show);
        }
    }

    #[test]
    fn bar_open_floors_to_the_bars_length() {
        let size = IbkrLiveBarSize::Min5;
        assert_eq!(
            size.bar_open(at("2026-11-23T14:37:59.900Z")),
            at("2026-11-23T14:35:00Z")
        );
        assert_eq!(
            size.bar_open(at("2026-11-23T14:35:00Z")),
            at("2026-11-23T14:35:00Z")
        );
        assert_eq!(
            IbkrLiveBarSize::Sec15.bar_open(at("2026-11-23T14:35:29Z")),
            at("2026-11-23T14:35:15Z")
        );
    }

    #[test]
    fn bar_open_floors_a_time_before_the_epoch_down() {
        assert_eq!(
            IbkrLiveBarSize::Min1.bar_open(at("1969-12-31T23:59:30Z")),
            at("1969-12-31T23:59:00Z")
        );
    }

    #[test]
    fn a_new_request_covers_regular_hours_from_the_forming_bar() {
        let request = IbkrCandleRequest::new(IbkrLiveBarSize::Min1, IbkrBarSource::Trades);
        assert!(request.regular_trading_hours_only);
        assert_eq!(request.since, None);
        let request = request
            .with_regular_trading_hours_only(false)
            .with_since(at("2026-11-23T14:30:00Z"));
        assert!(!request.regular_trading_hours_only);
        assert_eq!(request.since, Some(at("2026-11-23T14:30:00Z")));
    }

    #[test]
    fn a_request_deserializes_with_its_defaults() {
        let request: IbkrCandleRequest =
            serde_json::from_str(r#"{"bar_size":"Min1","source":"Trades"}"#).unwrap();
        assert_eq!(
            request,
            IbkrCandleRequest::new(IbkrLiveBarSize::Min1, IbkrBarSource::Trades)
        );
    }

    #[test]
    fn since_up_to_one_bar_ahead_is_accepted_and_later_refused() {
        let now = at("2026-11-23T14:30:10Z");
        let request = IbkrCandleRequest::new(IbkrLiveBarSize::Min1, IbkrBarSource::Trades);
        assert_eq!(request.refusal(now), None);
        let ok = request.clone().with_since(at("2026-11-23T14:31:10Z"));
        assert_eq!(ok.refusal(now), None);
        let past = request.clone().with_since(at("2026-11-20T14:31:00Z"));
        assert_eq!(past.refusal(now), None);
        let late = request.with_since(at("2026-11-23T14:31:11Z"));
        let reason = late.refusal(now).unwrap();
        assert!(reason.contains("later than one bar"), "{reason}");
    }

    #[test]
    fn history_duration_covers_the_span_and_the_forming_bar_in_seconds() {
        let now = at("2026-11-23T14:37:20Z");
        let duration = history_duration(at("2026-11-23T14:30:00Z"), now, IbkrLiveBarSize::Min1);
        // 440 s from the first open, plus the forming bar.
        assert_eq!(duration, Duration::seconds(500));
    }

    #[test]
    fn history_duration_asks_for_at_least_two_bars() {
        let now = at("2026-11-23T14:30:01Z");
        let duration = history_duration(at("2026-11-23T14:30:00Z"), now, IbkrLiveBarSize::Min1);
        assert_eq!(duration, Duration::seconds(120));
        let ahead = history_duration(at("2026-11-23T14:31:00Z"), now, IbkrLiveBarSize::Min1);
        assert_eq!(ahead, Duration::seconds(120));
    }

    #[test]
    fn history_duration_up_to_a_day_stays_in_seconds() {
        let now = at("2026-11-24T14:29:00Z");
        let duration = history_duration(at("2026-11-23T14:30:00Z"), now, IbkrLiveBarSize::Min1);
        assert_eq!(duration, Duration::seconds(86_400));
    }

    #[test]
    fn history_duration_past_a_day_counts_days_with_one_more() {
        let now = at("2026-11-24T14:29:01Z");
        let duration = history_duration(at("2026-11-23T14:30:00Z"), now, IbkrLiveBarSize::Min1);
        // 86,401 s: two days rounded up, plus one.
        assert_eq!(duration, Duration::days(3));
        let exact = history_duration(
            at("2026-11-21T14:29:00Z"),
            at("2026-11-23T14:28:00Z"),
            IbkrLiveBarSize::Min1,
        );
        // Exactly two days: two, plus one.
        assert_eq!(exact, Duration::days(3));
    }
}
