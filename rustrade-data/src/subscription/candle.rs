use super::SubscriptionKind;
use chrono::{DateTime, Duration, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use std::str::FromStr;
use thiserror::Error;

/// Barter [`Subscription`](super::Subscription) [`SubscriptionKind`] that yields [`Candle`]
/// [`MarketEvent<T>`](crate::event::MarketEvent) events.
///
/// The [`interval`](Self::interval) is intrinsic to a candle subscription — it is
/// the resolution being streamed, so there is no meaningful default (a phantom
/// "1m" default is a silent-bug footgun); the field is always explicit.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
pub struct Candles {
    /// The candle resolution to subscribe to. See [`CandleInterval`].
    pub interval: CandleInterval,
}

impl SubscriptionKind for Candles {
    type Event = Candle;

    /// Returns the fixed kind tag `"candles"`, independent of [`interval`](Self::interval).
    /// The tag identifies the subscription *kind* for routing and stays stable across
    /// resolutions; it is **not** the interval. For the resolution string use
    /// [`CandleInterval::as_str`] on the [`interval`](Self::interval) field — note that
    /// [`Display`](std::fmt::Display) for `Candles` also yields only `"candles"`.
    fn as_str(&self) -> &'static str {
        "candles"
    }
}

impl std::fmt::Display for Candles {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use chrono::Duration;
    use rust_decimal_macros::dec;

    /// Parse an RFC3339 UTC instant in tests.
    fn dt(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    /// Build a test [`Candle`] closing at the given RFC3339 instant.
    fn bar(
        close_time: &str,
        open: Decimal,
        high: Decimal,
        low: Decimal,
        close: Decimal,
        volume: Decimal,
        trade_count: u64,
    ) -> Candle {
        Candle {
            close_time: dt(close_time),
            open,
            high,
            low,
            close,
            volume: Some(volume),
            trade_count: Some(trade_count),
        }
    }

    #[test]
    fn fixed_step_adds_duration_exactly() {
        let open = dt("2024-01-15T12:00:00Z");
        assert_eq!(
            close_time_from_open(open, IntervalStep::Fixed(Duration::minutes(1))),
            Some(dt("2024-01-15T12:01:00Z"))
        );
        assert_eq!(
            close_time_from_open(open, IntervalStep::Fixed(Duration::hours(1))),
            Some(dt("2024-01-15T13:00:00Z"))
        );
        // Daily/weekly are exact fixed durations in UTC (no DST).
        assert_eq!(
            close_time_from_open(
                dt("2024-01-15T00:00:00Z"),
                IntervalStep::Fixed(Duration::days(1))
            ),
            Some(dt("2024-01-16T00:00:00Z"))
        );
        assert_eq!(
            close_time_from_open(
                dt("2024-01-15T00:00:00Z"),
                IntervalStep::Fixed(Duration::weeks(1))
            ),
            Some(dt("2024-01-22T00:00:00Z"))
        );
    }

    #[test]
    fn fixed_daily_step_crosses_month_boundary() {
        // A Jan 31 daily bar closes at Feb 1 00:00 UTC via Fixed(1 day).
        assert_eq!(
            close_time_from_open(
                dt("2024-01-31T00:00:00Z"),
                IntervalStep::Fixed(Duration::days(1))
            ),
            Some(dt("2024-02-01T00:00:00Z"))
        );
    }

    #[test]
    fn months_step_uses_calendar_arithmetic() {
        // Jan -> Feb (not +30 days = Jan 31).
        assert_eq!(
            close_time_from_open(dt("2024-01-01T00:00:00Z"), IntervalStep::Months(1)),
            Some(dt("2024-02-01T00:00:00Z"))
        );
        // Leap-year Feb -> Mar (Feb has 29 days in 2024).
        assert_eq!(
            close_time_from_open(dt("2024-02-01T00:00:00Z"), IntervalStep::Months(1)),
            Some(dt("2024-03-01T00:00:00Z"))
        );
        // Quarter = 3 months.
        assert_eq!(
            close_time_from_open(dt("2024-01-01T00:00:00Z"), IntervalStep::Months(3)),
            Some(dt("2024-04-01T00:00:00Z"))
        );
        // Year = 12 months.
        assert_eq!(
            close_time_from_open(dt("2024-01-01T00:00:00Z"), IntervalStep::Months(12)),
            Some(dt("2025-01-01T00:00:00Z"))
        );
    }

    #[test]
    fn months_step_clamps_jan_31_anchor() {
        // Monthly bar opens always land on the 1st from all known producers, so
        // this clamping is unreachable in practice; the test pins chrono's
        // documented behaviour for the variable-length-month edge case.
        // chrono clamps to the last valid day: Jan 31 + 1 month -> Feb 29 (leap year).
        assert_eq!(
            close_time_from_open(dt("2024-01-31T00:00:00Z"), IntervalStep::Months(1)),
            Some(dt("2024-02-29T00:00:00Z"))
        );
    }

    #[test]
    fn overflow_returns_none_not_panic() {
        let max = DateTime::<Utc>::MAX_UTC;
        assert_eq!(close_time_from_open(max, IntervalStep::Months(1)), None);
        assert_eq!(
            close_time_from_open(max, IntervalStep::Fixed(Duration::days(1))),
            None
        );
    }

    #[test]
    fn open_time_from_close_is_inverse() {
        // open = close − interval, for both Fixed and Months steps.
        assert_eq!(
            open_time_from_close(
                dt("2024-01-15T13:00:00Z"),
                IntervalStep::Fixed(Duration::hours(1))
            ),
            Some(dt("2024-01-15T12:00:00Z"))
        );
        // Feb 1 close of a January monthly bar → Jan 1 open.
        assert_eq!(
            open_time_from_close(dt("2024-02-01T00:00:00Z"), IntervalStep::Months(1)),
            Some(dt("2024-01-01T00:00:00Z"))
        );
        // Round-trip identity for the inputs this library actually produces:
        // monthly/quarterly closes always land on a calendar 1st, where chrono's
        // month arithmetic round-trips exactly. (It is NOT a universal identity —
        // `Months` day-clamping is asymmetric for non-1st anchors, e.g.
        // Feb 29 −1mo → Jan 29, +1mo → Feb 29; see `months_step_clamps_jan_31_anchor`.)
        let close = dt("2024-04-01T00:00:00Z");
        let open = open_time_from_close(close, IntervalStep::Months(3)).unwrap();
        assert_eq!(
            close_time_from_open(open, IntervalStep::Months(3)),
            Some(close)
        );
    }

    #[test]
    fn open_time_from_close_underflow_returns_none() {
        let min = DateTime::<Utc>::MIN_UTC;
        assert_eq!(open_time_from_close(min, IntervalStep::Months(1)), None);
        assert_eq!(
            open_time_from_close(min, IntervalStep::Fixed(Duration::days(1))),
            None
        );
    }

    #[test]
    fn candle_interval_all_covers_every_variant_in_ascending_order() {
        // `ALL`'s length is pinned to the variant count by both the
        // `[CandleInterval; 19]` type and this assertion. Full variant *coverage*
        // is not compile-enforced (Rust has no stable variant_count), so keep
        // `ALL` in sync when adding a variant.
        assert_eq!(CandleInterval::ALL.len(), 19);

        // Verify the documented ascending-duration ordering directly via `to_step`.
        // Comparing against the derived `Ord` would be tautological — that order is
        // declaration order, identical to `ALL`'s. Mapping through durations instead
        // actually fails if a variant is listed out of order.
        fn approx_secs(interval: CandleInterval) -> i64 {
            match interval.to_step() {
                IntervalStep::Fixed(d) => d.num_seconds(),
                // Only `Month1` is calendar-based; ~30d keeps it above `Week1` (7d).
                IntervalStep::Months(n) => i64::from(n) * 30 * 24 * 60 * 60,
            }
        }
        for pair in CandleInterval::ALL.windows(2) {
            assert!(
                approx_secs(pair[0]) < approx_secs(pair[1]),
                "ALL must be in strictly ascending duration order: {:?} !< {:?}",
                pair[0],
                pair[1]
            );
        }
    }

    #[test]
    fn candle_interval_display_equals_as_str_for_every_variant() {
        for interval in CandleInterval::ALL {
            assert_eq!(interval.to_string(), interval.as_str());
        }
    }

    #[test]
    fn candle_interval_as_str_matches_binance_exactly() {
        // Case-sensitive: `1M` (month) is the only uppercase form.
        assert_eq!(CandleInterval::Sec1.as_str(), "1s");
        assert_eq!(CandleInterval::Min1.as_str(), "1m");
        assert_eq!(CandleInterval::Hour6.as_str(), "6h");
        assert_eq!(CandleInterval::Month1.as_str(), "1M");
    }

    #[test]
    fn candle_interval_from_str_is_inverse_of_as_str() {
        for interval in CandleInterval::ALL {
            assert_eq!(interval.as_str().parse::<CandleInterval>(), Ok(interval));
        }
    }

    #[test]
    fn candle_interval_from_str_rejects_garbage() {
        assert!("".parse::<CandleInterval>().is_err());
        assert!("7m".parse::<CandleInterval>().is_err());
        // Case-sensitive: `1m` (minute) must not parse as `1M` (month) or vice versa.
        assert_eq!("1m".parse::<CandleInterval>(), Ok(CandleInterval::Min1));
        assert_eq!("1M".parse::<CandleInterval>(), Ok(CandleInterval::Month1));
    }

    #[test]
    fn candle_interval_serde_round_trips_every_variant() {
        for interval in CandleInterval::ALL {
            let json = serde_json::to_string(&interval).unwrap();
            // Serialises as the bare `as_str()` string.
            assert_eq!(json, format!("\"{}\"", interval.as_str()));
            let back: CandleInterval = serde_json::from_str(&json).unwrap();
            assert_eq!(back, interval);
        }
    }

    #[test]
    fn candles_kind_carries_interval_and_serde_round_trips() {
        let kind = Candles {
            interval: CandleInterval::Hour6,
        };
        let json = serde_json::to_string(&kind).unwrap();
        assert_eq!(json, r#"{"interval":"6h"}"#);
        let back: Candles = serde_json::from_str(&json).unwrap();
        assert_eq!(back, kind);
        // `SubscriptionKind::as_str` stays the kind tag, independent of interval.
        assert_eq!(kind.as_str(), "candles");
        assert_eq!(kind.to_string(), "candles");
    }

    #[test]
    fn aggregate_rolls_three_1s_bars_into_one_3s_bar() {
        let bars = [
            bar(
                "2024-01-15T12:00:01Z",
                dec!(10),
                dec!(12),
                dec!(9),
                dec!(11),
                dec!(1),
                5,
            ),
            bar(
                "2024-01-15T12:00:02Z",
                dec!(11),
                dec!(15),
                dec!(10),
                dec!(14),
                dec!(2),
                3,
            ),
            bar(
                "2024-01-15T12:00:03Z",
                dec!(14),
                dec!(16),
                dec!(13),
                dec!(15),
                dec!(3),
                2,
            ),
        ];
        let out = aggregate_candles(&bars, Duration::seconds(1), Duration::seconds(3)).unwrap();
        assert_eq!(
            out,
            vec![bar(
                "2024-01-15T12:00:03Z",
                dec!(10),
                dec!(16),
                dec!(9),
                dec!(15),
                dec!(6),
                10
            )]
        );
    }

    #[test]
    fn aggregate_splits_six_1s_bars_into_two_3s_bars() {
        let bars = [
            bar(
                "2024-01-15T12:00:01Z",
                dec!(10),
                dec!(12),
                dec!(9),
                dec!(11),
                dec!(1),
                1,
            ),
            bar(
                "2024-01-15T12:00:02Z",
                dec!(11),
                dec!(15),
                dec!(10),
                dec!(14),
                dec!(2),
                2,
            ),
            bar(
                "2024-01-15T12:00:03Z",
                dec!(14),
                dec!(16),
                dec!(13),
                dec!(15),
                dec!(3),
                3,
            ),
            bar(
                "2024-01-15T12:00:04Z",
                dec!(15),
                dec!(17),
                dec!(14),
                dec!(16),
                dec!(4),
                4,
            ),
            bar(
                "2024-01-15T12:00:05Z",
                dec!(16),
                dec!(18),
                dec!(15),
                dec!(17),
                dec!(5),
                5,
            ),
            bar(
                "2024-01-15T12:00:06Z",
                dec!(17),
                dec!(19),
                dec!(12),
                dec!(13),
                dec!(6),
                6,
            ),
        ];
        let out = aggregate_candles(&bars, Duration::seconds(1), Duration::seconds(3)).unwrap();
        assert_eq!(
            out,
            vec![
                bar(
                    "2024-01-15T12:00:03Z",
                    dec!(10),
                    dec!(16),
                    dec!(9),
                    dec!(15),
                    dec!(6),
                    6
                ),
                bar(
                    "2024-01-15T12:00:06Z",
                    dec!(15),
                    dec!(19),
                    dec!(12),
                    dec!(13),
                    dec!(15),
                    15
                ),
            ]
        );
    }

    #[test]
    fn aggregate_buckets_snap_to_the_epoch_grid_not_the_first_sample() {
        // Opens land at :01, :02, :03. First-sample anchoring would merge all
        // three into one [:01, :04) bucket; the epoch grid splits them into
        // [:00, :03) and [:03, :06).
        let bars = [
            bar(
                "2024-01-15T12:00:02Z",
                dec!(10),
                dec!(12),
                dec!(9),
                dec!(11),
                dec!(1),
                1,
            ),
            bar(
                "2024-01-15T12:00:03Z",
                dec!(11),
                dec!(15),
                dec!(10),
                dec!(14),
                dec!(2),
                2,
            ),
            bar(
                "2024-01-15T12:00:04Z",
                dec!(14),
                dec!(16),
                dec!(13),
                dec!(15),
                dec!(4),
                3,
            ),
        ];
        let target = Duration::seconds(3);
        let out = aggregate_candles(&bars, Duration::seconds(1), target).unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].close_time, dt("2024-01-15T12:00:03Z"));
        assert_eq!(out[0].volume, Some(dec!(3)));
        assert_eq!(out[1].close_time, dt("2024-01-15T12:00:06Z"));
        assert_eq!(out[1].volume, Some(dec!(4)));
        // Every output boundary sits on the epoch-anchored target grid.
        for candle in &out {
            assert_eq!(
                candle
                    .close_time
                    .timestamp_millis()
                    .rem_euclid(target.num_milliseconds()),
                0
            );
        }
    }

    #[test]
    fn aggregate_partial_bucket_does_not_scale_volume_to_bucket_width() {
        let bars = [bar(
            "2024-01-15T12:00:02Z",
            dec!(10),
            dec!(12),
            dec!(9),
            dec!(11),
            dec!(5),
            4,
        )];
        let out = aggregate_candles(&bars, Duration::seconds(1), Duration::seconds(3)).unwrap();
        assert_eq!(
            out,
            vec![bar(
                "2024-01-15T12:00:03Z",
                dec!(10),
                dec!(12),
                dec!(9),
                dec!(11),
                dec!(5),
                4
            )]
        );
    }

    #[test]
    fn aggregate_omits_fully_empty_buckets() {
        // Bars in the [:00, :03) and [:09, :12) buckets; [:03, :09) is a gap.
        let bars = [
            bar(
                "2024-01-15T12:00:01Z",
                dec!(10),
                dec!(12),
                dec!(9),
                dec!(11),
                dec!(1),
                1,
            ),
            bar(
                "2024-01-15T12:00:11Z",
                dec!(20),
                dec!(22),
                dec!(19),
                dec!(21),
                dec!(2),
                2,
            ),
        ];
        let out = aggregate_candles(&bars, Duration::seconds(1), Duration::seconds(3)).unwrap();
        // No synthetic bars for the empty buckets: gap policy is the caller's.
        assert_eq!(
            out,
            vec![
                bar(
                    "2024-01-15T12:00:03Z",
                    dec!(10),
                    dec!(12),
                    dec!(9),
                    dec!(11),
                    dec!(1),
                    1
                ),
                bar(
                    "2024-01-15T12:00:12Z",
                    dec!(20),
                    dec!(22),
                    dec!(19),
                    dec!(21),
                    dec!(2),
                    2
                ),
            ]
        );
    }

    #[test]
    fn aggregate_rejects_invalid_interval_arguments() {
        let base = Duration::seconds(2);
        assert_eq!(
            aggregate_candles(&[], base, Duration::seconds(5)).unwrap_err(),
            AggregateCandlesError::TargetNotMultipleOfBase {
                base,
                target: Duration::seconds(5)
            }
        );
        assert_eq!(
            aggregate_candles(&[], base, Duration::seconds(1)).unwrap_err(),
            AggregateCandlesError::TargetSmallerThanBase {
                base,
                target: Duration::seconds(1)
            }
        );
        assert_eq!(
            aggregate_candles(&[], Duration::zero(), Duration::seconds(3)).unwrap_err(),
            AggregateCandlesError::NonPositiveInterval {
                base: Duration::zero(),
                target: Duration::seconds(3)
            }
        );
        assert_eq!(
            aggregate_candles(&[], base, Duration::seconds(-3)).unwrap_err(),
            AggregateCandlesError::NonPositiveInterval {
                base,
                target: Duration::seconds(-3)
            }
        );
    }

    #[test]
    fn aggregate_rejects_sub_millisecond_intervals() {
        // 1.5ms floors to 1ms at the bucketing granularity, which would
        // silently mis-bucket — rejected at entry instead.
        let base = Duration::nanoseconds(1_500_000);
        let target = Duration::milliseconds(3);
        assert_eq!(
            aggregate_candles(&[], base, target).unwrap_err(),
            AggregateCandlesError::SubMillisecondInterval { base, target }
        );
        let base = Duration::milliseconds(1);
        let target = Duration::nanoseconds(2_500_000);
        assert_eq!(
            aggregate_candles(&[], base, target).unwrap_err(),
            AggregateCandlesError::SubMillisecondInterval { base, target }
        );
    }

    #[test]
    fn aggregate_of_empty_input_is_empty() {
        let out = aggregate_candles(&[], Duration::seconds(1), Duration::seconds(3)).unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn aggregate_close_time_round_trips_through_the_shared_boundary_helper() {
        let bars = [bar(
            "2024-01-15T12:00:01Z",
            dec!(10),
            dec!(12),
            dec!(9),
            dec!(11),
            dec!(1),
            1,
        )];
        let target = Duration::seconds(3);
        let out = aggregate_candles(&bars, Duration::seconds(1), target).unwrap();
        // close_time == bucket_open + target, via the single-sourced helper.
        assert_eq!(
            out[0].close_time,
            close_time_from_open(dt("2024-01-15T12:00:00Z"), IntervalStep::Fixed(target)).unwrap()
        );
    }

    #[test]
    fn aggregate_surfaces_bucket_close_overflow_as_an_error() {
        let target = Duration::seconds(3);
        // The final epoch-grid bucket below the maximum representable instant:
        // a 1ms sub-bar at its very start is itself representable, but the
        // bucket's exclusive close boundary is not.
        let bucket_open_ms = DateTime::<Utc>::MAX_UTC
            .timestamp_millis()
            .div_euclid(target.num_milliseconds())
            * target.num_milliseconds();
        let close_time = DateTime::from_timestamp_millis(bucket_open_ms + 1).unwrap();
        let bars = [Candle {
            close_time,
            open: dec!(1),
            high: dec!(1),
            low: dec!(1),
            close: dec!(1),
            volume: Some(dec!(1)),
            trade_count: Some(1),
        }];
        assert_eq!(
            aggregate_candles(&bars, Duration::milliseconds(1), target).unwrap_err(),
            AggregateCandlesError::TimestampOutOfRange { index: 0 }
        );
    }

    #[test]
    fn aggregate_sixty_1s_bars_matches_the_fields_of_a_native_1m_bar() {
        let session_open = dt("2024-01-15T12:00:00Z");
        let bars = (0..60i64)
            .map(|i| Candle {
                close_time: session_open + Duration::seconds(i + 1),
                open: Decimal::from(100 + i),
                high: Decimal::from(105 + i),
                low: Decimal::from(95 + i),
                close: Decimal::from(101 + i),
                volume: Some(Decimal::from(2)),
                trade_count: Some(3),
            })
            .collect::<Vec<_>>();
        let out = aggregate_candles(&bars, Duration::seconds(1), Duration::minutes(1)).unwrap();
        // Exactly the fields a native 1m candle over the same trades carries.
        assert_eq!(
            out,
            vec![Candle {
                close_time: dt("2024-01-15T12:01:00Z"),
                open: Decimal::from(100),
                high: Decimal::from(164),
                low: Decimal::from(95),
                close: Decimal::from(160),
                volume: Some(Decimal::from(120)),
                trade_count: Some(180),
            }]
        );
    }

    #[test]
    fn aggregate_any_none_constituent_poisons_the_bucket() {
        // Any sub-bar with unknown volume (or trade count) makes the whole
        // aggregated bucket unknown — never a silent under-count of the known
        // parts. This is the load-bearing invariant behind `Candle`'s optional
        // volume: an unknown component makes the sum unknown.
        let cell = |close: &str, v: Option<Decimal>, n: Option<u64>| Candle {
            close_time: dt(close),
            open: dec!(1),
            high: dec!(1),
            low: dec!(1),
            close: dec!(1),
            volume: v,
            trade_count: n,
        };
        let bars = [
            cell("2024-01-15T12:00:01Z", Some(dec!(1)), Some(1)),
            cell("2024-01-15T12:00:02Z", None, Some(2)), // unknown volume
            cell("2024-01-15T12:00:03Z", Some(dec!(3)), None), // unknown trade count
        ];
        let out = aggregate_candles(&bars, Duration::seconds(1), Duration::seconds(3)).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(
            out[0].volume, None,
            "one None-volume constituent poisons the summed volume"
        );
        assert_eq!(
            out[0].trade_count, None,
            "one None-trade_count constituent poisons the summed trade_count"
        );
    }

    #[test]
    fn aggregate_all_known_constituents_sum_normally() {
        // Control for the poisoning test: when every constituent is known the
        // bucket carries the plain totals.
        let bars = [
            bar(
                "2024-01-15T12:00:01Z",
                dec!(1),
                dec!(1),
                dec!(1),
                dec!(1),
                dec!(2),
                5,
            ),
            bar(
                "2024-01-15T12:00:02Z",
                dec!(1),
                dec!(1),
                dec!(1),
                dec!(1),
                dec!(3),
                7,
            ),
        ];
        let out = aggregate_candles(&bars, Duration::seconds(1), Duration::seconds(3)).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].volume, Some(dec!(5)));
        assert_eq!(out[0].trade_count, Some(12));
    }

    #[test]
    fn aggregate_gap_filled_input_carries_fills_without_double_counting() {
        // Pre-gap-filled contiguous 1s series: flat v=0 fill bars around one
        // real bar. Fills carry into open/low; zero volumes cannot
        // double-count, so fill-before-aggregate equals fill-after.
        let bars = [
            bar(
                "2024-01-15T12:00:01Z",
                dec!(90),
                dec!(90),
                dec!(90),
                dec!(90),
                dec!(0),
                0,
            ),
            bar(
                "2024-01-15T12:00:02Z",
                dec!(100),
                dec!(101),
                dec!(99),
                dec!(100.5),
                dec!(10),
                7,
            ),
            bar(
                "2024-01-15T12:00:03Z",
                dec!(100.5),
                dec!(100.5),
                dec!(100.5),
                dec!(100.5),
                dec!(0),
                0,
            ),
        ];
        let out = aggregate_candles(&bars, Duration::seconds(1), Duration::seconds(3)).unwrap();
        assert_eq!(
            out,
            vec![bar(
                "2024-01-15T12:00:03Z",
                dec!(90),
                dec!(101),
                dec!(90),
                dec!(100.5),
                dec!(10),
                7
            )]
        );
    }

    #[test]
    fn aggregate_bucket_of_only_zero_volume_bars_emits_one_flat_bar() {
        // "Non-empty" means has-candles, not has-volume.
        let bars = [
            bar(
                "2024-01-15T12:00:01Z",
                dec!(50),
                dec!(50),
                dec!(50),
                dec!(50),
                dec!(0),
                0,
            ),
            bar(
                "2024-01-15T12:00:02Z",
                dec!(50),
                dec!(50),
                dec!(50),
                dec!(50),
                dec!(0),
                0,
            ),
            bar(
                "2024-01-15T12:00:03Z",
                dec!(50),
                dec!(50),
                dec!(50),
                dec!(50),
                dec!(0),
                0,
            ),
        ];
        let out = aggregate_candles(&bars, Duration::seconds(1), Duration::seconds(3)).unwrap();
        assert_eq!(
            out,
            vec![bar(
                "2024-01-15T12:00:03Z",
                dec!(50),
                dec!(50),
                dec!(50),
                dec!(50),
                dec!(0),
                0
            )]
        );
    }

    #[test]
    fn aggregate_rejects_unsorted_and_duplicate_input_observably() {
        let first = bar(
            "2024-01-15T12:00:02Z",
            dec!(10),
            dec!(12),
            dec!(9),
            dec!(11),
            dec!(1),
            1,
        );
        let second = bar(
            "2024-01-15T12:00:01Z",
            dec!(11),
            dec!(15),
            dec!(10),
            dec!(14),
            dec!(2),
            2,
        );
        assert_eq!(
            aggregate_candles(&[first, second], Duration::seconds(1), Duration::seconds(3))
                .unwrap_err(),
            AggregateCandlesError::NonMonotonicInput { index: 1 }
        );
        // Strict ordering also rejects duplicate close_times (double-fetch /
        // overlapping-page data bugs) rather than double-counting their volume.
        assert_eq!(
            aggregate_candles(&[first, first], Duration::seconds(1), Duration::seconds(3))
                .unwrap_err(),
            AggregateCandlesError::NonMonotonicInput { index: 1 }
        );
    }

    #[test]
    fn aggregate_surfaces_sub_bar_open_underflow_as_an_error() {
        // close_time at the minimum representable instant: the sub-bar's open
        // (close − base) underflows and must surface, not panic.
        let bars = [Candle {
            close_time: DateTime::<Utc>::MIN_UTC,
            open: dec!(1),
            high: dec!(1),
            low: dec!(1),
            close: dec!(1),
            volume: Some(dec!(1)),
            trade_count: Some(1),
        }];
        assert_eq!(
            aggregate_candles(&bars, Duration::seconds(1), Duration::seconds(3)).unwrap_err(),
            AggregateCandlesError::TimestampOutOfRange { index: 0 }
        );
    }

    #[test]
    fn aggregate_surfaces_bucket_open_underflow_from_grid_flooring_as_an_error() {
        // The sub-bar's own open is exactly the minimum representable instant,
        // so the per-candle underflow check passes — but flooring that open
        // onto a coarse 3-day grid lands below it, a distinct underflow that
        // only the bucket-level check catches.
        let base = Duration::milliseconds(1);
        let bars = [Candle {
            close_time: DateTime::<Utc>::MIN_UTC + base,
            open: dec!(1),
            high: dec!(1),
            low: dec!(1),
            close: dec!(1),
            volume: Some(dec!(1)),
            trade_count: Some(1),
        }];
        assert_eq!(
            aggregate_candles(&bars, base, Duration::days(3)).unwrap_err(),
            AggregateCandlesError::TimestampOutOfRange { index: 0 }
        );
    }

    #[test]
    fn aggregate_floors_pre_epoch_opens_toward_negative_infinity() {
        // Sub-bar open is 1s *before* the epoch. Euclidean flooring snaps it to
        // -3s, so the bucket closes exactly on the epoch. Plain `/` truncates
        // toward zero, snapping to 0s and mis-bucketing the bar one bucket late.
        let bars = [bar(
            "1970-01-01T00:00:00Z",
            dec!(10),
            dec!(12),
            dec!(9),
            dec!(11),
            dec!(1),
            1,
        )];
        let out = aggregate_candles(&bars, Duration::seconds(1), Duration::seconds(3)).unwrap();
        assert_eq!(
            out,
            vec![bar(
                "1970-01-01T00:00:00Z",
                dec!(10),
                dec!(12),
                dec!(9),
                dec!(11),
                dec!(1),
                1
            )]
        );
    }

    #[test]
    fn aggregate_with_target_equal_to_base_is_an_identity_regrid() {
        // k = 1 passes validation deliberately: on-grid input passes through.
        let bars = [
            bar(
                "2024-01-15T12:00:01Z",
                dec!(10),
                dec!(12),
                dec!(9),
                dec!(11),
                dec!(1),
                1,
            ),
            bar(
                "2024-01-15T12:00:02Z",
                dec!(11),
                dec!(15),
                dec!(10),
                dec!(14),
                dec!(2),
                2,
            ),
        ];
        let out = aggregate_candles(&bars, Duration::seconds(1), Duration::seconds(1)).unwrap();
        assert_eq!(out, bars.to_vec());
    }

    /// Pins the serde contract documented on [`Candle::volume`].
    ///
    /// The absent-key case is the one worth a test: making the field optional turned a `missing
    /// field` rejection into a silent `None`, and nothing else in the codebase would notice if a
    /// `#[serde(default)]`-style change or a rename reintroduced the old behaviour.
    #[test]
    fn an_absent_volume_or_trade_count_deserialises_as_unknown_and_a_zero_stays_a_zero() {
        let without = serde_json::json!({
            "close_time": "2024-01-15T12:00:00Z",
            "open": "10", "high": "12", "low": "9", "close": "11",
        });
        let candle: Candle = serde_json::from_value(without).unwrap();
        assert_eq!(candle.volume, None);
        assert_eq!(candle.trade_count, None);

        // A record written before the migration: the fabricated zero survives verbatim, so this
        // type cannot recover the distinction it now expresses.
        let pre_migration = serde_json::json!({
            "close_time": "2024-01-15T12:00:00Z",
            "open": "10", "high": "12", "low": "9", "close": "11",
            "volume": "0", "trade_count": 0,
        });
        let candle: Candle = serde_json::from_value(pre_migration).unwrap();
        assert_eq!(candle.volume, Some(Decimal::ZERO));
        assert_eq!(candle.trade_count, Some(0));

        // `None` round-trips as an explicit null rather than degrading to the absent-key case.
        let unknown = Candle {
            close_time: dt("2024-01-15T12:00:00Z"),
            open: dec!(10),
            high: dec!(12),
            low: dec!(9),
            close: dec!(11),
            volume: None,
            trade_count: None,
        };
        let encoded = serde_json::to_value(unknown).unwrap();
        assert_eq!(encoded["volume"], serde_json::Value::Null);
        assert_eq!(encoded["trade_count"], serde_json::Value::Null);
        assert_eq!(serde_json::from_value::<Candle>(encoded).unwrap(), unknown);
    }
}

/// Normalised Barter OHLCV [`Candle`] model.
///
/// # `close_time` contract
///
/// `close_time` is the **exclusive end-of-period boundary** of the candle:
///
/// ```text
/// close_time == open_time + interval
/// ```
///
/// A candle aggregates the trades that fall in the **half-open interval**
/// `[close_time − interval, close_time)` — i.e. trades with
/// `open_time ≤ ts < close_time`. A trade landing exactly on `close_time`
/// belongs to the **next** candle, so `close_time` equals the next candle's
/// open instant.
///
/// Two distinct caveats apply to the boundary — do not conflate them:
///
/// - **Not session-aligned** (daily/weekly/monthly): the boundary is the UTC
///   period grid (`day → next 00:00 UTC`, etc.), **not** an exchange session
///   close. The library has no session calendar.
/// - **Variable-length calendar arithmetic** (month/quarter/year only): these
///   are nominal boundaries computed with calendar months (chrono [`Months`]),
///   not fixed [`Duration`]s. Daily and weekly are exact fixed durations in UTC
///   (no DST), exact to the millisecond. The `open_time + N months` equality
///   holds only when `open_time` is calendar-grid-aligned (e.g. a `1M` candle
///   opens on the 1st at 00:00 UTC); a non-aligned open day-clamps
///   (Jan 31 + 1 month → Feb 28/29). Venue-supplied open times are always
///   aligned, so clamping never arises in practice, but it is part of the
///   contract for callers computing boundaries from arbitrary instants.
///
/// `Candle` deliberately carries **neither `open_time` nor `interval`** — recover
/// them from the originating fetch request / subscription resolution
/// (`open_time ≡ close_time − interval`). Range-computing producers derive
/// `close_time` through [`close_time_from_open`] so the boundary is defined in
/// exactly one place (the Massive WS path uses the venue-supplied boundary
/// directly — see [`close_time_from_open`] for the full producer list).
///
/// # Using a `Candle` with the engine
///
/// When wrapping a `Candle` into a [`MarketEvent`](crate::event::MarketEvent) for
/// a consuming engine (live or backtest), set
/// [`time_exchange`](crate::event::MarketEvent::time_exchange) to this
/// `close_time` — it is the period-END instant, the only choice that avoids
/// lookahead (see that field's contract). The library's own candle producers
/// already do this.
///
/// [`Months`]: chrono::Months
/// [`Duration`]: chrono::Duration
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Hash, Debug, Deserialize, Serialize)]
pub struct Candle {
    pub close_time: DateTime<Utc>,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    /// Consolidated trade volume over the candle period, or `None` when the
    /// producer carries no volume for this instrument class.
    ///
    /// `None` is a first-class "volume is unknown" fact, **not** zero: some feeds
    /// (e.g. spot FX candles) publish no consolidated volume at all. Encoding
    /// that absence as `Some(0.0)` would be a silent lie — a volume-derived
    /// feature would read a real, quiet zero rather than an explicit unknown.
    /// [`aggregate_candles`] propagates the absence: any `None` constituent makes
    /// the whole aggregated bucket `None`.
    ///
    /// # Serde contract
    ///
    /// Plain `Option` semantics, with two consequences worth stating because they
    /// changed when this field became optional:
    ///
    /// - An **absent** `volume` key now deserialises to `None`. It was previously a
    ///   hard `missing field` error, so a truncated or hand-written record that used
    ///   to be rejected is now accepted as "volume unknown". Reject it explicitly if
    ///   a producer of yours must always report one.
    /// - A record persisted **before** the migration carries the fabricated
    ///   `"volume": 0` verbatim, and reads back as `Some(0)` — indistinguishable
    ///   from a genuine zero-volume bar. This type cannot recover the distinction;
    ///   re-fetch, or track the affected range out-of-band.
    ///
    /// `None` serialises as `"volume": null` (the key is written, not skipped), so a
    /// round trip through JSON preserves absence rather than degrading it into the
    /// missing-key case above.
    pub volume: Option<Decimal>,
    /// Number of trades in the candle period, or `None` when the producer does
    /// not report a trade count.
    ///
    /// Same contract as [`volume`](Self::volume) throughout, including serde:
    /// `None` means "unknown", never zero; any `None` constituent makes an
    /// aggregated bucket `None`; an absent key deserialises to `None`; and a
    /// pre-migration `"trade_count": 0` still reads back as `Some(0)`.
    pub trade_count: Option<u64>,
}

/// One step from a candle's open instant to its exclusive close boundary.
///
/// Keyed on a primitive step type (not on any per-exchange interval enum) so
/// every producer — regardless of how it names its native intervals — maps to
/// the same two cases and routes through [`close_time_from_open`]. This is the
/// mechanism that makes the [`Candle::close_time`] contract *enforced by
/// construction* rather than merely documented.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum IntervalStep {
    /// A fixed-length step (seconds through weeks — exact in UTC, no DST).
    Fixed(chrono::Duration),
    /// A variable-length calendar step in whole months. Covers calendar
    /// `month` (1), `quarter` (3) and `year` (12) — leap-year-correct via
    /// chrono's [`checked_add_months`](DateTime::checked_add_months).
    Months(u32),
}

/// Compute a candle's exclusive `close_time` boundary from its `open` instant.
///
/// This is the single shared boundary helper that every range-computing
/// [`Candle`] producer routes through (Massive REST, Hyperliquid, IBKR), so the
/// `close_time == open + interval` contract is computed in exactly one place. The
/// Massive WS path is the lone exception: it trusts the venue-supplied boundary
/// directly (see `WsAggregateMsg::into_candle` for the rationale).
///
/// - [`IntervalStep::Fixed`] adds a [`chrono::Duration`].
/// - [`IntervalStep::Months`] uses calendar-correct month arithmetic
///   ([`checked_add_months`](DateTime::checked_add_months)), so a Jan monthly
///   bar yields `Feb 1 00:00 UTC` and a leap-year Feb monthly bar yields
///   `Mar 1 00:00 UTC`.
///
/// # Returns
///
/// `None` on overflow — when the computed boundary falls outside the
/// representable [`DateTime<Utc>`] range. Callers **must** surface this as their
/// producer error type (an observable failure), **never** a silent fallback to a
/// plausible-but-wrong timestamp such as `UNIX_EPOCH`.
#[must_use]
pub fn close_time_from_open(open: DateTime<Utc>, step: IntervalStep) -> Option<DateTime<Utc>> {
    match step {
        IntervalStep::Fixed(duration) => open.checked_add_signed(duration),
        IntervalStep::Months(n) => open.checked_add_months(chrono::Months::new(n)),
    }
}

/// Inverse of [`close_time_from_open`]: recover a candle's `open` instant from its
/// exclusive `close_time` boundary (`open == close − interval`).
///
/// Used by range-bounded historical fetches to widen the venue request window:
/// the candle whose `close_time == start` has `open == start − interval`, so a
/// fetch that wants `close_time ∈ [start, end]` must ask the venue for opens down
/// to `start − interval` (then trim the result by `close_time`). See
/// [`Candle::close_time`].
///
/// # Returns
///
/// `None` on underflow (the computed open falls below the representable
/// [`DateTime<Utc>`] range).
///
/// For the range-widening use-case this `None` is **not** an error: it means the
/// candle whose `close_time == start` would have an unrepresentable open
/// (`start − interval` below [`DateTime<Utc>`] minimum) and therefore cannot
/// exist. Callers should fall back to the original lower bound — the un-widened
/// fetch already yields the complete, correct result set, so this is the right
/// outcome rather than a silent failure. (Contrast [`close_time_from_open`],
/// whose `None` *does* signal data loss for a real candle and must be surfaced
/// as an error.)
#[must_use]
pub fn open_time_from_close(close: DateTime<Utc>, step: IntervalStep) -> Option<DateTime<Utc>> {
    match step {
        IntervalStep::Fixed(duration) => close.checked_sub_signed(duration),
        IntervalStep::Months(n) => close.checked_sub_months(chrono::Months::new(n)),
    }
}

/// Error returned by [`aggregate_candles`] for invalid arguments or input.
///
/// Every variant is an observable failure: the aggregation never silently
/// repairs, reorders or drops input.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum AggregateCandlesError {
    /// `base_interval` or `target` is zero or negative.
    #[error("candle intervals must be positive (base: {base}, target: {target})")]
    NonPositiveInterval {
        /// The `base_interval` argument as supplied.
        base: Duration,
        /// The `target` argument as supplied.
        target: Duration,
    },
    /// `base_interval` or `target` has a sub-millisecond component. Bucketing
    /// operates at millisecond granularity (venue candle timestamps are
    /// millisecond-resolution), so a sub-millisecond interval would silently
    /// mis-bucket — it is rejected instead.
    #[error("candle intervals must be whole milliseconds (base: {base}, target: {target})")]
    SubMillisecondInterval {
        /// The `base_interval` argument as supplied.
        base: Duration,
        /// The `target` argument as supplied.
        target: Duration,
    },
    /// `target` is shorter than `base_interval` — aggregation only coarsens.
    #[error(
        "target interval must not be smaller than the base interval (base: {base}, target: {target})"
    )]
    TargetSmallerThanBase {
        /// The `base_interval` argument as supplied.
        base: Duration,
        /// The `target` argument as supplied.
        target: Duration,
    },
    /// `target` is not an integer multiple of `base_interval`, so base candles
    /// cannot tile target buckets exactly.
    #[error(
        "target interval must be an integer multiple of the base interval (base: {base}, target: {target})"
    )]
    TargetNotMultipleOfBase {
        /// The `base_interval` argument as supplied.
        base: Duration,
        /// The `target` argument as supplied.
        target: Duration,
    },
    /// The input candle at `index` is not strictly later (by `close_time`) than
    /// its predecessor. Strictness also rejects duplicate timestamps: no two
    /// distinct fixed-interval candles can legitimately share a `close_time`,
    /// so a duplicate is a data bug (e.g. an overlapping-page double fetch)
    /// whose volume must not be double-counted.
    #[error("input candles must be strictly ascending by close_time (violation at index {index})")]
    NonMonotonicInput {
        /// Index into the input slice of the offending candle.
        index: usize,
    },
    /// A boundary computed for the candle at `index` falls outside the
    /// representable [`DateTime<Utc>`] range. Three computations can fail this
    /// way: the sub-bar `open` underflowed (input near the minimum instant);
    /// the bucket's epoch-grid-floored open underflowed (flooring a
    /// *representable* open onto a coarse `target` grid can still land below
    /// the minimum instant); or the bucket `close_time` overflowed (input near
    /// the maximum instant).
    #[error(
        "candle at index {index} yields a boundary outside the representable DateTime<Utc> range"
    )]
    TimestampOutOfRange {
        /// Index into the input slice attributing the failure: the offending
        /// candle itself for a sub-bar `open` underflow, or the affected
        /// bucket's first sub-bar for a bucket-level boundary failure (whose
        /// own sub-bar boundary need not be invalid).
        index: usize,
    },
}

/// Aggregate fixed-interval OHLCV [`Candle`]s into a coarser fixed interval.
///
/// A pure, venue-agnostic batch primitive: `candles` at resolution
/// `base_interval` are bucketed onto the epoch-anchored `target` grid, and one
/// aggregated [`Candle`] is emitted per **non-empty** bucket, ascending by
/// `close_time`. All price/volume arithmetic is [`Decimal`]-exact — no
/// floating point.
///
/// # Aggregation rule
///
/// Each output candle carries, over its bucket's sub-bars:
///
/// - `open`: the earliest sub-bar's `open`
/// - `close`: the latest sub-bar's `close`
/// - `high` / `low`: the maximum / minimum across the bucket
/// - `volume` / `trade_count`: sums across the bucket
/// - `close_time`: the bucket's exclusive close boundary
///   (`bucket_open + target`), computed via [`close_time_from_open`] so the
///   [`Candle::close_time`] contract stays single-sourced
///
/// A **partial** bucket (fewer than `target / base_interval` sub-bars present)
/// aggregates only the sub-bars actually present — `volume` is never scaled up
/// to the bucket width.
///
/// # Bucketing: epoch anchoring
///
/// Each sub-bar's open (recovered as `close_time − base_interval` via
/// [`open_time_from_close`]) is floored onto the grid of integer multiples of
/// `target` since the Unix epoch. This is the natural anchoring for every
/// target that divides a day (seconds through hours). **Caveat**: the Unix
/// epoch was a *Thursday*, so for multi-day targets — e.g. 3-day or weekly
/// buckets — the epoch grid diverges from venue-native anchoring (venues serve
/// Monday-anchored weekly candles). Callers wanting venue-native weekly bars
/// should fetch them from the venue rather than aggregate.
///
/// # Caller obligations
///
/// - **Strictly ascending**: input must be strictly ascending by `close_time`.
///   Violations (including duplicate timestamps) surface as
///   [`AggregateCandlesError::NonMonotonicInput`] — input is never silently
///   reordered or deduplicated.
/// - **Uniform resolution**: every input candle must span exactly
///   `base_interval` (its open is `close_time − base_interval`). This cannot
///   be validated from a [`Candle`] alone (candles carry no interval field)
///   and is trusted.
/// - **Grid alignment is _not_ validated**: sub-bar opens are floored onto the
///   epoch grid wherever they fall, so legitimately non-epoch-anchored input
///   (e.g. venue-native Monday-anchored weekly bars) is accepted — but its
///   buckets are epoch-anchored regardless (see above).
/// - **Millisecond timestamps**: bucketing operates at millisecond
///   granularity (venue candle timestamps are millisecond-resolution). A
///   `close_time` with a sub-millisecond component has that component ignored
///   during bucketing. Sub-millisecond *intervals* are rejected as
///   [`AggregateCandlesError::SubMillisecondInterval`].
///
/// # Gap policy: none (deliberately)
///
/// A fully-empty bucket emits **no** output candle — synthesising flat bars
/// (forward-fill) is consumer policy, not a library concern. The primitive is
/// neutral about which side of the call that policy runs on:
///
/// - feed raw, sparse candles and gap-fill the *output*, or
/// - feed a pre-gap-filled contiguous series — zero-volume fill bars aggregate
///   consistently (they carry into `open`/`high`/`low`/`close`; volume sums
///   are unchanged, so fills cannot double-count).
///
/// "Non-empty" means *has input candles*, not *has volume*: a bucket
/// containing only zero-volume fill bars emits one flat zero-volume candle.
///
/// # Trailing bucket
///
/// Batch semantics: the final bucket is emitted even if the input merely
/// *ends* mid-bucket — indistinguishable from a trailing gap. Live/streaming
/// callers must withhold input until a bucket completes (or drop the tail)
/// themselves — and should feed bounded windows, not re-aggregate a growing
/// history buffer per event: every call re-validates and re-scans its entire
/// input.
///
/// # Limitations
///
/// Fixed durations only: calendar-month aggregation (variable-length buckets,
/// [`IntervalStep::Months`]) is out of scope. `target == base_interval` is
/// accepted and acts as an identity/re-grid pass.
///
/// # Panics
///
/// Panics if a bucket's accumulated `volume` overflows [`Decimal`] or its
/// accumulated `trade_count` overflows `u64`. Both require magnitudes no
/// venue feed produces (~7.9 × 10²⁸ summed volume, ~1.8 × 10¹⁹ summed
/// trades), so either indicates corrupt input; the panic keeps that failure
/// loud on every build profile rather than wrapping silently.
///
/// # Errors
///
/// See [`AggregateCandlesError`]. Boundary computations that leave the
/// representable [`DateTime<Utc>`] range — sub-bar open underflow near the
/// minimum instant, epoch-grid flooring pushing a bucket's open below the
/// minimum instant, or bucket `close_time` overflow near the maximum — surface
/// as [`AggregateCandlesError::TimestampOutOfRange`] — never a silent
/// plausible-but-wrong timestamp.
pub fn aggregate_candles(
    candles: &[Candle],
    base_interval: Duration,
    target: Duration,
) -> Result<Vec<Candle>, AggregateCandlesError> {
    if base_interval <= Duration::zero() || target <= Duration::zero() {
        return Err(AggregateCandlesError::NonPositiveInterval {
            base: base_interval,
            target,
        });
    }
    // Bucketing operates at millisecond granularity (venue candle timestamps
    // are ms); a sub-ms interval would floor and silently mis-bucket.
    if Duration::milliseconds(base_interval.num_milliseconds()) != base_interval
        || Duration::milliseconds(target.num_milliseconds()) != target
    {
        return Err(AggregateCandlesError::SubMillisecondInterval {
            base: base_interval,
            target,
        });
    }
    if target < base_interval {
        return Err(AggregateCandlesError::TargetSmallerThanBase {
            base: base_interval,
            target,
        });
    }
    let target_ms = target.num_milliseconds();
    if target_ms % base_interval.num_milliseconds() != 0 {
        return Err(AggregateCandlesError::TargetNotMultipleOfBase {
            base: base_interval,
            target,
        });
    }

    /// An in-progress epoch-grid bucket accumulator.
    struct Bucket {
        /// The bucket's open instant in epoch milliseconds (on the `target` grid).
        open_ms: i64,
        /// Index of the bucket's first sub-bar, for error attribution.
        first_index: usize,
        open: Decimal,
        high: Decimal,
        low: Decimal,
        close: Decimal,
        /// `None` if *any* sub-bar folded into this bucket had unknown volume —
        /// an unknown component makes the sum unknown, never a silent under-count.
        volume: Option<Decimal>,
        /// `None` under the same any-`None`-poisons rule as [`volume`](Self::volume).
        trade_count: Option<u64>,
    }

    /// Emit a completed bucket as an aggregated [`Candle`], deriving its
    /// exclusive close boundary through the shared [`close_time_from_open`]
    /// helper.
    fn flush(bucket: Bucket, target: Duration) -> Result<Candle, AggregateCandlesError> {
        let out_of_range = AggregateCandlesError::TimestampOutOfRange {
            index: bucket.first_index,
        };
        // `open_ms` is grid-floored, so it can sit *below* a representable
        // sub-bar open that already passed the per-candle range check —
        // a distinct underflow that must surface here too.
        let bucket_open = DateTime::from_timestamp_millis(bucket.open_ms).ok_or(out_of_range)?;
        let close_time =
            close_time_from_open(bucket_open, IntervalStep::Fixed(target)).ok_or(out_of_range)?;
        Ok(Candle {
            close_time,
            open: bucket.open,
            high: bucket.high,
            low: bucket.low,
            close: bucket.close,
            volume: bucket.volume,
            trade_count: bucket.trade_count,
        })
    }

    // Empty input is valid (arguments were still checked above) and needs no
    // allocation at all — `Vec::new()` rather than the 1-slot hint below.
    if candles.is_empty() {
        return Ok(Vec::new());
    }

    // Dense-case capacity estimate (one output per `target / base_interval`
    // sub-bars), not an upper bound: sparse input can emit up to one bucket
    // per candle, in which case the `Vec` grows normally past the hint. The
    // `usize::MAX` fallback (quotient overflowing `usize` on 32-bit platforms,
    // where `usize::MAX < i64::MAX`) degrades to a capacity of 1.
    let sub_bars_per_bucket =
        usize::try_from(target_ms / base_interval.num_milliseconds()).unwrap_or(usize::MAX);
    let mut output = Vec::with_capacity(candles.len() / sub_bars_per_bucket + 1);
    let mut bucket: Option<Bucket> = None;
    let mut prev_close_time: Option<DateTime<Utc>> = None;

    for (index, candle) in candles.iter().enumerate() {
        if prev_close_time.is_some_and(|prev| candle.close_time <= prev) {
            return Err(AggregateCandlesError::NonMonotonicInput { index });
        }
        prev_close_time = Some(candle.close_time);

        // Recover the sub-bar's open through the shared boundary helper, then
        // snap it onto the epoch grid. `div_euclid` (not `/`) floors toward
        // negative infinity, which keeps pre-1970 instants correctly bucketed.
        // The re-multiply cannot overflow: floor-then-remultiply stays within
        // one `target_ms` of the input, and a representable `DateTime<Utc>` is
        // ~±8.3 × 10¹⁵ ms — three orders of magnitude inside `i64`.
        let sub_open = open_time_from_close(candle.close_time, IntervalStep::Fixed(base_interval))
            .ok_or(AggregateCandlesError::TimestampOutOfRange { index })?;
        let bucket_open_ms = sub_open.timestamp_millis().div_euclid(target_ms) * target_ms;

        match &mut bucket {
            Some(current) if current.open_ms == bucket_open_ms => {
                current.high = current.high.max(candle.high);
                current.low = current.low.min(candle.low);
                current.close = candle.close;
                // Any-`None`-poisons: a bucket with one unknown component sums to
                // an unknown total, never a silent under-count of the known parts.
                current.volume = match (current.volume, candle.volume) {
                    (Some(acc), Some(v)) => Some(acc + v),
                    _ => None,
                };
                // A raw u64 `+=` would wrap silently in release builds (no
                // overflow-checks profile); a checked add keeps overflow loud
                // on every profile. The `expect` is the deliberate documented
                // panic of `# Panics`: overflow means corrupt input, not a
                // recoverable state. `None` on either side propagates unknown.
                #[allow(clippy::expect_used)]
                {
                    current.trade_count = match (current.trade_count, candle.trade_count) {
                        (Some(acc), Some(n)) => Some(
                            acc.checked_add(n)
                                .expect("bucket trade_count overflows u64"),
                        ),
                        _ => None,
                    };
                }
            }
            _ => {
                if let Some(completed) = bucket.take() {
                    output.push(flush(completed, target)?);
                }
                bucket = Some(Bucket {
                    open_ms: bucket_open_ms,
                    first_index: index,
                    open: candle.open,
                    high: candle.high,
                    low: candle.low,
                    close: candle.close,
                    volume: candle.volume,
                    trade_count: candle.trade_count,
                });
            }
        }
    }

    if let Some(last) = bucket.take() {
        output.push(flush(last, target)?);
    }

    Ok(output)
}

/// Candle interval/resolution, shared across every venue that produces candles.
///
/// This is the **venue-agnostic union** of all supported candle resolutions — it
/// is deliberately *not* gated to any one exchange's capabilities. Per-venue
/// support rules (e.g. Hyperliquid rejecting `Sec1`/`Hour6`) live in the exchange
/// layer, not on this enum (separation of concerns). Because the enum is a union,
/// **each venue's interval guard must be re-reviewed whenever a variant is added.**
///
/// # String form
///
/// [`as_str`](Self::as_str) is the **single source of truth** for every string
/// representation: [`Display`](std::fmt::Display), [`Serialize`], [`FromStr`] and [`Deserialize`] all
/// delegate to it (or its inverse), so there is exactly one place mapping
/// variant↔string. The strings follow Binance's kline `interval` convention and are
/// **case-sensitive** — note `Month1 → "1M"` (uppercase) vs `Min1 → "1m"`. The union
/// is a superset of any one venue's menu: `Sec5`/`Sec15`/`Sec30` have no Binance
/// kline equivalent, so a string round-trip is *not* proof a venue serves it.
///
/// # Ordering
///
/// Variants are declared in **ascending duration** order. `Ord`/`PartialOrd` exist
/// only as a compile requirement (the type embeds in
/// [`Candles`], which must stay `Ord` to
/// preserve the derived `Ord` on `Subscription`); the chronological declaration
/// order makes the derived order at least sensible. Nothing currently sorts
/// intervals semantically.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub enum CandleInterval {
    /// 1 second
    Sec1,
    /// 5 seconds
    Sec5,
    /// 15 seconds
    Sec15,
    /// 30 seconds
    Sec30,
    /// 1 minute
    Min1,
    /// 3 minutes
    Min3,
    /// 5 minutes
    Min5,
    /// 15 minutes
    Min15,
    /// 30 minutes
    Min30,
    /// 1 hour
    Hour1,
    /// 2 hours
    Hour2,
    /// 4 hours
    Hour4,
    /// 6 hours
    Hour6,
    /// 8 hours
    Hour8,
    /// 12 hours
    Hour12,
    /// 1 day
    Day1,
    /// 3 days
    Day3,
    /// 1 week
    Week1,
    /// 1 month
    Month1,
}

impl CandleInterval {
    /// Every [`CandleInterval`] variant, in ascending-duration declaration order.
    ///
    /// Lets variant-exhaustive tests (round-trip, channel-suffix drift guards)
    /// iterate without hand-listing the variants.
    ///
    /// When adding a [`CandleInterval`] variant, add it here too: the length
    /// literal and the `candle_interval_all_covers_every_variant_in_ascending_order`
    /// test pin `ALL`'s length to the variant count, but full coverage is not
    /// compile-enforced — the exhaustive `match`es elsewhere are the compile gate.
    pub const ALL: [CandleInterval; 19] = [
        Self::Sec1,
        Self::Sec5,
        Self::Sec15,
        Self::Sec30,
        Self::Min1,
        Self::Min3,
        Self::Min5,
        Self::Min15,
        Self::Min30,
        Self::Hour1,
        Self::Hour2,
        Self::Hour4,
        Self::Hour6,
        Self::Hour8,
        Self::Hour12,
        Self::Day1,
        Self::Day3,
        Self::Week1,
        Self::Month1,
    ];

    /// The exchange string form of this interval (e.g. `"1m"`, `"6h"`, `"1M"`).
    ///
    /// The **single source of truth** for all string representations — `Display`,
    /// `Serialize`, `FromStr` and `Deserialize` all key off this. Case-sensitive:
    /// `Month1 → "1M"`, every other variant lowercase.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Sec1 => "1s",
            Self::Sec5 => "5s",
            Self::Sec15 => "15s",
            Self::Sec30 => "30s",
            Self::Min1 => "1m",
            Self::Min3 => "3m",
            Self::Min5 => "5m",
            Self::Min15 => "15m",
            Self::Min30 => "30m",
            Self::Hour1 => "1h",
            Self::Hour2 => "2h",
            Self::Hour4 => "4h",
            Self::Hour6 => "6h",
            Self::Hour8 => "8h",
            Self::Hour12 => "12h",
            Self::Day1 => "1d",
            Self::Day3 => "3d",
            Self::Week1 => "1w",
            Self::Month1 => "1M",
        }
    }

    /// Map this interval to the shared [`IntervalStep`] used to compute a candle's
    /// exclusive `close_time` boundary via [`close_time_from_open`]. All intervals
    /// are fixed-length except `1M`, which is a calendar month.
    #[must_use]
    pub fn to_step(self) -> IntervalStep {
        match self {
            Self::Sec1 => IntervalStep::Fixed(Duration::seconds(1)),
            Self::Sec5 => IntervalStep::Fixed(Duration::seconds(5)),
            Self::Sec15 => IntervalStep::Fixed(Duration::seconds(15)),
            Self::Sec30 => IntervalStep::Fixed(Duration::seconds(30)),
            Self::Min1 => IntervalStep::Fixed(Duration::minutes(1)),
            Self::Min3 => IntervalStep::Fixed(Duration::minutes(3)),
            Self::Min5 => IntervalStep::Fixed(Duration::minutes(5)),
            Self::Min15 => IntervalStep::Fixed(Duration::minutes(15)),
            Self::Min30 => IntervalStep::Fixed(Duration::minutes(30)),
            Self::Hour1 => IntervalStep::Fixed(Duration::hours(1)),
            Self::Hour2 => IntervalStep::Fixed(Duration::hours(2)),
            Self::Hour4 => IntervalStep::Fixed(Duration::hours(4)),
            Self::Hour6 => IntervalStep::Fixed(Duration::hours(6)),
            Self::Hour8 => IntervalStep::Fixed(Duration::hours(8)),
            Self::Hour12 => IntervalStep::Fixed(Duration::hours(12)),
            Self::Day1 => IntervalStep::Fixed(Duration::days(1)),
            Self::Day3 => IntervalStep::Fixed(Duration::days(3)),
            Self::Week1 => IntervalStep::Fixed(Duration::weeks(1)),
            Self::Month1 => IntervalStep::Months(1),
        }
    }
}

impl std::fmt::Display for CandleInterval {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Error returned by [`CandleInterval::from_str`] for an unrecognised string.
///
/// The offending input is kept private and exposed via [`input`](Self::input) so
/// the error's representation can evolve (e.g. gaining context) without a
/// breaking change — mirroring `std`'s opaque parse-error types.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseCandleIntervalError {
    invalid: String,
}

impl ParseCandleIntervalError {
    /// The input string that failed to parse.
    #[must_use]
    pub fn input(&self) -> &str {
        &self.invalid
    }
}

impl std::fmt::Display for ParseCandleIntervalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "invalid candle interval: {:?}", self.invalid)
    }
}

impl std::error::Error for ParseCandleIntervalError {}

impl FromStr for CandleInterval {
    type Err = ParseCandleIntervalError;

    /// The inverse of [`CandleInterval::as_str`] — case-sensitive (`"1M"` is the
    /// only uppercase form). Keeps variant↔string mapping in exactly one place.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "1s" => Ok(Self::Sec1),
            "5s" => Ok(Self::Sec5),
            "15s" => Ok(Self::Sec15),
            "30s" => Ok(Self::Sec30),
            "1m" => Ok(Self::Min1),
            "3m" => Ok(Self::Min3),
            "5m" => Ok(Self::Min5),
            "15m" => Ok(Self::Min15),
            "30m" => Ok(Self::Min30),
            "1h" => Ok(Self::Hour1),
            "2h" => Ok(Self::Hour2),
            "4h" => Ok(Self::Hour4),
            "6h" => Ok(Self::Hour6),
            "8h" => Ok(Self::Hour8),
            "12h" => Ok(Self::Hour12),
            "1d" => Ok(Self::Day1),
            "3d" => Ok(Self::Day3),
            "1w" => Ok(Self::Week1),
            "1M" => Ok(Self::Month1),
            other => Err(ParseCandleIntervalError {
                invalid: other.to_owned(),
            }),
        }
    }
}

impl Serialize for CandleInterval {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for CandleInterval {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = <std::borrow::Cow<'de, str>>::deserialize(deserializer)?;
        raw.parse().map_err(de::Error::custom)
    }
}
