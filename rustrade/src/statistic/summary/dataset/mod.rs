use crate::statistic::{algorithm::welford_online, summary::dataset::dispersion::Dispersion};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Utilities for analysing a datasets measured of dispersion - range, variance & standard deviation.
pub mod dispersion;

/// Maintains running statistical summaries of a dataset using Welford's online algorithm.
///
/// Efficiently computes and maintains key statistical measures of a dataset in "one-pass" (as
/// new values arrive), without storing the entire dataset in memory.
///
/// # Statistical Measures
/// Tracks:
/// - Count of observations
/// - Sum of all values
/// - Running mean
/// - Dispersion measures (range, variance, and standard deviation)
///
/// The variance and standard deviation are derived when read, by [`variance`](Self::variance)
/// and [`std_dev`](Self::std_dev), so an update stays cheap.
///
/// # Algorithm
/// Uses Welford's online algorithm which:
/// - Updates statistics incrementally with each new value
/// - Provides better numerical stability than naive methods
/// - Requires only O(1) memory regardless of dataset size
///
/// See: <https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Welford's_online_algorithm>
///
/// # Example
/// ```
/// use rust_decimal_macros::dec;
/// use rustrade::statistic::summary::dataset::DataSetSummary;
///
/// // Initialise empty DataSetSummary
/// let mut stats = DataSetSummary::default();
///
/// // Update with new values
/// stats.update(dec!(1.0));
/// stats.update(dec!(2.0));
/// stats.update(dec!(3.0));
///
/// assert_eq!(stats.count, dec!(3));
/// assert_eq!(stats.sum, dec!(6.0));
/// assert_eq!(stats.mean, dec!(2.0));
/// ```
#[derive(Debug, Clone, PartialEq, PartialOrd, Default, Deserialize, Serialize)]
pub struct DataSetSummary {
    pub count: Decimal,
    pub sum: Decimal,
    pub mean: Decimal,
    pub dispersion: Dispersion,
}

impl DataSetSummary {
    /// Updates dataset statistics with the new value using Welford's online algorithm.
    ///
    /// This method:
    /// 1. Increments the observation counter
    /// 2. Updates the running sum
    /// 3. Recalculates the mean using Welford's algorithm
    /// 4. Updates the dispersion measures it holds: the range and the recurrence relation M
    pub fn update(&mut self, next_value: Decimal) {
        // Increment counter
        self.count += Decimal::ONE;

        // Update Sum
        self.sum += next_value;

        // Update Mean
        let prev_mean = self.mean;
        self.mean = welford_online::calculate_mean(self.mean, next_value, self.count);

        // Update Dispersion
        self.dispersion.update(prev_mean, self.mean, next_value);
    }

    /// The population variance of the dataset. Zero when it is empty.
    pub fn variance(&self) -> Decimal {
        self.dispersion.variance(self.count)
    }

    /// The population standard deviation of the dataset. Zero when it is empty.
    ///
    /// Computed on each call, with a `Decimal` square root.
    pub fn std_dev(&self) -> Decimal {
        self.dispersion.std_dev(self.count)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use crate::statistic::summary::dataset::dispersion::Range;
    use rust_decimal_macros::dec;
    use std::str::FromStr;

    #[test]
    fn update_data_summary_with_position() {
        struct TestCase {
            input_next_value: Decimal,
            expected_variance: Decimal,
            expected_std_dev: Decimal,
            expected_summary: DataSetSummary,
        }

        let mut data_summary = DataSetSummary::default();

        let test_cases = vec![
            TestCase {
                // Test case 0: First value of 1.1
                input_next_value: dec!(1.1),
                expected_variance: dec!(0.0),
                expected_std_dev: dec!(0.0),
                expected_summary: DataSetSummary {
                    count: dec!(1),
                    sum: dec!(1.1),
                    mean: dec!(1.1),
                    dispersion: Dispersion {
                        range: Range {
                            activated: true,
                            high: dec!(1.1),
                            low: dec!(1.1),
                        },
                        recurrence_relation_m: dec!(0.0),
                    },
                },
            },
            TestCase {
                // Test case 1: Second value of 1.2
                input_next_value: dec!(1.2),
                expected_variance: dec!(0.0025),
                expected_std_dev: dec!(0.05),
                expected_summary: DataSetSummary {
                    count: dec!(2),
                    sum: dec!(2.3),
                    mean: Decimal::from_str("1.15").unwrap(), // 2.3/2.0
                    dispersion: Dispersion {
                        range: Range {
                            activated: true,
                            high: dec!(1.2),
                            low: dec!(1.1),
                        },
                        recurrence_relation_m: dec!(0.005),
                    },
                },
            },
            TestCase {
                // Test case 2: Third value of 1.3
                input_next_value: dec!(1.3),
                expected_variance: Decimal::from_str("0.006666666667").unwrap(), // 1/150
                expected_std_dev: Decimal::from_str("0.081649658092").unwrap(),  // sqrt(1/150)
                expected_summary: DataSetSummary {
                    count: dec!(3),
                    sum: dec!(3.6),
                    mean: dec!(1.2), // 3.6/3.0
                    dispersion: Dispersion {
                        range: Range {
                            activated: true,
                            high: dec!(1.3),
                            low: dec!(1.1),
                        },
                        recurrence_relation_m: dec!(0.02),
                    },
                },
            },
        ];

        for (index, test) in test_cases.into_iter().enumerate() {
            data_summary.update(test.input_next_value);

            // Basic statistics checks - exact equality for simple operations
            assert_eq!(
                data_summary.count, test.expected_summary.count,
                "Count Input: {:?}",
                index
            );
            assert_eq!(
                data_summary.sum, test.expected_summary.sum,
                "Sum Input: {:?}",
                index
            );
            assert_eq!(
                data_summary.mean, test.expected_summary.mean,
                "Mean Input: {:?}",
                index
            );

            // Range checks - exact equality
            assert_eq!(
                data_summary.dispersion.range, test.expected_summary.dispersion.range,
                "Range Input: {:?}",
                index
            );

            // Statistical calculations - check within tolerance
            let tolerance = Decimal::from_str("0.000000000001").unwrap();

            let recurrence_diff = (data_summary.dispersion.recurrence_relation_m
                - test.expected_summary.dispersion.recurrence_relation_m)
                .abs();
            assert!(
                recurrence_diff <= tolerance,
                "Recurrence difference {} exceeds tolerance, Input: {:?}",
                recurrence_diff,
                index
            );

            let variance_diff = (data_summary.variance() - test.expected_variance).abs();
            assert!(
                variance_diff <= tolerance,
                "Variance difference {} exceeds tolerance, Input: {:?}",
                variance_diff,
                index
            );

            let std_dev_diff = (data_summary.std_dev() - test.expected_std_dev).abs();
            assert!(
                std_dev_diff <= tolerance,
                "Std Dev difference {} exceeds tolerance, Input: {:?}",
                std_dev_diff,
                index
            );
        }
    }

    #[test]
    fn empty_summary_has_zero_variance_and_std_dev() {
        let summary = DataSetSummary::default();

        assert_eq!(summary.variance(), Decimal::ZERO);
        assert_eq!(summary.std_dev(), Decimal::ZERO);
    }

    #[test]
    fn summary_serialised_with_variance_and_std_dev_still_loads() {
        let json = r#"{
            "count": "2",
            "sum": "2.3",
            "mean": "1.15",
            "dispersion": {
                "range": { "activated": true, "high": "1.2", "low": "1.1" },
                "recurrence_relation_m": "0.005",
                "variance": "0.0025",
                "std_dev": "0.05"
            }
        }"#;

        let summary: DataSetSummary = serde_json::from_str(json).unwrap();

        assert_eq!(summary.variance(), dec!(0.0025));
        assert_eq!(summary.std_dev(), dec!(0.05));
        let reserialised = serde_json::to_value(&summary).unwrap();
        assert!(reserialised["dispersion"].get("std_dev").is_none());
    }
}
