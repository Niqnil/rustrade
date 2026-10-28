use crate::statistic::algorithm::welford_online;
use rust_decimal::{Decimal, MathematicalOps};
use serde::{Deserialize, Serialize};

/// Representation of a dataset using measures of dispersion - range, variance & standard deviation.
///
/// Holds the range and the Welford recurrence relation M, the running sum of squared deviations
/// from the mean. The variance and standard deviation are derived from M when read, not on each
/// update: [`std_dev`](Self::std_dev) takes a `Decimal` square root, which would otherwise cost
/// every update far more than the update itself.
#[derive(Debug, Clone, PartialEq, PartialOrd, Default, Deserialize, Serialize)]
pub struct Dispersion {
    pub range: Range,
    pub recurrence_relation_m: Decimal,
}

impl Dispersion {
    /// Iteratively updates the measures of Dispersion given the previous mean, new mean and new
    /// value.
    pub fn update(&mut self, prev_mean: Decimal, new_mean: Decimal, new_value: Decimal) {
        // Update Range
        self.range.update(new_value);

        // Update Welford Online recurrence relation M
        self.recurrence_relation_m = welford_online::calculate_recurrence_relation_m(
            self.recurrence_relation_m,
            prev_mean,
            new_value,
            new_mean,
        );
    }

    /// The population variance of a dataset of `count` values, the count this `Dispersion` was
    /// updated with. Zero for an empty dataset.
    pub fn variance(&self, count: Decimal) -> Decimal {
        welford_online::calculate_population_variance(self.recurrence_relation_m, count)
    }

    /// The population standard deviation of a dataset of `count` values, the count this
    /// `Dispersion` was updated with. Zero for an empty dataset.
    ///
    /// Computed on each call, with a `Decimal` square root.
    #[allow(clippy::expect_used)] // sqrt fails only on a negative input, which abs() rules out
    pub fn std_dev(&self, count: Decimal) -> Decimal {
        // Welford variance can be slightly negative due to Decimal rounding; abs() guards sqrt input
        self.variance(count)
            .abs()
            .sqrt()
            .expect("abs() ensures non-negative input")
    }
}

/// Measure of dispersion providing the highest and lowest value of a dataset. Lazy evaluation is
/// used when calculating the range value via the range() method.
#[derive(Debug, Clone, PartialEq, PartialOrd, Default, Deserialize, Serialize)]
pub struct Range {
    pub activated: bool,
    pub high: Decimal,
    pub low: Decimal,
}

impl Range {
    /// Initialises the Range with the provided first value of the dataset.
    pub fn init(first_value: Decimal) -> Self {
        Self {
            activated: true,
            high: first_value,
            low: first_value,
        }
    }

    /// Iteratively updates the Range given the next value in the dataset.
    pub fn update(&mut self, new_value: Decimal) {
        if self.activated {
            if new_value > self.high {
                self.high = new_value;
            }

            if new_value < self.low {
                self.low = new_value;
            }
        } else {
            self.activated = true;
            self.high = new_value;
            self.low = new_value;
        }
    }

    /// Calculates the range between the highest and lowest value of a dataset.
    pub fn range(&self) -> Decimal {
        self.high - self.low
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use std::str::FromStr;

    #[test]
    fn update_dispersion() {
        let mut dispersion = Dispersion::default();

        // Dataset  = [1.1, 1.2, 1.3, 1.4, 0.6]
        // Means    = [1.1, 1.15, 1.2, 1.25, 1.12]
        struct UpdateInput {
            prev_mean: Decimal,
            new_mean: Decimal,
            new_value: Decimal,
            value_count: Decimal,
        }

        let input_1 = UpdateInput {
            prev_mean: dec!(0.0),
            new_mean: dec!(1.1),
            new_value: dec!(1.1),
            value_count: dec!(1),
        };
        let input_2 = UpdateInput {
            prev_mean: dec!(1.1),
            new_mean: dec!(1.15),
            new_value: dec!(1.2),
            value_count: dec!(2),
        };
        let input_3 = UpdateInput {
            prev_mean: dec!(1.15),
            new_mean: dec!(1.2),
            new_value: dec!(1.3),
            value_count: dec!(3),
        };
        let input_4 = UpdateInput {
            prev_mean: dec!(1.2),
            new_mean: dec!(1.25),
            new_value: dec!(1.4),
            value_count: dec!(4),
        };
        let input_5 = UpdateInput {
            prev_mean: dec!(1.25),
            new_mean: dec!(1.12),
            new_value: dec!(0.6),
            value_count: dec!(5),
        };
        let inputs = vec![input_1, input_2, input_3, input_4, input_5];

        // Expected outputs calculated with high precision decimal arithmetic:
        // Recurrence_M = [0.0, 0.005, 0.02, 0.05, 0.388]
        // Variance     = [0.0, 0.0025, 0.006666666667, 0.0125, 0.0776]
        // Std. Dev     = [0.0, 0.05, 0.081649658092, 0.111803398875, 0.278567765544]
        let output_1 = Dispersion {
            range: Range {
                activated: true,
                high: dec!(1.1),
                low: dec!(1.1),
            },
            recurrence_relation_m: dec!(0.0),
        };

        let output_2 = Dispersion {
            range: Range {
                activated: true,
                high: dec!(1.2),
                low: dec!(1.1),
            },
            recurrence_relation_m: dec!(0.005),
        };

        let output_3 = Dispersion {
            range: Range {
                activated: true,
                high: dec!(1.3),
                low: dec!(1.1),
            },
            recurrence_relation_m: dec!(0.02),
        };

        let output_4 = Dispersion {
            range: Range {
                activated: true,
                high: dec!(1.4),
                low: dec!(1.1),
            },
            recurrence_relation_m: dec!(0.05),
        };

        let output_5 = Dispersion {
            range: Range {
                activated: true,
                high: dec!(1.4),
                low: dec!(0.6),
            },
            recurrence_relation_m: dec!(0.388),
        };

        // (dispersion, variance, std dev) after each value
        let outputs = vec![
            (output_1, dec!(0.0), dec!(0.0)),
            (output_2, dec!(0.0025), dec!(0.05)),
            (
                output_3,
                Decimal::from_str("0.006666666667").unwrap(),
                Decimal::from_str("0.081649658092").unwrap(),
            ),
            (
                output_4,
                dec!(0.0125),
                Decimal::from_str("0.111803398875").unwrap(),
            ),
            (
                output_5,
                dec!(0.0776),
                Decimal::from_str("0.278567765544").unwrap(),
            ),
        ];

        for (input, (out, variance, std_dev)) in inputs.into_iter().zip(outputs) {
            dispersion.update(input.prev_mean, input.new_mean, input.new_value);

            // Range checks - exact equality since these are simple operations
            assert_eq!(dispersion.range.activated, out.range.activated);
            assert_eq!(dispersion.range.high, out.range.high);
            assert_eq!(dispersion.range.low, out.range.low);

            // Statistical calculations - check within tolerance due to decimal arithmetic
            let tolerance = Decimal::from_str("0.000000000001").unwrap();

            let recurrence_diff =
                (dispersion.recurrence_relation_m - out.recurrence_relation_m).abs();
            assert!(
                recurrence_diff <= tolerance,
                "Recurrence M difference {} exceeds tolerance",
                recurrence_diff
            );

            let variance_diff = (dispersion.variance(input.value_count) - variance).abs();
            assert!(
                variance_diff <= tolerance,
                "Variance difference {} exceeds tolerance",
                variance_diff
            );

            let std_dev_diff = (dispersion.std_dev(input.value_count) - std_dev).abs();
            assert!(
                std_dev_diff <= tolerance,
                "Standard deviation difference {} exceeds tolerance",
                std_dev_diff
            );
        }
    }

    #[test]
    fn update_range() {
        let dataset = [
            dec!(0.1),
            dec!(1.01),
            dec!(1.02),
            dec!(1.03),
            dec!(1.04),
            dec!(1.05),
            dec!(1.06),
            dec!(1.07),
            dec!(9999.0),
        ];
        let mut actual_range = Range::default();

        for &value in &dataset {
            actual_range.update(value);
        }

        let expected_range = Range {
            activated: true,
            high: dec!(9999.0),
            low: dec!(0.1),
        };

        assert_eq!(actual_range, expected_range);
        assert_eq!(actual_range.range(), dec!(9998.9));
    }
}
