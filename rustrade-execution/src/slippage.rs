//! Adverse slippage on the simulated venue's taker fills.
//!
//! [`SlippageConfig`] says how far a fill is moved against the order that takes liquidity, and
//! [`Slippage`] is that configuration plus the random stream it draws from. The venue applies it
//! to the price its [`FillModel`](crate::fill::FillModel) gives, so slippage composes with every
//! pricing model alike.
//!
//! # Backtest-only
//!
//! Only simulated execution slips a fill. Live execution clients receive real fill prices from the
//! venue.

use rand::{Rng, SeedableRng, rngs::Xoshiro256PlusPlus};
use rust_decimal::Decimal;
use rustrade_instrument::Side;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// How far the simulated venue moves a taker fill against the order, as a fraction of the price the
/// fill would otherwise get.
///
/// A buy fills at `reference × (1 + s)` and a sell at `reference × (1 − s)`, where `reference` is
/// what the venue's [`FillModel`](crate::fill::FillModel) priced the fill at, and `s` is a rate
/// this configuration gives:
/// - [`none`](Self::none), the default: `s = 0`, and the fill is exactly the reference.
/// - [`fixed`](Self::fixed): the same `s` for every fill.
/// - [`uniform`](Self::uniform): a fresh `s` for each fill, drawn uniformly from `[low, high]`,
///   both ends included, from a random stream seeded by the configured seed.
///
/// The rate is a fraction rather than basis points, so `0.01` is one percent. Several percent per
/// side is accepted, and so is anything up to but not including `1`, past which a sell would fill
/// at or below zero.
///
/// # What it applies to
///
/// Every fill that takes liquidity: a market order, and a limit order that is marketable when it
/// arrives. The venue clamps the slipped price to a limit order's own limit, so slippage can cost a
/// limit order its price improvement, but never fill it past its limit. A resting order that the
/// market later reaches fills at its own limit exactly, with no slippage, as a maker's fill does.
///
/// A slipped fill is not bounded by the bar's or the book's range: a large rate on a short bar can
/// print outside that bar's high and low. Capping it would silently erase most of the rate.
///
/// # Reproducibility
///
/// [`uniform`](Self::uniform) draws from its own generator, seeded by the `u64` in this
/// configuration and used for nothing else, so a run is reproducible from its seed: the same seed
/// and the same sequence of orders give the same fill prices. The generator is xoshiro256++, which
/// the `rand` crate guarantees gives the same stream in every version. One draw is made for each
/// fill that trades: an order that is refused, cancelled on arrival or rests without trading
/// takes none. A rate whose two ends are equal makes no draw at all, so a zero rate is identical
/// to configuring no slippage.
///
/// # Validation
///
/// The constructors refuse a rate outside `0 ≤ low ≤ high < 1`, and so does deserialising one,
/// so a `SlippageConfig` that exists is one the venue can apply.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Deserialize, Serialize,
)]
#[serde(try_from = "SlippageRepr", into = "SlippageRepr")]
pub struct SlippageConfig(SlippageRepr);

/// The serialised form of a [`SlippageConfig`], before validation.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Deserialize, Serialize,
)]
enum SlippageRepr {
    #[default]
    None,
    Fixed {
        #[serde(with = "rust_decimal::serde::str")]
        rate: Decimal,
    },
    Uniform {
        #[serde(with = "rust_decimal::serde::str")]
        low: Decimal,
        #[serde(with = "rust_decimal::serde::str")]
        high: Decimal,
        seed: u64,
    },
}

impl SlippageConfig {
    /// No slippage: every fill is exactly the price the fill model gives. The default.
    pub const fn none() -> Self {
        Self(SlippageRepr::None)
    }

    /// The same rate for every fill.
    ///
    /// # Errors
    /// Returns [`SlippageConfigError`] unless `0 ≤ rate < 1`.
    pub fn fixed(rate: Decimal) -> Result<Self, SlippageConfigError> {
        Self::try_from(SlippageRepr::Fixed { rate })
    }

    /// A rate drawn for each fill, uniformly from `[low, high]` with both ends included, from a
    /// generator seeded by `seed`.
    ///
    /// # Errors
    /// Returns [`SlippageConfigError`] unless `0 ≤ low ≤ high < 1`.
    pub fn uniform(low: Decimal, high: Decimal, seed: u64) -> Result<Self, SlippageConfigError> {
        Self::try_from(SlippageRepr::Uniform { low, high, seed })
    }

    /// The lowest rate this configuration can give a fill: zero for [`none`](Self::none), the
    /// rate for [`fixed`](Self::fixed).
    pub fn low(&self) -> Decimal {
        match self.0 {
            SlippageRepr::None => Decimal::ZERO,
            SlippageRepr::Fixed { rate } => rate,
            SlippageRepr::Uniform { low, .. } => low,
        }
    }

    /// The highest rate this configuration can give a fill: zero for [`none`](Self::none), the
    /// rate for [`fixed`](Self::fixed).
    pub fn high(&self) -> Decimal {
        match self.0 {
            SlippageRepr::None => Decimal::ZERO,
            SlippageRepr::Fixed { rate } => rate,
            SlippageRepr::Uniform { high, .. } => high,
        }
    }

    /// The seed of a [`uniform`](Self::uniform) configuration, `None` for any other.
    pub fn seed(&self) -> Option<u64> {
        match self.0 {
            SlippageRepr::Uniform { seed, .. } => Some(seed),
            SlippageRepr::None | SlippageRepr::Fixed { .. } => None,
        }
    }
}

impl TryFrom<SlippageRepr> for SlippageConfig {
    type Error = SlippageConfigError;

    fn try_from(repr: SlippageRepr) -> Result<Self, Self::Error> {
        let (low, high) = match repr {
            SlippageRepr::None => return Ok(Self(repr)),
            SlippageRepr::Fixed { rate } => (rate, rate),
            SlippageRepr::Uniform { low, high, .. } => (low, high),
        };
        if low.is_sign_negative() && !low.is_zero() {
            return Err(SlippageConfigError::Negative(low));
        }
        if high >= Decimal::ONE {
            return Err(SlippageConfigError::NotBelowOne(high));
        }
        if low > high {
            return Err(SlippageConfigError::Inverted { low, high });
        }
        Ok(Self(repr))
    }
}

impl From<SlippageConfig> for SlippageRepr {
    fn from(config: SlippageConfig) -> Self {
        config.0
    }
}

/// Why a [`SlippageConfig`] was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum SlippageConfigError {
    /// A rate below zero, which would move a fill in the order's favour.
    #[error("slippage rate {0} is negative; slippage is adverse, so a rate is at least 0")]
    Negative(Decimal),
    /// A rate of one or more, which would fill a sell at or below zero.
    #[error("slippage rate {0} is not below 1, so a sell would fill at or below zero")]
    NotBelowOne(Decimal),
    /// A range whose low end is above its high end.
    #[error("slippage range is inverted: low {low} is above high {high}")]
    Inverted { low: Decimal, high: Decimal },
}

/// A [`SlippageConfig`] and the random stream it draws from: what the simulated venue applies to
/// each taker fill.
///
/// Built once per venue from its configuration, and held for the venue's lifetime, so the stream
/// advances one draw per fill across the whole run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Slippage {
    config: SlippageConfig,
    rng: Xoshiro256PlusPlus,
}

impl Slippage {
    /// The slippage `config` describes, its stream at the start of its seed.
    pub fn new(config: SlippageConfig) -> Self {
        Self {
            config,
            // A configuration without a seed never draws, so the stream it gets is never read.
            rng: Xoshiro256PlusPlus::seed_from_u64(config.seed().unwrap_or_default()),
        }
    }

    /// The configuration this slippage was built from.
    pub fn config(&self) -> SlippageConfig {
        self.config
    }

    /// `reference` moved against an order trading `side`, by a rate drawn for this one fill.
    ///
    /// Advances the stream by one draw unless the configured range is a single rate.
    pub fn apply(&mut self, side: Side, reference: Decimal) -> Decimal {
        let rate = self.draw();
        if rate.is_zero() {
            return reference;
        }
        match side {
            Side::Buy => reference * (Decimal::ONE + rate),
            Side::Sell => reference * (Decimal::ONE - rate),
        }
    }

    /// The rate for one fill.
    fn draw(&mut self) -> Decimal {
        let (low, high) = (self.config.low(), self.config.high());
        if low == high {
            return low;
        }
        // `k / u64::MAX` spans `[0, 1]` with both ends reachable, which is what makes the range
        // inclusive. Mapped here rather than by `rand`'s range sampling, whose algorithm the crate
        // does not promise to keep, so a seed's prices hold across `rand` versions. Both factors
        // of the product lie in `[0, 1]`, so it cannot overflow.
        let unit = Decimal::from(self.rng.next_u64()) / Decimal::from(u64::MAX);
        low + (high - low) * unit
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;

    fn d(s: &str) -> Decimal {
        s.parse().unwrap()
    }

    #[test]
    fn none_is_the_default_and_leaves_every_price_alone() {
        let mut slippage = Slippage::new(SlippageConfig::default());
        assert_eq!(SlippageConfig::default(), SlippageConfig::none());
        assert_eq!(slippage.apply(Side::Buy, d("100.5")), d("100.5"));
        assert_eq!(slippage.apply(Side::Sell, d("100.5")), d("100.5"));
    }

    #[test]
    fn a_fixed_rate_moves_a_buy_up_and_a_sell_down() {
        let mut slippage = Slippage::new(SlippageConfig::fixed(d("0.01")).unwrap());
        assert_eq!(slippage.apply(Side::Buy, d("200")), d("202"));
        assert_eq!(slippage.apply(Side::Sell, d("200")), d("198"));
    }

    #[test]
    fn several_percent_is_accepted() {
        let mut slippage = Slippage::new(SlippageConfig::fixed(d("0.05")).unwrap());
        assert_eq!(slippage.apply(Side::Sell, d("100")), d("95"));
    }

    /// A range of one rate never reads the stream, so it is the fixed rate exactly, and a zero
    /// range is no slippage at all.
    #[test]
    fn a_range_of_one_rate_makes_no_draw() {
        let mut ranged = Slippage::new(SlippageConfig::uniform(d("0.02"), d("0.02"), 9).unwrap());
        let untouched = ranged.rng.clone();
        assert_eq!(ranged.apply(Side::Buy, d("50")), d("51"));
        assert_eq!(
            ranged.rng, untouched,
            "a single-rate range drew from its stream"
        );

        let mut zero =
            Slippage::new(SlippageConfig::uniform(Decimal::ZERO, Decimal::ZERO, 9).unwrap());
        assert_eq!(zero.apply(Side::Buy, d("50.25")), d("50.25"));
    }

    #[test]
    fn every_uniform_draw_lies_inside_its_range() {
        let (low, high) = (d("0.01"), d("0.025"));
        let mut slippage = Slippage::new(SlippageConfig::uniform(low, high, 7).unwrap());
        for _ in 0..10_000 {
            let rate = slippage.draw();
            assert!(
                low <= rate && rate <= high,
                "{rate} outside [{low}, {high}]"
            );
        }
    }

    #[test]
    fn the_same_seed_gives_the_same_prices_and_another_seed_does_not() {
        let config = |seed| SlippageConfig::uniform(d("0.01"), d("0.025"), seed).unwrap();
        let prices = |seed| {
            let mut slippage = Slippage::new(config(seed));
            (0..32)
                .map(|i| {
                    let side = if i % 2 == 0 { Side::Buy } else { Side::Sell };
                    slippage.apply(side, d("100"))
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(prices(42), prices(42));
        assert_ne!(prices(42), prices(43));
    }

    /// Pins the stream a seed gives, so a change of generator or of the mapping from its output
    /// to a rate, either of which would silently change every seeded backtest's prices, fails here.
    #[test]
    fn a_seed_gives_the_same_rates_in_every_version() {
        let mut slippage =
            Slippage::new(SlippageConfig::uniform(d("0.01"), d("0.025"), 42).unwrap());
        let rates = (0..3).map(|_| slippage.draw()).collect::<Vec<_>>();
        assert_eq!(rates, GOLDEN_RATES.map(d).to_vec());
    }

    /// The first three rates of seed 42 over `[0.01, 0.025]`. Cross-checked against an independent
    /// implementation of SplitMix64 seeding and xoshiro256++ from their published reference code.
    const GOLDEN_RATES: [&str; 3] = [
        "0.0222145771768436479843294918",
        "0.0147823156009249183850225810",
        "0.0247584125226623325452642722",
    ];

    #[test]
    fn the_constructors_refuse_a_rate_the_venue_cannot_apply() {
        assert_eq!(
            SlippageConfig::fixed(d("-0.01")),
            Err(SlippageConfigError::Negative(d("-0.01")))
        );
        assert_eq!(
            SlippageConfig::fixed(Decimal::ONE),
            Err(SlippageConfigError::NotBelowOne(Decimal::ONE))
        );
        assert_eq!(
            SlippageConfig::uniform(d("0.02"), d("0.01"), 0),
            Err(SlippageConfigError::Inverted {
                low: d("0.02"),
                high: d("0.01")
            })
        );
        assert!(
            SlippageConfig::fixed(d("-0")).is_ok(),
            "negative zero is zero"
        );
        assert!(SlippageConfig::fixed(d("0.999")).is_ok());
    }

    #[test]
    fn deserialising_validates_and_round_trips() {
        let config = SlippageConfig::uniform(d("0.01"), d("0.025"), 42).unwrap();
        let json = serde_json::to_string(&config).unwrap();
        assert_eq!(
            json,
            r#"{"Uniform":{"low":"0.01","high":"0.025","seed":42}}"#
        );
        assert_eq!(
            serde_json::from_str::<SlippageConfig>(&json).unwrap(),
            config
        );

        assert_eq!(
            serde_json::from_str::<SlippageConfig>(r#"{"Fixed":{"rate":"0.01"}}"#).unwrap(),
            SlippageConfig::fixed(d("0.01")).unwrap()
        );
        assert_eq!(
            serde_json::from_str::<SlippageConfig>(r#""None""#).unwrap(),
            SlippageConfig::none()
        );
        let refused = serde_json::from_str::<SlippageConfig>(r#"{"Fixed":{"rate":"1.5"}}"#);
        assert!(refused.unwrap_err().to_string().contains("not below 1"));
    }
}
