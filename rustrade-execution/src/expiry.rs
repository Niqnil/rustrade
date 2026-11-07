//! How an option position settles at expiry.

use serde::{Deserialize, Serialize};

/// How a long option position settles at expiry, whoever settles it: an engine that settles
/// expiry itself, or the [`SimulatedVenue`](crate::exchange::mock::SimulatedVenue). Short options
/// and futures are not affected.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub enum LongOptionExpiry {
    /// An option in the money is exercised, against the underlying's last price. One out of the
    /// money settles at zero. The default, matching an exchange that exercises in-the-money
    /// options automatically. What exercise does to the account is up to whoever settles it: an
    /// engine settles in cash at the intrinsic value, and the simulated venue settles as the
    /// contract's [`OptionSettlement`](rustrade_instrument::instrument::kind::option::OptionSettlement)
    /// says.
    #[default]
    Exercise,
    /// Every long option lapses: the position settles at zero, in the money or not, and no
    /// underlying price is needed for it. This matches a holder who instructs the broker not to
    /// exercise any option still held at expiry.
    Lapse,
}
