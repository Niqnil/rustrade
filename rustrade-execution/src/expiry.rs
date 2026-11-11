//! How an option position settles at expiry, and what a simulated venue sells before it.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// How a long option position settles at expiry, whoever settles it: an engine that settles
/// expiry itself, or the [`SimulatedVenue`](crate::exchange::mock::SimulatedVenue). Short options
/// and futures are not affected.
#[non_exhaustive]
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize,
)]
pub enum LongOptionExpiry {
    /// An option in the money is exercised, against the underlying's price as whoever settles it
    /// takes it (see their documentation). One out of the money settles at zero. The default, matching an exchange that exercises in-the-money
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

/// When a [`SimulatedVenue`](crate::exchange::mock::SimulatedVenue) sells a long option the
/// account cannot afford to exercise, before it expires, as a broker does for an account short of
/// the cash or shares an exercise needs.
///
/// Set on [`MockExecutionConfig::expiry_sell_out`]. For example, Alpaca exercises a long option in
/// the money at expiry, but sells it in the last hour before expiry when the account lacks the
/// buying power (a call) or the shares (a put):
///
/// ```
/// use rust_decimal_macros::dec;
/// use rustrade_execution::expiry::{ExpirySellOut, SellOutAffordability};
/// use std::time::Duration;
///
/// // Checked an hour before expiry, the whole position sold if it cannot all be exercised, at 2%
/// // under the price a market sell would fill at.
/// let sell_out = ExpirySellOut::new(Duration::from_secs(60 * 60))
///     .with_affordability(SellOutAffordability::PerPosition)
///     .with_haircut(dec!(0.02))
///     .unwrap();
/// ```
///
/// # What the venue does
/// At [`before_expiry`](Self::before_expiry) before each physically settled option's expiry, the
/// venue looks at every long it holds in that option:
/// - **In the money?** Against the underlying's price as expiry settlement takes it, the mid of
///   its spot market, else its last price. Only an option in the money, by any amount, is sold.
///   Without a price for the underlying the venue cannot tell, warns, and sells nothing.
/// - **Affordable?** Exercising a call pays `strike × quantity × contract_size` of quote: the
///   account's free quote, plus on a venue with spot margin what it would lend for a buy (its
///   leverage limit and the asset's borrow cap). Exercising a put delivers `quantity ×
///   contract_size` of the underlying, which the account must hold free: shares securing a short
///   call are not free, and the venue never lends shares to exercise a put. Several longs checked
///   at one instant share what the account has, by instrument name, and what one sale brings in
///   does not count towards another's exercise.
/// - **What is sold:** under [`SellOutAffordability::PerPosition`], the whole position, unless
///   all of it is affordable; under [`SellOutAffordability::PerContract`], the contracts beyond
///   what is affordable.
/// - **How:** the orders still working on the option are cancelled first, then the venue sells as
///   a market sell would fill: priced by its fill model, moved by its slippage, up to the depth
///   on offer, paying a taker's sell-side fee. The price is then cut by the
///   [`haircut`](Self::haircut). The trade is reported as a
///   [`TradeOrigin::Liquidation`](crate::trade::TradeOrigin::Liquidation), with no client order
///   id.
///
/// Whatever is not sold, because it was affordable or out of the money, or because the option or
/// its underlying had no price, the depth ran out or the account could not pay the fee, goes on to
/// expiry and settles under [`MockExecutionConfig::long_option_expiry`]. A cash-settled option is
/// never sold: exercising it needs no money.
///
/// # Known limitations
/// - **One check.** A broker sells at some point in a window before expiry. The venue checks once,
///   at the instant this sets, against the market it holds then, however old. A price that
///   arrives later in the window does not sell what the check could not.
/// - **No order cut-off.** Orders that open or add to an expiring position are still taken
///   between the check and expiry, which a broker may refuse.
/// - **Only the account's own money.** What exercising one option receives (a put's cash) does
///   not fund another (a call), and an option a little out of the money, which a broker may also
///   sell, is left to expire.
/// - **The haircut is fixed.** Where a broker's forced sale fills is not documented; the haircut
///   is a modelling choice, and zero sells at the price a market sell would get.
///
/// [`MockExecutionConfig::expiry_sell_out`]:
///     crate::client::mock::MockExecutionConfig::expiry_sell_out
/// [`MockExecutionConfig::long_option_expiry`]:
///     crate::client::mock::MockExecutionConfig::long_option_expiry
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
#[serde(try_from = "ExpirySellOutRepr", into = "ExpirySellOutRepr")]
pub struct ExpirySellOut {
    before_expiry: Duration,
    affordability: SellOutAffordability,
    haircut: Decimal,
}

/// How much of a long option in the money the venue sells when the account cannot afford to
/// exercise all of it. See [`ExpirySellOut`].
#[non_exhaustive]
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize,
)]
pub enum SellOutAffordability {
    /// The whole position is sold unless the account can exercise all of it, as Alpaca documents
    /// its sell-out. The default.
    #[default]
    PerPosition,
    /// As many whole contracts as the account can exercise are kept for expiry, and the rest are
    /// sold, so a position can be partly exercised and partly sold.
    PerContract,
}

/// The serialised form of an [`ExpirySellOut`], before validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
struct ExpirySellOutRepr {
    before_expiry: Duration,
    #[serde(default)]
    affordability: SellOutAffordability,
    #[serde(default, with = "rust_decimal::serde::str")]
    haircut: Decimal,
}

/// Why an [`ExpirySellOut`] was refused.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ExpirySellOutError {
    /// The haircut is below zero, or not below one: a sale at or below nothing.
    #[error("expiry sell-out haircut {haircut} is outside [0, 1)")]
    Haircut {
        /// The haircut refused.
        haircut: Decimal,
    },
}

impl ExpirySellOut {
    /// Sells `before_expiry` before each expiry, the whole position unless all of it is
    /// affordable ([`SellOutAffordability::PerPosition`]), at the price a market sell would get
    /// (no haircut).
    pub const fn new(before_expiry: Duration) -> Self {
        Self {
            before_expiry,
            affordability: SellOutAffordability::PerPosition,
            haircut: Decimal::ZERO,
        }
    }

    /// With `affordability` deciding how much of a position is sold.
    #[must_use]
    pub const fn with_affordability(mut self, affordability: SellOutAffordability) -> Self {
        self.affordability = affordability;
        self
    }

    /// With the sale's price cut by `haircut`, a fraction of it: `0.02` sells at 98% of the price
    /// a market sell would get.
    ///
    /// # Errors
    /// [`ExpirySellOutError::Haircut`] unless `0 <= haircut < 1`.
    pub fn with_haircut(mut self, haircut: Decimal) -> Result<Self, ExpirySellOutError> {
        if !(Decimal::ZERO..Decimal::ONE).contains(&haircut) {
            return Err(ExpirySellOutError::Haircut { haircut });
        }
        self.haircut = haircut;
        Ok(self)
    }

    /// How long before an option's expiry the venue checks it.
    pub const fn before_expiry(&self) -> Duration {
        self.before_expiry
    }

    /// How much of a position the venue sells.
    pub const fn affordability(&self) -> SellOutAffordability {
        self.affordability
    }

    /// The fraction the sale's price is cut by.
    pub const fn haircut(&self) -> Decimal {
        self.haircut
    }
}

impl TryFrom<ExpirySellOutRepr> for ExpirySellOut {
    type Error = ExpirySellOutError;

    fn try_from(repr: ExpirySellOutRepr) -> Result<Self, Self::Error> {
        Self::new(repr.before_expiry)
            .with_affordability(repr.affordability)
            .with_haircut(repr.haircut)
    }
}

impl From<ExpirySellOut> for ExpirySellOutRepr {
    fn from(sell_out: ExpirySellOut) -> Self {
        Self {
            before_expiry: sell_out.before_expiry,
            affordability: sell_out.affordability,
            haircut: sell_out.haircut,
        }
    }
}
