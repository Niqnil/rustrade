//! What a simulated venue holds against a short option.

use serde::{Deserialize, Serialize};

/// How much quote a short put holds on the
/// [`SimulatedVenue`](crate::exchange::mock::SimulatedVenue), which takes only fully secured
/// shorts: a put written against cash set aside to buy the underlying at the strike if it is
/// assigned. See its `# How an option is accounted`.
///
/// Brokers differ on whether the premium the sale receives counts towards that cash. A short call
/// holds the underlying it would deliver, which the premium cannot pay for, so this setting does
/// not apply to it.
#[non_exhaustive]
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize,
)]
pub enum ShortPutCollateral {
    /// The account must hold `strike × quantity × contract_size` of quote before the sale, and
    /// the premium it receives does not count towards it. The default, and the stricter rule:
    /// a cash-secured put as the options clearing house's risk disclosure defines it, and as
    /// Interactive Brokers holds it in a cash account.
    #[default]
    Strike,
    /// The premium the sale receives counts towards the strike, so the account needs only
    /// `strike × quantity × contract_size` less that premium beforehand. Alpaca holds a
    /// cash-secured put this way. A limit sale resting on the book holds the strike less the
    /// premium at its limit price, and gives back what a better fill does not need.
    StrikeLessPremium,
}
