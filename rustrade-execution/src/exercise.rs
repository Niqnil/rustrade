//! Instructions about how a held option is exercised, sent through
//! [`OptionExerciseClient`](crate::client::OptionExerciseClient).
//!
//! At expiry, an option in the money is exercised automatically unless its holder instructs the
//! broker otherwise. A holder who would rather the option lapse, because the account cannot pay
//! for the delivery or does not want the shares, sends a [`Lapse`](ExerciseInstruction::Lapse)
//! instruction before the venue's deadline. When and for which positions to send one is the
//! caller's decision; the client only sends it and reports the venue's answer.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// What the holder of a long option instructs its venue to do with it.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExerciseInstruction {
    /// Do not exercise the option at expiry: let it lapse, in the money or not, forgoing what it
    /// is worth. Some venues call this a do-not-exercise instruction.
    Lapse {
        /// How much of the position lapses.
        contracts: ExerciseQuantity,
    },
}

/// How much of a long option position an [`ExerciseInstruction`] covers.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExerciseQuantity {
    /// The whole position. What it covers once the position changes after the instruction is the
    /// venue's rule; each implementation says what it knows of it.
    WholePosition,
    /// This many contracts of the position. A venue that instructs only whole positions refuses
    /// it with [`ExerciseRefusal::QuantityUnsupported`].
    ///
    /// [`ExerciseRefusal::QuantityUnsupported`]: crate::error::ExerciseRefusal::QuantityUnsupported
    Contracts(Decimal),
}

/// A venue's acceptance of an [`ExerciseInstruction`].
///
/// Accepted is not yet applied: the instruction takes effect at expiry, as the venue applies it.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
pub struct ExerciseAck {
    /// The instruction the venue accepted.
    pub instruction: ExerciseInstruction,
}

impl ExerciseAck {
    /// The venue accepted `instruction`.
    pub const fn new(instruction: ExerciseInstruction) -> Self {
        Self { instruction }
    }
}
