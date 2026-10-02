use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Represents all possible errors that can occur when building, or searching for indexes in, an
/// [`IndexedInstruments`](super::IndexedInstruments) collection.
///
/// `#[non_exhaustive]`: further failure causes can be added without breaking downstream exhaustive
/// matches.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize, Error)]
#[non_exhaustive]
pub enum IndexError {
    /// Indicates a failure to find an [`ExchangeIndex`](crate::exchange::ExchangeIndex) for a
    /// given exchange identifier.
    ///
    /// Contains a description of the failed lookup attempt.
    #[error("ExchangeIndex: {0}")]
    ExchangeIndex(String),

    /// Indicates a failure to find an [`AssetIndex`](crate::asset::AssetIndex) for a given
    /// asset identifier.
    ///
    /// Contains a description of the failed lookup attempt.
    #[error("AssetIndex: {0}")]
    AssetIndex(String),

    /// Indicates a failure to find an [`InstrumentIndex`](crate::instrument::InstrumentIndex)
    /// for a given instrument identifier.
    ///
    /// Contains a description of the failed lookup attempt.
    #[error("InstrumentIndex: {0}")]
    InstrumentIndex(String),

    /// Two or more [`Instrument`](crate::instrument::Instrument)s share one
    /// [`InstrumentNameInternal`](crate::instrument::name::InstrumentNameInternal), violating the
    /// uniqueness invariant that index-keyed state depends on.
    ///
    /// Downstream state maps are keyed on `InstrumentNameInternal` but read **positionally** by
    /// `InstrumentIndex`, so duplicates collapse the map and silently shift every index past the
    /// collision onto the wrong instrument. Detected while building rather than left to corrupt
    /// state at runtime.
    ///
    /// Contains a description naming the duplicated name and the instruments that share it.
    #[error("duplicate InstrumentNameInternal: {0}")]
    DuplicateInstrumentNameInternal(String),

    /// Two or more [`Instrument`](crate::instrument::Instrument)s on one exchange share an
    /// [`InstrumentNameExchange`](crate::instrument::name::InstrumentNameExchange).
    ///
    /// The exchange-side name is how a venue reports an instrument, so every order, fill and
    /// position it sends is resolved back to an instrument by `(exchange, name_exchange)`. With two
    /// candidates, each lookup would pick one arbitrarily: a fill for a spot `AAPL` could land on a
    /// CFD named `AAPL` on the same venue, with that CFD's contract size and settlement asset.
    ///
    /// Contains a description naming the duplicated name, the exchange, and the instruments that
    /// share it.
    #[error("duplicate InstrumentNameExchange: {0}")]
    DuplicateInstrumentNameExchange(String),

    /// An [`Instrument`](crate::instrument::Instrument)'s `contract_size` is not a positive
    /// multiplier.
    ///
    /// `contract_size` multiplies every money quantity derived from an instrument — quote
    /// notional, unrealised and realised PnL, and any notional-scaled fee model. Neither
    /// degenerate value fails anywhere downstream:
    /// - **Zero** makes every notional, fee and PnL zero, so a backtest trades freely, is charged
    ///   nothing and never moves. It reads as a strategy that found no edge.
    /// - **Negative** inverts the sign of PnL, so a losing strategy reports a profit.
    ///
    /// Checked while building because that is the last point at which the value is still
    /// attributable to the instrument that carried it; past it the multiplier is copied into
    /// positions and fee calculations, where a wrong number is indistinguishable from a wrong
    /// price.
    ///
    /// Contains a description naming the instrument and the rejected value.
    #[error("invalid contract_size: {0}")]
    InvalidContractSize(String),

    /// Two distinct [`Asset`](crate::asset::Asset)s on one exchange share an
    /// [`AssetNameInternal`](crate::asset::name::AssetNameInternal), differing only in
    /// `name_exchange`.
    ///
    /// The asset-side twin of [`Self::DuplicateInstrumentNameInternal`]. Asset lookups and
    /// downstream asset state are keyed on `(exchange, name_internal)` while being read
    /// **positionally** by `AssetIndex`, so the pair would take two indices but collapse into one
    /// entry, and every index past it would resolve to the wrong asset.
    ///
    /// Contains a description naming the exchange, the shared name, and both `name_exchange`s.
    #[error("duplicate AssetNameInternal: {0}")]
    DuplicateAssetNameInternal(String),
}
