use derive_more::Constructor;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(
    Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize, Constructor,
)]
pub struct InstrumentSpec<AssetKey> {
    pub price: InstrumentSpecPrice,
    pub quantity: InstrumentSpecQuantity<AssetKey>,
    pub notional: InstrumentSpecNotional,
}

#[derive(
    Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize, Constructor,
)]
pub struct InstrumentSpecPrice {
    pub min: Decimal,
    pub tick_size: Decimal,
}

#[derive(
    Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize, Constructor,
)]
pub struct InstrumentSpecQuantity<AssetKey> {
    pub unit: OrderQuantityUnits<AssetKey>,
    pub min: Decimal,
    pub increment: Decimal,
}

/// What an [`InstrumentSpecQuantity`] counts quantities in.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize)]
pub enum OrderQuantityUnits<AssetKey> {
    /// The named asset.
    ///
    /// Prefer [`Base`](Self::Base) for the instrument's own base asset: `Asset` holds a key of its
    /// own, which must equal the instrument's base key to count in the base. On an unindexed
    /// instrument the keys are names, so a spelling that differs from the base's (`btc` and `BTC`)
    /// is another asset. An instrument index registers this asset alongside the instrument's own.
    Asset(AssetKey),
    /// Contracts of a derivative.
    Contract,
    /// The quote asset.
    Quote,
    /// This instrument's own base asset, whatever its key or spelling.
    ///
    /// The unit an order's quantity is counted in on most spot venues. It names no asset, so it
    /// cannot disagree with the instrument's base.
    Base,
}

/// Why [`Instrument::try_quantity_step`](super::Instrument::try_quantity_step) has no step.
///
/// It only reports: what to do about it, refuse to start or carry on unrounded, is the caller's.
/// Non-exhaustive, so a later reason is not a breaking change.
#[derive(
    Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize, Error,
)]
#[non_exhaustive]
pub enum StepUnavailable<AssetKey> {
    /// The instrument has no [`InstrumentSpec`].
    #[error("the instrument has no spec")]
    NoSpec,
    /// The spec counts quantities in a unit other than the one a position is counted in. That
    /// includes [`OrderQuantityUnits::Base`] on a derivative whose contract is not one unit of its
    /// base, where a position is counted in contracts.
    #[error("the spec counts quantities in {0:?}, not in the unit a position is counted in")]
    OtherUnit(OrderQuantityUnits<AssetKey>),
    /// The spec's quantity increment is not above zero.
    #[error("the spec's quantity increment {0} is not above zero")]
    NotPositive(Decimal),
}

#[derive(
    Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize, Constructor,
)]
pub struct InstrumentSpecNotional {
    pub min: Decimal,
}
