//! [`PositionSeed`]s from the positions a venue reports in an account snapshot.

use super::PositionSeed;
use rustrade_execution::{UnindexedAccountSnapshot, position::Position as VenuePosition};
use rustrade_instrument::{
    Side,
    index::IndexedInstruments,
    instrument::name::{InstrumentNameExchange, InstrumentNameInternal},
};
use serde::{Deserialize, Serialize};

impl PositionSeed {
    /// Seed the netting slot from a position a venue reported, such as an
    /// [`InstrumentAccountSnapshot::position`](rustrade_execution::InstrumentAccountSnapshot::position).
    ///
    /// `instrument` is the engine's name for the instrument the venue reported the position on.
    /// [`VenuePositionSeeds::from_account_snapshot`] resolves it from the instruments the engine
    /// is built with.
    ///
    /// Returns `None` when the position is flat, or when the venue reported no entry price: a
    /// seed needs one, and inventing it would misstate the position's PnL.
    ///
    /// # What the seed holds
    /// - The side from the sign of the venue's quantity, and its absolute value as the quantity.
    /// - The venue's entry price as reported. It is quoted as orders are priced, so for an option
    ///   or a future it is per unit of the underlying, as a fill's price is. IBKR folds
    ///   commissions into it, so realised PnL at close measured from it is already net of the
    ///   entry commission. Alpaca's excludes them. Hyperliquid does not document whether its
    ///   `entryPx` includes fees.
    /// - No entry fees: the venue does not report them separately. Add them with
    ///   [`Self::with_fees_enter`] only for a venue whose entry price excludes them.
    /// - The position's `time_exchange`, when it was read, as its `time_enter`. The venues do not
    ///   report an entry time (the Alpaca, IBKR and Hyperliquid clients stamp their own clock),
    ///   so this is later than the true entry.
    /// - No slot: under [`OmsMode::Hedging`](super::OmsMode::Hedging) add one with
    ///   [`Self::with_position_id`].
    pub fn from_venue_position(
        instrument: impl Into<InstrumentNameInternal>,
        position: &VenuePosition,
    ) -> Option<Self> {
        if position.is_flat() {
            return None;
        }
        let side = if position.is_long() {
            Side::Buy
        } else {
            Side::Sell
        };
        Some(Self::new(
            instrument,
            side,
            position.abs_quantity(),
            position.entry_price?,
            position.time_exchange,
        ))
    }
}

/// [`PositionSeed`]s built from the positions in one venue's account snapshot, plus every open
/// position that could not be seeded.
///
/// `#[non_exhaustive]`: built by [`Self::from_account_snapshot`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
#[non_exhaustive]
pub struct VenuePositionSeeds {
    /// One netting-slot seed per open position the snapshot reports, in snapshot order.
    pub seeds: Vec<PositionSeed>,
    /// Open positions that have no seed, and why. The engine will not know about them.
    pub skipped: Vec<SkippedVenuePosition>,
}

impl VenuePositionSeeds {
    /// Build a seed for each open position in `snapshot`, resolving the venue's instrument name
    /// to the engine's through `instruments`, the instruments the engine is built with.
    ///
    /// The snapshot is typically a client's
    /// [`account_snapshot`](rustrade_execution::client::ExecutionClient::account_snapshot),
    /// fetched before the engine starts. Pass the seeds to
    /// [`SystemBuilder::positions`](crate::system::builder::SystemBuilder::positions) or
    /// [`EngineStateBuilder::positions`](crate::engine::state::builder::EngineStateBuilder::positions).
    ///
    /// An instrument whose position is unreported or flat yields nothing. An open position yields
    /// either a seed, built by [`PositionSeed::from_venue_position`], or an entry in
    /// [`Self::skipped`]. Check `skipped`: starting with positions left out means the engine
    /// does not know it holds them.
    ///
    /// Only venues that report positions yield seeds (see
    /// [`InstrumentAccountSnapshot::position`](rustrade_execution::InstrumentAccountSnapshot::position)).
    /// A holding reported as an asset balance, such as spot crypto, has no position to seed.
    ///
    /// Each snapshot entry yields its own seed, so a snapshot listing one instrument twice yields
    /// two, which the build rejects as
    /// [`PositionSeedError::DuplicateSlot`](super::PositionSeedError::DuplicateSlot). Seeds take
    /// the netting slot; under [`OmsMode::Hedging`](super::OmsMode::Hedging) give each a slot with
    /// [`PositionSeed::with_position_id`] before building.
    pub fn from_account_snapshot(
        instruments: &IndexedInstruments,
        snapshot: &UnindexedAccountSnapshot,
    ) -> Self {
        let mut out = Self::default();
        for instrument_snapshot in &snapshot.instruments {
            let Some(position) = instrument_snapshot
                .position
                .open()
                .filter(|position| !position.is_flat())
            else {
                continue;
            };
            let name_exchange = &instrument_snapshot.instrument;
            let name_internal = instruments.instruments().iter().find_map(|keyed| {
                let instrument = &keyed.value;
                (instrument.exchange.value == snapshot.exchange
                    && instrument.name_exchange == *name_exchange)
                    .then_some(&instrument.name_internal)
            });

            let seed = match name_internal {
                None => Err(VenuePositionSkipReason::UnknownInstrument),
                // Flat positions were filtered above, so `None` here means no entry price.
                Some(name_internal) => {
                    PositionSeed::from_venue_position(name_internal.clone(), position)
                        .ok_or(VenuePositionSkipReason::NoEntryPrice)
                }
            };
            match seed {
                Ok(seed) => out.seeds.push(seed),
                Err(reason) => out.skipped.push(SkippedVenuePosition {
                    instrument: name_exchange.clone(),
                    position: position.clone(),
                    reason,
                }),
            }
        }
        out
    }
}

/// An open venue position [`VenuePositionSeeds::from_account_snapshot`] could not seed.
///
/// `#[non_exhaustive]`: built by [`VenuePositionSeeds::from_account_snapshot`].
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[non_exhaustive]
pub struct SkippedVenuePosition {
    /// The venue's name for the instrument.
    pub instrument: InstrumentNameExchange,
    /// The position as the venue reported it.
    pub position: VenuePosition,
    /// Why it has no seed.
    pub reason: VenuePositionSkipReason,
}

/// Why an open venue position has no [`PositionSeed`].
///
/// `#[non_exhaustive]`: a further reason can be added without breaking downstream exhaustive
/// matches.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize)]
#[non_exhaustive]
pub enum VenuePositionSkipReason {
    /// The instrument is not among those the engine is built with, on the snapshot's exchange.
    UnknownInstrument,
    /// The venue reported no entry price, which a seed requires.
    NoEntryPrice,
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panicking on a bad fixture is acceptable
mod tests {
    use super::*;
    use chrono::{DateTime, Utc};
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use rustrade_execution::{
        AccountSnapshot, InstrumentAccountSnapshot, position::PositionReport,
    };
    use rustrade_instrument::{
        asset::name::AssetNameExchange, exchange::ExchangeId, test_utils::instrument,
    };

    const EXCHANGE: ExchangeId = ExchangeId::AlpacaBroker;
    const OTHER: ExchangeId = ExchangeId::Ibkr;

    fn time() -> DateTime<Utc> {
        DateTime::<Utc>::from_timestamp(1_700_000_000, 0).unwrap()
    }

    fn venue_position(quantity: Decimal, entry_price: Option<Decimal>) -> VenuePosition {
        VenuePosition::new(quantity, entry_price, None, None, None, None, time())
    }

    fn instrument_snapshot(
        name: &str,
        position: Option<VenuePosition>,
    ) -> InstrumentAccountSnapshot<ExchangeId, AssetNameExchange, InstrumentNameExchange> {
        let position = position.map_or(PositionReport::Unreported, PositionReport::from_position);
        InstrumentAccountSnapshot::new(name.into(), vec![], false, position, None)
    }

    #[test]
    fn long_position_seeds_a_buy() {
        let seed =
            PositionSeed::from_venue_position("x", &venue_position(dec!(2.5), Some(dec!(10))))
                .unwrap();
        assert_eq!(
            seed,
            PositionSeed::new("x", Side::Buy, dec!(2.5), dec!(10), time())
        );
    }

    #[test]
    fn short_position_seeds_a_sell_with_absolute_quantity() {
        let seed = PositionSeed::from_venue_position("x", &venue_position(dec!(-3), Some(dec!(7))))
            .unwrap();
        assert_eq!(
            seed,
            PositionSeed::new("x", Side::Sell, dec!(3), dec!(7), time())
        );
    }

    #[test]
    fn flat_or_unpriced_position_seeds_nothing() {
        for position in [
            venue_position(Decimal::ZERO, Some(dec!(7))),
            venue_position(-Decimal::ZERO, Some(dec!(7))),
            venue_position(Decimal::ZERO, None),
            venue_position(dec!(1), None),
        ] {
            assert_eq!(PositionSeed::from_venue_position("x", &position), None);
        }
    }

    #[test]
    fn snapshot_seeds_open_positions_and_reports_the_rest() {
        let instruments = IndexedInstruments::new([
            instrument(EXCHANGE, "aapl", "usd"),
            instrument(EXCHANGE, "msft", "usd"),
            // Same venue name on another exchange: must not resolve the snapshot's instrument.
            instrument(OTHER, "tsla", "usd"),
        ]);
        let internal = |i: usize| instruments.instruments()[i].value.name_internal.clone();

        let snapshot = AccountSnapshot::new(
            EXCHANGE,
            vec![],
            vec![
                instrument_snapshot("aapl_usd", Some(venue_position(dec!(-4), Some(dec!(100))))),
                // No position, and a flat one: nothing to seed or report.
                instrument_snapshot("msft_usd", None),
                instrument_snapshot("msft_usd", Some(venue_position(Decimal::ZERO, None))),
                instrument_snapshot("tsla_usd", Some(venue_position(dec!(1), Some(dec!(5))))),
                instrument_snapshot("msft_usd", Some(venue_position(dec!(2), None))),
            ],
        );

        let seeds = VenuePositionSeeds::from_account_snapshot(&instruments, &snapshot);

        assert_eq!(
            seeds.seeds,
            vec![PositionSeed::new(
                internal(0),
                Side::Sell,
                dec!(4),
                dec!(100),
                time()
            )]
        );
        assert_eq!(
            seeds.skipped,
            vec![
                SkippedVenuePosition {
                    instrument: "tsla_usd".into(),
                    position: venue_position(dec!(1), Some(dec!(5))),
                    reason: VenuePositionSkipReason::UnknownInstrument,
                },
                SkippedVenuePosition {
                    instrument: "msft_usd".into(),
                    position: venue_position(dec!(2), None),
                    reason: VenuePositionSkipReason::NoEntryPrice,
                },
            ]
        );
    }
}
