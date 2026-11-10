//! An engine's positions on one simulated venue, saved where a [`VenueCheckpoint`] reached it, so a
//! new engine and a restored venue start from one consistent point.

use super::{AssetCarry, OmsMode, Position, PositionId, PositionManager, PositionSeedError};
use crate::engine::state::{EngineState, asset::AssetStates};
use rust_decimal::Decimal;
use rustrade_execution::{
    exchange::mock::venue::VenueCheckpoint, stored::FormatVersion, trade::AssetFees,
};
use rustrade_instrument::{
    Side,
    asset::{AssetIndex, ExchangeAsset, name::AssetNameInternal},
    exchange::{ExchangeId, ExchangeIndex},
    index::IndexedInstruments,
    instrument::{
        InstrumentIndex,
        kind::InstrumentKind,
        name::{InstrumentNameExchange, InstrumentNameInternal},
    },
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// A simulated venue's [`VenueCheckpoint`], and the engine's open positions on that venue at the
/// point the checkpoint reached it in the account stream.
///
/// The `Engine` returns one as an
/// [`EngineOutput::Checkpoint`](crate::engine::EngineOutput::Checkpoint) when it processes a
/// checkpoint event, having applied every account event the venue sent before it, and none after.
/// So the two halves describe one moment: an order the venue filled before the checkpoint is in
/// both, and one filled after it in neither.
///
/// Restore both halves from the same value: the venue from [`venue`](Self::venue), and the
/// engine's positions with
/// [`EngineStateBuilder::restored_positions`](crate::engine::state::builder::EngineStateBuilder::restored_positions).
/// For a paper venue, `PaperOptions::with_checkpoint` restores the venue and checks the two halves
/// agree.
///
/// Only positions are saved of the engine. Its order state comes back from the venue: a restored
/// venue opens its account stream with a snapshot listing its open orders, which the engine takes
/// up as it does on any connect. Statistics and tear sheets start afresh from the restore.
///
/// # Saving and reading it back
/// Serialise it whole. It carries one format [`version`](Self::version) for both halves, which
/// changes whenever either half's format does; `add_paper` refuses a checkpoint of another
/// version. In a self-describing format such as JSON, fields a later release adds are read with
/// defaults from an older checkpoint; a format such as bincode reads only the exact shape it
/// wrote.
///
/// A checkpoint whose shape a later format changed fails in the deserialiser, without saying why.
/// In a self-describing format such as JSON, read it as a
/// [`Stored`](rustrade_execution::stored::Stored) checkpoint and call
/// [`into_current`](rustrade_execution::stored::Stored::into_current), which reports the version
/// it was written in instead.
///
/// `Ord` and `Hash` exist so it can sit in an [`EngineOutput`](crate::engine::EngineOutput), which
/// requires them, and are not a meaningful ranking.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
pub struct EngineCheckpoint {
    version: u32,
    /// The venue's state at the checkpoint.
    pub venue: VenueCheckpoint,
    /// The engine's open positions on the venue's instruments at the checkpoint, in instrument
    /// order and then slot order.
    pub positions: Vec<PositionCheckpoint>,
}

impl EngineCheckpoint {
    /// The format version this release writes, and the only one it restores.
    ///
    /// A release raises it whenever it raises
    /// [`SimulatedVenueSnapshot::VERSION`](rustrade_execution::exchange::mock::venue::SimulatedVenueSnapshot::VERSION)
    /// or changes the positions' format, so that the checkpoint's version says whether both
    /// halves read. A checkpoint whose venue half is of another version anyway, such as one edited
    /// by hand, is refused when the venue is restored.
    pub const VERSION: u32 = 1;

    /// A checkpoint of `venue` and the engine's `positions` at it, in this release's format
    /// version.
    pub fn new(venue: VenueCheckpoint, positions: Vec<PositionCheckpoint>) -> Self {
        Self {
            version: Self::VERSION,
            venue,
            positions,
        }
    }

    /// The format version this checkpoint was written in.
    pub fn version(&self) -> u32 {
        self.version
    }

    /// The exchange the venue simulates.
    pub fn exchange(&self) -> ExchangeId {
        self.venue.snapshot.exchange()
    }

    /// The venue's number for this checkpoint, as
    /// [`VenueCheckpoint::id`](rustrade_execution::exchange::mock::venue::VenueCheckpoint::id).
    pub fn id(&self) -> u64 {
        self.venue.id
    }

    /// Checks the engine's positions agree with the venue's on every cash-settled instrument: the
    /// engine's net quantity on each, over all its slots, is the venue's net position.
    ///
    /// A spot instrument is not checked. The venue holds a spot position as an asset balance,
    /// which also holds whatever else the account has of that asset, such as a fee taken in it,
    /// so the two need not be equal.
    ///
    /// `instruments` are the instruments the engine is built with, which name the venue's
    /// instruments in the engine's terms.
    ///
    /// # Errors
    /// [`CheckpointMismatch`] for the first instrument on which they differ, or for a position on
    /// an instrument `instruments` do not list for the venue's exchange.
    pub fn check_positions_agree(
        &self,
        instruments: &IndexedInstruments,
    ) -> Result<(), CheckpointMismatch> {
        let exchange = self.exchange();
        let on_exchange = || {
            instruments
                .instruments()
                .iter()
                .map(|keyed| &keyed.value)
                .filter(move |instrument| instrument.exchange.value == exchange)
        };

        for position in &self.positions {
            if !on_exchange().any(|instrument| &instrument.name_internal == position.instrument()) {
                return Err(CheckpointMismatch::UnknownInstrument(
                    position.instrument().clone(),
                ));
            }
        }

        let venue = self.venue.snapshot.positions().collect::<Vec<_>>();
        for instrument in
            on_exchange().filter(|instrument| matches!(instrument.kind, InstrumentKind::Cfd(_)))
        {
            let venue_quantity = venue
                .iter()
                .find(|(name, _)| *name == &instrument.name_exchange)
                .map_or(Decimal::ZERO, |(_, quantity)| *quantity);
            let engine_quantity = self
                .positions
                .iter()
                .filter(|position| position.instrument() == &instrument.name_internal)
                .map(PositionCheckpoint::signed_quantity)
                .sum::<Decimal>();
            if venue_quantity != engine_quantity {
                return Err(CheckpointMismatch::Quantity {
                    instrument: instrument.name_internal.clone(),
                    venue: venue_quantity,
                    engine: engine_quantity,
                });
            }
        }

        // A venue position on an instrument the engine does not list for the exchange.
        if let Some((name, _)) = venue
            .iter()
            .find(|(name, _)| !on_exchange().any(|instrument| &instrument.name_exchange == *name))
        {
            return Err(CheckpointMismatch::UnknownVenueInstrument((*name).clone()));
        }
        Ok(())
    }
}

impl FormatVersion for EngineCheckpoint {
    // The inherent constant: an inherent item takes priority over the trait's, so this does not
    // refer to itself.
    const VERSION: u32 = Self::VERSION;

    fn format_version(&self) -> u32 {
        self.version
    }
}

/// How an [`EngineCheckpoint`]'s two halves disagree: see
/// [`EngineCheckpoint::check_positions_agree`].
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum CheckpointMismatch {
    /// The engine and the venue hold different net positions on a cash-settled instrument.
    #[error("checkpoint: the engine holds {engine} of {instrument}, the venue {venue}")]
    Quantity {
        instrument: InstrumentNameInternal,
        venue: Decimal,
        engine: Decimal,
    },
    /// The engine half holds a position on an instrument not listed for the venue's exchange.
    #[error("checkpoint: the engine holds a position on {0}, which the venue does not trade")]
    UnknownInstrument(InstrumentNameInternal),
    /// The venue half holds a position on an instrument the engine does not list.
    #[error("checkpoint: the venue holds a position on {0}, which the engine does not list")]
    UnknownVenueInstrument(InstrumentNameExchange),
}

/// One open position as an [`EngineCheckpoint`] saves it: the engine's [`Position`] exactly, with
/// its instrument and assets named rather than indexed, and the slot it occupied.
///
/// Named because an index is a position in the engine's instrument list, which another build of
/// the engine need not share. Assets are named within the instrument's exchange.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
pub struct PositionCheckpoint {
    /// The slot the position occupied in its instrument's position manager.
    pub position_id: PositionId,
    /// The position, every field as it was.
    pub position: Position<AssetNameInternal, InstrumentNameInternal>,
}

impl PositionCheckpoint {
    /// The instrument the position is held on.
    pub fn instrument(&self) -> &InstrumentNameInternal {
        &self.position.instrument
    }

    /// The position's quantity, positive long and negative short.
    fn signed_quantity(&self) -> Decimal {
        match self.position.side {
            Side::Buy => self.position.quantity_abs,
            Side::Sell => -self.position.quantity_abs,
        }
    }

    /// Inserts this position into `manager`, the position manager of instrument `key` on
    /// `exchange`, with its assets indexed by `assets`.
    ///
    /// The manager's [`time_history_start`](PositionManager::time_history_start) is not carried
    /// over. It only matters as the later of it and the session start, and a restored engine's
    /// session starts after everything the checkpoint saw, as long as its clock does not start
    /// earlier than the checkpoint.
    pub(crate) fn restore_into(
        self,
        manager: &mut PositionManager,
        key: InstrumentIndex,
        exchange: ExchangeId,
        assets: &AssetStates,
    ) -> Result<(), PositionSeedError> {
        let Self {
            position_id,
            position,
        } = self;
        let instrument = position.instrument.clone();

        // An open position holds a quantity: what a checkpoint holds otherwise was edited.
        if position.quantity_abs <= Decimal::ZERO {
            return Err(PositionSeedError::NonPositiveQuantity {
                instrument,
                quantity_abs: position.quantity_abs,
            });
        }
        if manager.mode == OmsMode::Netting && position_id != PositionId::NETTING {
            return Err(PositionSeedError::PositionIdInNetting {
                instrument,
                position_id,
            });
        }
        if manager.positions.contains_key(&position_id) {
            return Err(PositionSeedError::DuplicateSlot {
                instrument,
                position_id,
            });
        }

        let index = |asset: AssetNameInternal| {
            assets
                .0
                .get_index_of(&ExchangeAsset::new(exchange, asset.clone()))
                .map(AssetIndex)
                .ok_or_else(|| PositionSeedError::UnknownAsset {
                    instrument: instrument.clone(),
                    asset,
                })
        };
        let position = map_position_keys(position, |_| Ok(key), index)?;
        manager.positions.insert(position_id, position);
        Ok(())
    }
}

impl<GlobalData, InstrumentData> EngineState<GlobalData, InstrumentData> {
    /// Every open position held on `exchange`'s instruments, as an [`EngineCheckpoint`] saves it.
    ///
    /// In instrument order, then slot order, so two calls on one state are equal.
    ///
    /// # Panics
    /// If a position holds an asset index this state's assets do not contain, which no position
    /// the engine built can, as for `AssetStates::asset_index`.
    pub fn position_checkpoints(&self, exchange: ExchangeIndex) -> Vec<PositionCheckpoint> {
        let asset_name =
            |index: AssetIndex| -> Result<AssetNameInternal, std::convert::Infallible> {
                // Every asset index a position holds was handed out by this state's own asset list,
                // so it is present: the same invariant `AssetStates::asset_index` panics on.
                let (key, _) = self
                    .assets
                    .0
                    .get_index(index.index())
                    .unwrap_or_else(|| panic!("AssetStates does not contain: {index}"));
                Ok(key.asset.clone())
            };
        self.instruments
            .0
            .values()
            .filter(|state| state.instrument.exchange == exchange)
            .flat_map(|state| {
                let name = &state.instrument.name_internal;
                state
                    .position
                    .positions
                    .iter()
                    .map(move |(position_id, position)| PositionCheckpoint {
                        position_id: position_id.clone(),
                        position: match map_position_keys(
                            position.clone(),
                            |_| Ok(name.clone()),
                            asset_name,
                        ) {
                            Ok(position) => position,
                            Err(never) => match never {},
                        },
                    })
            })
            .collect()
    }
}

/// `position` with its instrument key mapped by `instrument` and every asset key by `asset`.
///
/// Destructures every field, so a field added to [`Position`] fails to compile here until it is
/// carried over.
fn map_position_keys<AssetFrom, AssetTo, InstrumentFrom, InstrumentTo, Error>(
    position: Position<AssetFrom, InstrumentFrom>,
    instrument: impl FnOnce(InstrumentFrom) -> Result<InstrumentTo, Error>,
    asset: impl Fn(AssetFrom) -> Result<AssetTo, Error>,
) -> Result<Position<AssetTo, InstrumentTo>, Error> {
    let Position {
        instrument: instrument_key,
        side,
        price_entry_average,
        quantity_abs,
        quantity_abs_max,
        pnl_unrealised,
        pnl_realised,
        carry,
        carry_assets: AssetCarry { base, other },
        fees_enter,
        fees_exit,
        time_enter,
        time_exchange_update,
        trades,
        orders_enter,
        contract_size,
    } = position;
    let fees = |fees: AssetFees<AssetFrom>| -> Result<AssetFees<AssetTo>, Error> {
        Ok(AssetFees {
            asset: asset(fees.asset)?,
            fees: fees.fees,
            fees_quote: fees.fees_quote,
        })
    };
    Ok(Position {
        instrument: instrument(instrument_key)?,
        side,
        price_entry_average,
        quantity_abs,
        quantity_abs_max,
        pnl_unrealised,
        pnl_realised,
        carry,
        carry_assets: AssetCarry {
            base,
            other: other
                .into_iter()
                .map(|(key, amount)| Ok((asset(key)?, amount)))
                .collect::<Result<_, Error>>()?,
        },
        fees_enter: fees(fees_enter)?,
        fees_exit: fees(fees_exit)?,
        time_enter,
        time_exchange_update,
        trades,
        orders_enter,
        contract_size,
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use crate::engine::state::position::PositionSeed;
    use chrono::{DateTime, Utc};
    use rust_decimal_macros::dec;
    use rustrade_execution::{
        order::id::{OrderId, StrategyId},
        trade::{Trade, TradeId},
    };
    use rustrade_instrument::{
        Underlying,
        asset::Asset,
        instrument::{Instrument, kind::perpetual::PerpetualContract, quote::InstrumentQuoteAsset},
    };

    const SPOT: &str = "binance_spot_btc_usdt";
    const PERP: &str = "binance_futures_usd_btc_usdt_perp";

    fn instruments() -> IndexedInstruments {
        IndexedInstruments::builder()
            .add_instrument(Instrument::spot(
                ExchangeId::BinanceSpot,
                SPOT,
                "BTCUSDT",
                Underlying::new("btc", "usdt"),
                None,
            ))
            .add_instrument(Instrument::new(
                ExchangeId::BinanceFuturesUsd,
                PERP,
                "BTCUSDT",
                Underlying::new("btc", "usdt"),
                InstrumentQuoteAsset::UnderlyingQuote,
                InstrumentKind::Perpetual(PerpetualContract {
                    contract_size: dec!(10),
                    settlement_asset: Asset::new_from_exchange("usdt"),
                }),
                None,
            ))
            .build()
    }

    fn time(secs: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(secs, 0).unwrap()
    }

    fn spot() -> InstrumentNameInternal {
        InstrumentNameInternal::new(SPOT)
    }

    /// A state holding a spot long built from two buys and a partial sell, with entry and exit
    /// fees, realised PnL, carry in the quote, base and another asset, and two entering orders,
    /// and a perpetual long on another exchange.
    fn traded_state(instruments: &IndexedInstruments) -> EngineState<(), ()> {
        let mut state = EngineState::builder(instruments, (), |_| ()).build();
        for (name, trades) in [
            (
                SPOT,
                vec![
                    ("o1", Side::Buy, dec!(50_000), dec!(1), dec!(5)),
                    ("o2", Side::Buy, dec!(51_000), dec!(1), dec!(5)),
                    ("o3", Side::Sell, dec!(52_000), dec!(0.5), dec!(2)),
                ],
            ),
            (
                PERP,
                vec![("p1", Side::Buy, dec!(60_000), dec!(3), dec!(1))],
            ),
        ] {
            let instrument = state
                .instruments
                .instrument_mut(&InstrumentNameInternal::new(name));
            let quote = instrument.instrument.underlying.quote;
            for (i, (order, side, price, quantity, fee)) in trades.into_iter().enumerate() {
                instrument.update_from_trade(&Trade::new(
                    TradeId::new(format!("{order}-trade")),
                    OrderId::new(order),
                    instrument.key,
                    StrategyId::new("strategy"),
                    time(1_000 + i as i64),
                    side,
                    price,
                    quantity,
                    None,
                    AssetFees::new(quote, fee, Some(fee)),
                ));
            }
        }
        let spot = state.instruments.instrument_mut(&spot());
        let base = spot.instrument.underlying.base;
        let position = spot
            .position
            .positions
            .get_mut(&PositionId::NETTING)
            .unwrap();
        position.carry = dec!(-3);
        position.carry_assets.base = dec!(-0.001);
        position.carry_assets.other = vec![(base, dec!(-0.2))];
        state
    }

    fn spot_exchange(instruments: &IndexedInstruments) -> ExchangeIndex {
        instruments
            .find_exchange_index(ExchangeId::BinanceSpot)
            .unwrap()
    }

    /// Saved and restored, every field of a position is as it was, and only the asked exchange's
    /// positions are saved.
    #[test]
    fn a_restored_position_is_the_saved_one_exactly() {
        let instruments = instruments();
        let original = traded_state(&instruments);
        let saved = original.position_checkpoints(spot_exchange(&instruments));
        assert_eq!(saved.len(), 1, "the perpetual is on another exchange");
        assert_eq!(saved[0].instrument(), &spot());
        assert_eq!(
            saved[0].position.carry_assets.other[0].0,
            AssetNameInternal::new("btc")
        );
        assert_eq!(
            saved[0].position.fees_enter.asset,
            AssetNameInternal::new("usdt")
        );
        assert_eq!(
            saved[0].position.orders_enter,
            [OrderId::new("o1"), OrderId::new("o2")]
        );

        let json = serde_json::to_string(&saved).unwrap();
        let restored = EngineState::builder(&instruments, (), |_| ())
            .restored_positions(serde_json::from_str::<Vec<PositionCheckpoint>>(&json).unwrap())
            .try_build::<()>()
            .unwrap();

        let position = |state: &EngineState<(), ()>| {
            state.instruments.instrument(&spot()).position.positions[&PositionId::NETTING].clone()
        };
        assert_eq!(position(&restored), position(&original));
        assert!(position(&original).pnl_realised != Decimal::ZERO);
        assert!(position(&original).fees_exit.fees != Decimal::ZERO);
        assert_eq!(
            restored.position_checkpoints(spot_exchange(&instruments)),
            saved
        );
    }

    const CFD: &str = "binance_spot_btc_usdt_cfd";

    /// The spot instrument, and a CFD with contract size 1 on the same exchange.
    fn cfd_instruments() -> IndexedInstruments {
        IndexedInstruments::builder()
            .add_instrument(Instrument::spot(
                ExchangeId::BinanceSpot,
                SPOT,
                "BTCUSDT",
                Underlying::new("btc", "usdt"),
                None,
            ))
            .add_instrument(Instrument::new(
                ExchangeId::BinanceSpot,
                CFD,
                "BTCCFD",
                Underlying::new("btc", "usdt"),
                InstrumentQuoteAsset::UnderlyingQuote,
                InstrumentKind::Cfd(rustrade_instrument::instrument::kind::cfd::CfdContract {
                    contract_size: dec!(1),
                    settlement_asset: Asset::new_from_exchange("usdt"),
                }),
                None,
            ))
            .build()
    }

    /// The checkpoint of a venue that starts short one CFD contract, answered with `seeds` as
    /// the engine's positions.
    fn cfd_checkpoint(
        instruments: &IndexedInstruments,
        seeds: Vec<PositionSeed>,
    ) -> EngineCheckpoint {
        use rustrade_execution::{
            AccountSnapshot, InstrumentAccountSnapshot,
            balance::{AssetBalance, Balance},
            client::mock::MockExecutionConfig,
            exchange::mock::SimulatedVenue,
            position::{Position as VenuePosition, PositionReport},
        };
        use rustrade_instrument::asset::name::AssetNameExchange;

        let config = MockExecutionConfig::new(
            ExchangeId::BinanceSpot,
            AccountSnapshot {
                exchange: ExchangeId::BinanceSpot,
                balances: vec![AssetBalance::new(
                    AssetNameExchange::new("usdt"),
                    Balance::new(dec!(100_000), dec!(100_000)),
                    time(0),
                )],
                instruments: vec![InstrumentAccountSnapshot {
                    instrument: InstrumentNameExchange::new("BTCCFD"),
                    orders: vec![],
                    orders_complete: true,
                    position: PositionReport::from_position(VenuePosition::new(
                        dec!(-1),
                        Some(dec!(5_000)),
                        None,
                        None,
                        None,
                        None,
                        None,
                        time(0),
                    )),
                    isolated: None,
                }],
            },
            0,
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
            None,
            None,
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
        );
        let mut venue = SimulatedVenue::new_market_driven(
            &config,
            crate::execution::builder::generate_mock_exchange_instruments(
                instruments,
                ExchangeId::BinanceSpot,
            ),
        );
        let positions = EngineState::builder(instruments, (), |_| ())
            .positions(seeds)
            .build::<()>()
            .position_checkpoints(spot_exchange(instruments));
        // The second, so a test reading the id cannot pass on the first's 1 by chance.
        venue.checkpoint();
        EngineCheckpoint::new(venue.checkpoint(), positions)
    }

    fn short_cfd(quantity: Decimal) -> PositionSeed {
        PositionSeed::new(CFD, Side::Sell, quantity, dec!(5_000), time(0))
    }

    /// The two halves agree when the engine is short what the venue is on the CFD, whatever the
    /// engine holds on spot, which is not checked.
    #[test]
    fn halves_holding_the_same_cfd_position_agree() {
        let instruments = cfd_instruments();
        let spot_long = PositionSeed::new(SPOT, Side::Buy, dec!(1), dec!(50_000), time(0));
        let checkpoint = cfd_checkpoint(&instruments, vec![short_cfd(dec!(1)), spot_long]);

        assert_eq!(checkpoint.exchange(), ExchangeId::BinanceSpot);
        assert_eq!(checkpoint.id(), 2);
        assert_eq!(checkpoint.check_positions_agree(&instruments), Ok(()));
    }

    /// A checkpoint reads back as a stored one, and one written before a later position field
    /// existed reads with that field's default; one of another version is refused with it.
    /// A tripwire: a change to either version fails here, so whoever makes it checks the
    /// checkpoint's version rose with the venue snapshot's.
    #[test]
    fn the_checkpoint_version_is_revisited_with_the_venue_snapshot_version() {
        use rustrade_execution::exchange::mock::venue::SimulatedVenueSnapshot;

        assert_eq!(
            (EngineCheckpoint::VERSION, SimulatedVenueSnapshot::VERSION),
            (1, 1),
            "raise EngineCheckpoint::VERSION whenever SimulatedVenueSnapshot::VERSION rises, \
             then update this pair"
        );
    }

    #[test]
    fn a_stored_checkpoint_reads_back_and_an_older_one_takes_a_later_field_s_default() {
        use rustrade_execution::stored::{Stored, StoredFormatError};

        let instruments = cfd_instruments();
        let checkpoint = cfd_checkpoint(&instruments, vec![short_cfd(dec!(1))]);
        assert_eq!(checkpoint.version(), EngineCheckpoint::VERSION);
        let stored = |json: serde_json::Value| {
            serde_json::from_value::<Stored<EngineCheckpoint>>(json)
                .unwrap()
                .into_current()
        };
        let json = serde_json::to_value(&checkpoint).unwrap();
        assert_eq!(stored(json.clone()), Ok(checkpoint.clone()));

        // `carry_assets` is a later field of a position.
        let mut older = json.clone();
        let position = older["positions"][0]["position"].as_object_mut().unwrap();
        assert!(position.remove("carry_assets").is_some());
        let read = stored(older).unwrap();
        assert_eq!(
            read.positions[0].position.carry_assets,
            AssetCarry::default()
        );
        assert_eq!(read.check_positions_agree(&instruments), Ok(()));

        // This version in a shape it does not read in is malformed.
        let mut malformed = json.clone();
        malformed["positions"] = serde_json::json!("a later shape");
        assert_eq!(
            stored(malformed),
            Err(StoredFormatError::Malformed {
                version: EngineCheckpoint::VERSION
            })
        );

        // A later version is refused, whether it still reads in this shape or not.
        let unsupported = Err(StoredFormatError::UnsupportedVersion {
            found: EngineCheckpoint::VERSION + 1,
            supported: EngineCheckpoint::VERSION,
        });
        let mut later = json;
        later["version"] = serde_json::json!(EngineCheckpoint::VERSION + 1);
        assert_eq!(stored(later.clone()), unsupported);
        later["positions"] = serde_json::json!("a later shape");
        assert_eq!(stored(later), unsupported);
    }

    #[test]
    fn halves_holding_different_cfd_positions_disagree() {
        let instruments = cfd_instruments();
        let cfd = InstrumentNameInternal::new(CFD);

        let checkpoint = cfd_checkpoint(&instruments, vec![short_cfd(dec!(2))]);
        assert_eq!(
            checkpoint.check_positions_agree(&instruments),
            Err(CheckpointMismatch::Quantity {
                instrument: cfd.clone(),
                venue: dec!(-1),
                engine: dec!(-2),
            })
        );

        let flat_engine = cfd_checkpoint(&instruments, vec![]);
        assert_eq!(
            flat_engine.check_positions_agree(&instruments),
            Err(CheckpointMismatch::Quantity {
                instrument: cfd,
                venue: dec!(-1),
                engine: Decimal::ZERO,
            })
        );
    }

    #[test]
    fn a_half_on_an_instrument_the_other_does_not_list_disagrees() {
        let instruments = cfd_instruments();
        let mut checkpoint = cfd_checkpoint(&instruments, vec![short_cfd(dec!(1))]);
        // Checked against instruments that list the spot pair alone on the venue's exchange.
        let spot_only = instruments_spot_only();
        assert_eq!(
            checkpoint.check_positions_agree(&spot_only),
            Err(CheckpointMismatch::UnknownInstrument(
                InstrumentNameInternal::new(CFD)
            ))
        );

        checkpoint.positions.clear();
        assert_eq!(
            checkpoint.check_positions_agree(&spot_only),
            Err(CheckpointMismatch::UnknownVenueInstrument(
                InstrumentNameExchange::new("BTCCFD")
            ))
        );
    }

    fn instruments_spot_only() -> IndexedInstruments {
        IndexedInstruments::builder()
            .add_instrument(Instrument::spot(
                ExchangeId::BinanceSpot,
                SPOT,
                "BTCUSDT",
                Underlying::new("btc", "usdt"),
                None,
            ))
            .build()
    }

    #[test]
    fn a_restored_position_on_an_unknown_instrument_or_asset_or_without_quantity_is_refused() {
        let instruments = instruments();
        let saved = traded_state(&instruments).position_checkpoints(spot_exchange(&instruments));
        let build = |positions: Vec<PositionCheckpoint>| {
            EngineState::builder(&instruments, (), |_| ())
                .restored_positions(positions)
                .try_build::<()>()
                .map(|_| ())
        };

        let mut unknown_instrument = saved.clone();
        unknown_instrument[0].position.instrument = InstrumentNameInternal::new("nope");
        assert_eq!(
            build(unknown_instrument),
            Err(PositionSeedError::UnknownInstrument {
                instrument: InstrumentNameInternal::new("nope")
            })
        );

        let mut unknown_asset = saved.clone();
        unknown_asset[0].position.fees_exit.asset = AssetNameInternal::new("bnb");
        assert_eq!(
            build(unknown_asset),
            Err(PositionSeedError::UnknownAsset {
                instrument: spot(),
                asset: AssetNameInternal::new("bnb")
            })
        );

        for quantity_abs in [Decimal::ZERO, dec!(-1)] {
            let mut empty = saved.clone();
            empty[0].position.quantity_abs = quantity_abs;
            assert_eq!(
                build(empty),
                Err(PositionSeedError::NonPositiveQuantity {
                    instrument: spot(),
                    quantity_abs
                })
            );
        }
    }

    #[test]
    fn a_restored_position_in_a_taken_or_foreign_slot_is_refused() {
        let instruments = instruments();
        let saved = traded_state(&instruments).position_checkpoints(spot_exchange(&instruments));

        let twice = EngineState::builder(&instruments, (), |_| ())
            .restored_positions(saved.clone())
            .restored_positions(saved.clone())
            .try_build::<()>()
            .map(|_| ());
        assert_eq!(
            twice,
            Err(PositionSeedError::DuplicateSlot {
                instrument: spot(),
                position_id: PositionId::NETTING
            })
        );

        let mut hedged = saved;
        hedged[0].position_id = PositionId::new("slot");
        let refused = EngineState::builder(&instruments, (), |_| ())
            .restored_positions(hedged.clone())
            .try_build::<()>()
            .map(|_| ());
        assert_eq!(
            refused,
            Err(PositionSeedError::PositionIdInNetting {
                instrument: spot(),
                position_id: PositionId::new("slot")
            })
        );
        let in_hedging = EngineState::builder(&instruments, (), |_| ())
            .oms_mode(OmsMode::Hedging)
            .restored_positions(hedged)
            .try_build::<()>()
            .unwrap();
        assert!(
            in_hedging
                .instruments
                .instrument(&spot())
                .position
                .positions
                .contains_key(&PositionId::new("slot"))
        );
    }
}
