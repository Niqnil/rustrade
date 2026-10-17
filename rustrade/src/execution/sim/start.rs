//! One starting state for a simulated venue and the `Engine` trading on it.

use crate::engine::state::position::PositionSeed;
use chrono::{DateTime, Utc};
use derive_more::Constructor;
use fnv::FnvHashMap;
use rust_decimal::Decimal;
use rustrade_execution::{
    AccountSnapshot, InstrumentAccountSnapshot, UnindexedAccountSnapshot,
    balance::{AssetBalance, Balance},
    position::{Position, PositionReport},
};
use rustrade_instrument::{
    Side,
    asset::name::AssetNameExchange,
    exchange::ExchangeId,
    instrument::{
        Instrument,
        kind::InstrumentKind,
        name::{InstrumentNameExchange, InstrumentNameInternal},
    },
};
use rustrade_integration::collection::FnvIndexMap;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Where a simulated venue, and the `Engine` trading on it, both start: the assets the account
/// holds, and the positions it holds with their entry prices.
///
/// Passed to [`SimExecutionBuilder::add_venue_with_start`], which derives both sides from it so
/// they cannot disagree:
/// - **The venue's opening account.** Each balance, plus what each position holds at the venue:
///   - a spot long is held as its base asset, so its quantity is credited to that asset's
///     balance;
///   - a CFD position is held as a position, at its entry price, which the venue's ledger closes
///     against.
/// - **The `Engine`'s opening positions.** The [`positions`](Self::positions) themselves, returned
///   by [`SimExecutionBuild::position_seeds`] for [`EngineStateBuilder::positions`]. The entry
///   price exists only here: a spot venue holds an asset balance, which has none.
///
/// A [`SimStepper`] checks, on its first call, that the `Engine` it is handed holds exactly these
/// positions on the venue's instruments, and returns
/// [`SimStepError::StartMismatch`](super::SimStepError::StartMismatch) otherwise.
///
/// # A reset is a rebuild
/// There is no reset in place. To start an episode again, build the venue and the `Engine` again
/// from the same `SimStart`: nothing is spawned or connected, so it costs little.
///
/// [`SimExecutionBuilder::add_venue_with_start`]: super::SimExecutionBuilder::add_venue_with_start
/// [`SimExecutionBuild::position_seeds`]: super::SimExecutionBuild::position_seeds
/// [`SimStepper`]: super::SimStepper
/// [`EngineStateBuilder::positions`]: crate::engine::state::builder::EngineStateBuilder::positions
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize, Constructor)]
pub struct SimStart {
    /// What the account holds in each asset, apart from the base asset a spot position holds,
    /// which [`positions`](Self::positions) credits. Each asset at most once, and none negative.
    /// An asset the venue trades that is not listed opens at zero.
    ///
    /// Listing a spot position's base asset here as well is allowed only at the quantity the
    /// position holds, so the two cannot disagree.
    ///
    /// A CFD position's margin is not listed here: the venue holds it in the position, as the full
    /// notional at entry, and closing the position pays it back with the realised PnL.
    pub balances: Vec<StartBalance>,
    /// Positions held at the start, each naming its instrument as the `Engine` does. At most one
    /// per instrument, since the venue nets.
    ///
    /// Spot positions must be long: a spot short needs a margin account, which the simulated venue
    /// does not model.
    pub positions: Vec<PositionSeed>,
}

/// How much of one asset a [`SimStart`] account holds, all of it free.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize, Constructor,
)]
pub struct StartBalance {
    /// The asset, as the venue names it.
    pub asset: AssetNameExchange,
    /// How much is held. Free in full: a starting account has no open orders to reserve against.
    pub total: Decimal,
}

/// Why a [`SimStart`] could not be applied to a simulated venue.
///
/// `#[non_exhaustive]`: a later check adds a variant without a breaking change.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize, Error)]
#[non_exhaustive]
pub enum SimStartError {
    /// The venue's configuration already carries an opening account, so there would be two
    /// sources for it. Leave `MockExecutionConfig::initial_state` empty when starting from a
    /// [`SimStart`].
    #[error(
        "{exchange}'s MockExecutionConfig::initial_state already holds balances or instruments; \
         a SimStart must be its only starting state"
    )]
    InitialStateNotEmpty { exchange: ExchangeId },
    /// An asset is listed more than once in [`SimStart::balances`].
    #[error("SimStart lists {asset} more than once")]
    DuplicateBalance { asset: AssetNameExchange },
    /// A balance in [`SimStart::balances`] is negative: a spot account cannot owe an asset.
    #[error("SimStart lists {asset} at {total}; a balance cannot be negative")]
    NegativeBalance {
        asset: AssetNameExchange,
        total: Decimal,
    },
    /// A position names an instrument the venue does not trade.
    #[error("SimStart position in {instrument}, which {exchange} does not trade")]
    UnknownInstrument {
        instrument: InstrumentNameInternal,
        exchange: ExchangeId,
    },
    /// A position names an instrument kind the simulated venue cannot hold.
    #[error("SimStart position in {instrument}, whose kind the simulated venue cannot hold")]
    UnsupportedKind { instrument: InstrumentNameInternal },
    /// A position has no quantity.
    #[error("SimStart position in {instrument} has quantity {quantity}; it must be positive")]
    InvalidQuantity {
        instrument: InstrumentNameInternal,
        quantity: Decimal,
    },
    /// A spot position is short, which needs a margin account the simulated venue does not model.
    #[error("SimStart position in {instrument} is a spot short, which the venue cannot hold")]
    SpotShort { instrument: InstrumentNameInternal },
    /// More than one position names one instrument, but the venue holds one net position each.
    #[error("SimStart holds more than one position in {instrument}; the venue nets them")]
    DuplicatePosition { instrument: InstrumentNameInternal },
    /// A spot position's base asset is also listed as a balance, at a different quantity.
    #[error(
        "SimStart lists {asset} at {balance}, but its spot positions hold {positions}; list it \
         at that quantity or not at all"
    )]
    BaseBalanceConflict {
        asset: AssetNameExchange,
        balance: Decimal,
        positions: Decimal,
    },
}

impl SimStart {
    /// The venue's opening account and the `Engine`'s position seeds, both from this start.
    ///
    /// `instruments` is what the venue trades, keyed by its own name for each.
    pub(crate) fn resolve(
        self,
        exchange: ExchangeId,
        instruments: &FnvHashMap<InstrumentNameExchange, Instrument<ExchangeId, AssetNameExchange>>,
    ) -> Result<(UnindexedAccountSnapshot, Vec<PositionSeed>), SimStartError> {
        let mut balances = FnvIndexMap::<AssetNameExchange, Decimal>::default();
        for StartBalance { asset, total } in self.balances {
            if total < Decimal::ZERO {
                return Err(SimStartError::NegativeBalance { asset, total });
            }
            if balances.insert(asset.clone(), total).is_some() {
                return Err(SimStartError::DuplicateBalance { asset });
            }
        }

        let by_internal = instruments
            .iter()
            .map(|(name, instrument)| (&instrument.name_internal, (name, instrument)))
            .collect::<FnvHashMap<_, _>>();

        let mut held = FnvIndexMap::<AssetNameExchange, Decimal>::default();
        let mut cfds = Vec::new();
        let mut seen = FnvHashMap::<&InstrumentNameInternal, ()>::default();

        for seed in &self.positions {
            let Some((name, instrument)) = by_internal.get(&seed.instrument) else {
                return Err(SimStartError::UnknownInstrument {
                    instrument: seed.instrument.clone(),
                    exchange,
                });
            };
            if seen.insert(&seed.instrument, ()).is_some() {
                return Err(SimStartError::DuplicatePosition {
                    instrument: seed.instrument.clone(),
                });
            }
            if seed.quantity_abs <= Decimal::ZERO {
                return Err(SimStartError::InvalidQuantity {
                    instrument: seed.instrument.clone(),
                    quantity: seed.quantity_abs,
                });
            }

            match instrument.kind {
                InstrumentKind::Spot => {
                    if seed.side == Side::Sell {
                        return Err(SimStartError::SpotShort {
                            instrument: seed.instrument.clone(),
                        });
                    }
                    *held.entry(instrument.underlying.base.clone()).or_default() +=
                        seed.quantity_abs;
                }
                InstrumentKind::Cfd(_) => {
                    let quantity = match seed.side {
                        Side::Buy => seed.quantity_abs,
                        Side::Sell => -seed.quantity_abs,
                    };
                    cfds.push(InstrumentAccountSnapshot {
                        instrument: (*name).clone(),
                        orders: Vec::new(),
                        orders_complete: false,
                        position: PositionReport::Open(Position::new(
                            quantity,
                            Some(seed.price_entry_average),
                            None,
                            None,
                            None,
                            None,
                            None,
                            seed.time_enter,
                        )),
                        isolated: None,
                    });
                }
                _ => {
                    return Err(SimStartError::UnsupportedKind {
                        instrument: seed.instrument.clone(),
                    });
                }
            }
        }

        for (asset, positions) in held {
            match balances.get(&asset) {
                Some(balance) if *balance != positions => {
                    return Err(SimStartError::BaseBalanceConflict {
                        asset,
                        balance: *balance,
                        positions,
                    });
                }
                Some(_) => {}
                None => {
                    balances.insert(asset, positions);
                }
            }
        }

        // The venue's ledger needs a balance for every asset it trades, and debiting one it lacks
        // is a mis-specified account. An asset this start does not mention opens at zero, sorted so
        // the opening account is the same on every run.
        let mut traded = instruments
            .values()
            .flat_map(|instrument| {
                let settlement = match &instrument.kind {
                    InstrumentKind::Cfd(contract) => Some(contract.settlement_asset.clone()),
                    _ => None,
                };
                [
                    Some(instrument.underlying.base.clone()),
                    Some(instrument.underlying.quote.clone()),
                    settlement,
                ]
            })
            .flatten()
            .collect::<Vec<_>>();
        traded.sort();
        for asset in traded {
            balances.entry(asset).or_default();
        }

        let snapshot = AccountSnapshot {
            exchange,
            balances: balances
                .into_iter()
                .map(|(asset, total)| AssetBalance {
                    asset,
                    balance: Balance::new(total, total),
                    // Re-stamped to the session start when the venue's account is seeded into the
                    // `Engine` (see `SimRunner::new`), so the instant written here is never read.
                    time_exchange: DateTime::<Utc>::MIN_UTC,
                })
                .collect(),
            instruments: cfds,
        };

        Ok((snapshot, self.positions))
    }
}

#[cfg(test)]
// Test code: panicking on a bad fixture is acceptable.
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::execution::builder::generate_mock_exchange_instruments;
    use rust_decimal_macros::dec;
    use rustrade_instrument::{
        index::IndexedInstruments, instrument::kind::cfd::CfdContract, test_utils::instrument,
    };

    const EXCHANGE: ExchangeId = ExchangeId::BinanceSpot;

    fn at(secs: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(secs, 0).unwrap()
    }

    fn venue_instruments()
    -> FnvHashMap<InstrumentNameExchange, Instrument<ExchangeId, AssetNameExchange>> {
        let mut cfd = instrument(EXCHANGE, "spx500", "usdt");
        cfd.kind = InstrumentKind::Cfd(CfdContract {
            contract_size: dec!(1),
            settlement_asset: cfd.underlying.quote.clone(),
        });
        let instruments = IndexedInstruments::new([
            instrument(EXCHANGE, "btc", "usdt"),
            instrument(EXCHANGE, "eth", "usdt"),
            cfd,
        ]);
        generate_mock_exchange_instruments(&instruments, EXCHANGE)
    }

    fn usdt(total: Decimal) -> StartBalance {
        StartBalance::new(AssetNameExchange::new("usdt"), total)
    }

    fn seed(instrument: &str, side: Side, quantity: Decimal, entry: Decimal) -> PositionSeed {
        PositionSeed::new(instrument, side, quantity, entry, at(0))
    }

    fn balance(snapshot: &UnindexedAccountSnapshot, asset: &str) -> Option<Decimal> {
        snapshot
            .balances
            .iter()
            .find(|balance| balance.asset == AssetNameExchange::new(asset))
            .map(|balance| balance.balance.total)
    }

    #[test]
    fn a_spot_long_is_credited_to_its_base_asset_and_seeded_into_the_engine() {
        let start = SimStart::new(
            vec![usdt(dec!(1000))],
            vec![seed(
                "binance_spot-btc_usdt",
                Side::Buy,
                dec!(2),
                dec!(30000),
            )],
        );

        let (snapshot, seeds) = start
            .clone()
            .resolve(EXCHANGE, &venue_instruments())
            .unwrap();

        assert_eq!(balance(&snapshot, "usdt"), Some(dec!(1000)));
        assert_eq!(balance(&snapshot, "btc"), Some(dec!(2)));
        assert_eq!(
            balance(&snapshot, "eth"),
            Some(Decimal::ZERO),
            "traded, so opened at zero"
        );
        assert!(
            snapshot.instruments.is_empty(),
            "a spot holding is a balance"
        );
        assert_eq!(seeds, start.positions);
    }

    #[test]
    fn a_cfd_position_is_seeded_into_the_venue_at_its_signed_quantity_and_entry() {
        let start = SimStart::new(
            vec![usdt(dec!(1000))],
            vec![seed(
                "binance_spot-spx500_usdt",
                Side::Sell,
                dec!(3),
                dec!(5000),
            )],
        );

        let (snapshot, _) = start.resolve(EXCHANGE, &venue_instruments()).unwrap();

        let position = snapshot.instruments[0].position.open().unwrap();
        assert_eq!(position.quantity, dec!(-3));
        assert_eq!(position.entry_price, Some(dec!(5000)));
        assert_eq!(
            balance(&snapshot, "spx500"),
            Some(Decimal::ZERO),
            "a CFD holds no base asset"
        );
    }

    #[test]
    fn a_cfd_settled_in_its_own_asset_opens_that_asset_at_zero() {
        let mut cfd = instrument(EXCHANGE, "spx500", "usdt");
        cfd.kind = InstrumentKind::Cfd(CfdContract {
            contract_size: dec!(1),
            settlement_asset: rustrade_instrument::asset::Asset::new_from_exchange("usd"),
        });
        let instruments =
            generate_mock_exchange_instruments(&IndexedInstruments::new([cfd]), EXCHANGE);

        let (snapshot, _) = SimStart::default().resolve(EXCHANGE, &instruments).unwrap();

        assert_eq!(balance(&snapshot, "usd"), Some(Decimal::ZERO));
    }

    #[test]
    fn spot_longs_sharing_a_base_asset_sum_into_one_balance() {
        let mut instruments = venue_instruments();
        let btc_eth = generate_mock_exchange_instruments(
            &IndexedInstruments::new([instrument(EXCHANGE, "btc", "eth")]),
            EXCHANGE,
        );
        instruments.extend(btc_eth);

        let start = SimStart::new(
            vec![],
            vec![
                seed("binance_spot-btc_usdt", Side::Buy, dec!(2), dec!(30000)),
                seed("binance_spot-btc_eth", Side::Buy, dec!(1), dec!(15)),
            ],
        );

        let (snapshot, _) = start.resolve(EXCHANGE, &instruments).unwrap();

        assert_eq!(balance(&snapshot, "btc"), Some(dec!(3)));
    }

    #[test]
    fn a_base_balance_listed_at_the_positions_quantity_is_accepted() {
        let start = SimStart::new(
            vec![StartBalance::new(AssetNameExchange::new("btc"), dec!(2))],
            vec![seed(
                "binance_spot-btc_usdt",
                Side::Buy,
                dec!(2),
                dec!(30000),
            )],
        );

        let (snapshot, _) = start.resolve(EXCHANGE, &venue_instruments()).unwrap();

        assert_eq!(balance(&snapshot, "btc"), Some(dec!(2)));
    }

    #[test]
    fn a_start_that_cannot_hold_is_refused_with_the_reason() {
        let instruments = venue_instruments();
        let refused = |start: SimStart| start.resolve(EXCHANGE, &instruments).unwrap_err();
        let btc = || seed("binance_spot-btc_usdt", Side::Buy, dec!(2), dec!(30000));

        assert_eq!(
            refused(SimStart::new(
                vec![StartBalance::new(AssetNameExchange::new("btc"), dec!(5))],
                vec![btc()],
            )),
            SimStartError::BaseBalanceConflict {
                asset: AssetNameExchange::new("btc"),
                balance: dec!(5),
                positions: dec!(2),
            }
        );
        assert_eq!(
            refused(SimStart::new(vec![usdt(dec!(1)), usdt(dec!(2))], vec![])),
            SimStartError::DuplicateBalance {
                asset: AssetNameExchange::new("usdt")
            }
        );
        assert_eq!(
            refused(SimStart::new(vec![usdt(dec!(-1))], vec![])),
            SimStartError::NegativeBalance {
                asset: AssetNameExchange::new("usdt"),
                total: dec!(-1),
            }
        );
        // An empty balance is not a negative one.
        assert!(
            SimStart::new(vec![usdt(dec!(0))], vec![])
                .resolve(EXCHANGE, &instruments)
                .is_ok()
        );
        assert_eq!(
            refused(SimStart::new(vec![], vec![btc(), btc()])),
            SimStartError::DuplicatePosition {
                instrument: InstrumentNameInternal::new("binance_spot-btc_usdt")
            }
        );
        assert_eq!(
            refused(SimStart::new(
                vec![],
                vec![seed(
                    "binance_spot-btc_usdt",
                    Side::Sell,
                    dec!(1),
                    dec!(30000)
                )],
            )),
            SimStartError::SpotShort {
                instrument: InstrumentNameInternal::new("binance_spot-btc_usdt")
            }
        );
        assert_eq!(
            refused(SimStart::new(
                vec![],
                vec![seed(
                    "binance_spot-btc_usdt",
                    Side::Buy,
                    dec!(0),
                    dec!(30000)
                )],
            )),
            SimStartError::InvalidQuantity {
                instrument: InstrumentNameInternal::new("binance_spot-btc_usdt"),
                quantity: dec!(0),
            }
        );
        assert_eq!(
            refused(SimStart::new(
                vec![],
                vec![seed("kraken-sol_usdt", Side::Buy, dec!(1), dec!(100))],
            )),
            SimStartError::UnknownInstrument {
                instrument: InstrumentNameInternal::new("kraken-sol_usdt"),
                exchange: EXCHANGE,
            }
        );
    }
}
