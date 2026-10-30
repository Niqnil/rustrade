//! One starting state for a simulated venue and the `Engine` trading on it.

use crate::engine::state::position::PositionSeed;
use chrono::{DateTime, Utc};
use derive_more::Constructor;
use fnv::FnvHashMap;
use rust_decimal::Decimal;
use rustrade_execution::{
    AccountSnapshot, InstrumentAccountSnapshot, UnindexedAccountSnapshot,
    balance::{AssetBalance, Balance, MarginDetails},
    margin::SpotMarginConfig,
    position::{Position, PositionReport},
};
use rustrade_instrument::{
    Side, Underlying,
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
/// holds and owes, and the positions it holds with their entry prices.
///
/// Passed to [`SimExecutionBuilder::add_venue_with_start`], which derives both sides from it so
/// they cannot disagree:
/// - **The venue's opening account.** Each balance with its loan, plus what each position holds at
///   the venue:
///   - spot positions are held as their base asset, netted per base asset as a cross margin
///     account nets them: a net long is credited to that asset's balance, and a net short is
///     borrowed, so the asset opens owing the net quantity and holding none of it;
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
/// # Margin
/// A spot short, and a loan in [`balances`](Self::balances), need a venue that lends: one whose
/// `MockExecutionConfig::spot_margin` is set. A cash venue refuses both. On a venue that lends:
/// - **A short's proceeds are listed, not credited.** The quote its sale raised is listed in
///   `balances`, as the cash a long leaves after its purchase is: the start credits or debits
///   nothing for either side.
/// - **Interest** on what the account starts owing is charged from the venue's interest model's
///   first boundary after the venue's clock first moves, with nothing charged for the borrow
///   itself, since the loan was taken before the start. Interest listed as already owed earns
///   none, and is repaid before principal, as interest the venue charges is.
/// - **Leverage is checked at the start.** Its debt may not exceed what the venue lends,
///   `(max_leverage − 1) × equity`, valued in the configuration's valuation asset. No market
///   exists yet, so the start is valued at its own prices: the valuation asset at one, and an asset
///   a spot position pairs with the valuation asset, in either direction, at that position's
///   entry price. An asset with no such price adds nothing to equity, as the venue values an
///   asset it has no market for, and a debt in one is refused, since it could be any size.
///
///   The check stops there. Once markets arrive, the venue values the account at them, and lends
///   nothing more while the account is over its limit. It does not liquidate.
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
    /// What the account holds and owes in each asset, apart from what the spot positions hold or
    /// borrow of their base assets, which [`positions`](Self::positions) adds. Each asset at most
    /// once, with no negative total or loan. An asset the venue trades that is not listed opens at
    /// zero, owing nothing.
    ///
    /// Listing a spot position's base asset here as well is allowed only where the two agree: its
    /// total less its borrowed principal must equal the positions' net quantity, negative for a
    /// net short. Listing it is how a start owes more than the net, or owes interest already.
    ///
    /// A CFD position's margin is not listed here: the venue holds it in the position, as the full
    /// notional at entry, and closing the position pays it back with the realised PnL.
    pub balances: Vec<StartBalance>,
    /// Positions held at the start, each naming its instrument as the `Engine` does. At most one
    /// per instrument, since the venue nets.
    ///
    /// A spot short needs a venue that lends: see [Margin](Self#margin).
    pub positions: Vec<PositionSeed>,
}

/// How much of one asset a [`SimStart`] account holds, all of it free, and what it owes in it.
///
/// `#[non_exhaustive]`: construct with [`StartBalance::new`] and add a loan with
/// [`StartBalance::with_loan`], so a field can be added without a breaking change.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
#[non_exhaustive]
pub struct StartBalance {
    /// The asset, as the venue names it.
    pub asset: AssetNameExchange,
    /// How much is held. Free in full: a starting account has no open orders to reserve against.
    pub total: Decimal,
    /// What the account owes in the asset: the principal borrowed, and the interest accrued on it.
    /// Zero by default, including when absent from a deserialised balance. A loan needs a venue
    /// that lends: see [`SimStart`]'s [Margin](SimStart#margin).
    #[serde(default, skip_serializing_if = "owes_nothing")]
    pub loan: MarginDetails,
}

impl StartBalance {
    /// `total` of `asset`, owing nothing.
    pub fn new(asset: AssetNameExchange, total: Decimal) -> Self {
        Self {
            asset,
            total,
            loan: MarginDetails::default(),
        }
    }

    /// This balance, owing `borrowed` principal and `interest` accrued on it.
    #[must_use]
    pub fn with_loan(self, borrowed: Decimal, interest: Decimal) -> Self {
        Self {
            loan: MarginDetails::new(borrowed, interest),
            ..self
        }
    }
}

/// Whether `loan` owes nothing, so a serialised [`StartBalance`] can leave it out.
fn owes_nothing(loan: &MarginDetails) -> bool {
    *loan == MarginDetails::default()
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
    /// A balance in [`SimStart::balances`] is negative. What the account owes is listed as the
    /// balance's loan instead — see [`StartBalance::with_loan`].
    #[error(
        "SimStart lists {asset} at {total}; a balance cannot be negative, so list a debt as its \
         loan"
    )]
    NegativeBalance {
        asset: AssetNameExchange,
        total: Decimal,
    },
    /// A loan in [`SimStart::balances`] has a negative principal or interest.
    #[error(
        "SimStart lists {asset} owing {borrowed} borrowed and {interest} interest; neither can be \
         negative"
    )]
    NegativeLoan {
        asset: AssetNameExchange,
        borrowed: Decimal,
        interest: Decimal,
    },
    /// A balance in [`SimStart::balances`] owes a loan, but the venue does not lend: its
    /// `MockExecutionConfig::spot_margin` is not set.
    #[error("SimStart lists {asset} owing a loan, but {exchange} has no spot_margin to lend it")]
    LoanWithoutMargin {
        asset: AssetNameExchange,
        exchange: ExchangeId,
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
    /// A spot position is short on a venue that does not lend, so the account cannot have
    /// borrowed the base asset it sold: the venue's `MockExecutionConfig::spot_margin` is not set.
    #[error(
        "SimStart position in {instrument} is a spot short, but the venue has no spot_margin to \
         lend its base asset"
    )]
    SpotShort { instrument: InstrumentNameInternal },
    /// More than one position names one instrument, but the venue holds one net position each.
    #[error("SimStart holds more than one position in {instrument}; the venue nets them")]
    DuplicatePosition { instrument: InstrumentNameInternal },
    /// A spot position's base asset is also listed as a balance, whose total less its borrowed
    /// principal, `listed`, differs from the spot positions' net quantity, `positions`.
    #[error(
        "SimStart lists {asset} holding {listed} net of its borrowed principal, but its spot \
         positions net to {positions}; list it at that or not at all"
    )]
    BaseBalanceConflict {
        asset: AssetNameExchange,
        listed: Decimal,
        positions: Decimal,
    },
    /// The start owes `asset`, which it gives no price for in the valuation asset, so its
    /// leverage cannot be checked — see [`SimStart`]'s [Margin](SimStart#margin).
    #[error(
        "SimStart owes {asset}, which it gives no price for in {valuation_asset}; seed a spot \
         position pairing the two"
    )]
    UnpricedDebt {
        asset: AssetNameExchange,
        valuation_asset: AssetNameExchange,
    },
    /// The start owes more than the venue lends against its equity, both valued at the start's
    /// own prices — see [`SimStart`]'s [Margin](SimStart#margin). `unvalued` lists the assets it
    /// holds but gives no price for, which add nothing to `equity`.
    #[error(
        "SimStart owes {debt} {valuation_asset} against an equity of {equity}, more than a max \
         leverage of {max_leverage} lends; held but valued at zero, for want of a price: \
         {unvalued:?}"
    )]
    OverLeveraged {
        valuation_asset: AssetNameExchange,
        debt: Decimal,
        equity: Decimal,
        max_leverage: Decimal,
        unvalued: Vec<AssetNameExchange>,
    },
}

/// A spot instrument a [`SimStart`] seeds a position in, with that position's entry price: what
/// the start values its assets at.
type StartPrice<'a> = (
    &'a InstrumentNameExchange,
    &'a Underlying<AssetNameExchange>,
    Decimal,
);

impl SimStart {
    /// The venue's opening account and the `Engine`'s position seeds, both from this start.
    ///
    /// `instruments` is what the venue trades, keyed by its own name for each, and `spot_margin`
    /// how it lends, if it does.
    pub(crate) fn resolve(
        self,
        exchange: ExchangeId,
        instruments: &FnvHashMap<InstrumentNameExchange, Instrument<ExchangeId, AssetNameExchange>>,
        spot_margin: Option<&SpotMarginConfig>,
    ) -> Result<(UnindexedAccountSnapshot, Vec<PositionSeed>), SimStartError> {
        let lends = spot_margin.is_some();

        let mut balances = FnvIndexMap::<AssetNameExchange, (Decimal, MarginDetails)>::default();
        for StartBalance { asset, total, loan } in self.balances {
            if total < Decimal::ZERO {
                return Err(SimStartError::NegativeBalance { asset, total });
            }
            if loan.borrowed < Decimal::ZERO || loan.interest < Decimal::ZERO {
                return Err(SimStartError::NegativeLoan {
                    asset,
                    borrowed: loan.borrowed,
                    interest: loan.interest,
                });
            }
            if !lends && !owes_nothing(&loan) {
                return Err(SimStartError::LoanWithoutMargin { asset, exchange });
            }
            if balances.insert(asset.clone(), (total, loan)).is_some() {
                return Err(SimStartError::DuplicateBalance { asset });
            }
        }

        let by_internal = instruments
            .iter()
            .map(|(name, instrument)| (&instrument.name_internal, (name, instrument)))
            .collect::<FnvHashMap<_, _>>();

        // Each spot base asset's net quantity: positive held, negative borrowed.
        let mut nets = FnvIndexMap::<AssetNameExchange, Decimal>::default();
        let mut prices = Vec::<StartPrice<'_>>::new();
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
            let quantity = match seed.side {
                Side::Buy => seed.quantity_abs,
                Side::Sell => -seed.quantity_abs,
            };

            match instrument.kind {
                InstrumentKind::Spot => {
                    if seed.side == Side::Sell && !lends {
                        return Err(SimStartError::SpotShort {
                            instrument: seed.instrument.clone(),
                        });
                    }
                    *nets.entry(instrument.underlying.base.clone()).or_default() += quantity;
                    // A price of zero values nothing, and the inverse would divide by it.
                    if seed.price_entry_average > Decimal::ZERO {
                        prices.push((name, &instrument.underlying, seed.price_entry_average));
                    }
                }
                InstrumentKind::Cfd(_) => {
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

        for (asset, net) in nets {
            match balances.get(&asset) {
                Some((total, loan)) if *total - loan.borrowed != net => {
                    return Err(SimStartError::BaseBalanceConflict {
                        asset,
                        listed: *total - loan.borrowed,
                        positions: net,
                    });
                }
                Some(_) => {}
                None => {
                    let opening = match net >= Decimal::ZERO {
                        true => (net, MarginDetails::default()),
                        false => (Decimal::ZERO, MarginDetails::new(-net, Decimal::ZERO)),
                    };
                    balances.insert(asset, opening);
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

        if let Some(config) = spot_margin {
            check_leverage(config, &balances, &prices)?;
        }

        let snapshot = AccountSnapshot {
            exchange,
            balances: balances
                .into_iter()
                .map(|(asset, (total, loan))| AssetBalance {
                    asset,
                    // A margin account reports every balance's loan, owed or not.
                    balance: match lends {
                        true => Balance::new_margin(total, total, loan.borrowed, loan.interest),
                        false => Balance::new(total, total),
                    },
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

/// Refuses a start owing more than `config` lends against its equity, both valued at the start's
/// own `prices` — see [`SimStart`]'s [Margin](SimStart#margin).
///
/// Values as the venue's own lending limit does, so that a start it accepts is one the venue
/// would have lent: equity is each asset's net holding, debt each asset's principal and interest.
fn check_leverage(
    config: &SpotMarginConfig,
    balances: &FnvIndexMap<AssetNameExchange, (Decimal, MarginDetails)>,
    prices: &[StartPrice<'_>],
) -> Result<(), SimStartError> {
    let valuation = config.valuation_asset();
    let mut equity = Decimal::ZERO;
    let mut debt = Decimal::ZERO;
    let mut unvalued = Vec::new();

    for (asset, (total, loan)) in balances {
        let owed = loan.borrowed + loan.interest;
        let Some(price) = price_in(asset, valuation, prices) else {
            if owed > Decimal::ZERO {
                return Err(SimStartError::UnpricedDebt {
                    asset: asset.clone(),
                    valuation_asset: valuation.clone(),
                });
            }
            if *total > Decimal::ZERO {
                unvalued.push(asset.clone());
            }
            continue;
        };
        equity += (*total - owed) * price;
        debt += owed * price;
    }

    let max_leverage = config.max_leverage();
    if debt > (max_leverage - Decimal::ONE) * equity {
        unvalued.sort();
        return Err(SimStartError::OverLeveraged {
            valuation_asset: valuation.clone(),
            debt,
            equity,
            max_leverage,
            unvalued,
        });
    }
    Ok(())
}

/// What one unit of `asset` is worth in `valuation` at a start's own `prices`: one for
/// `valuation` itself, else the entry price of a spot position pairing the two, in either
/// direction. Of several such positions, the one first by the venue's instrument name is used, as
/// the venue chooses among the instruments pairing two assets.
fn price_in(
    asset: &AssetNameExchange,
    valuation: &AssetNameExchange,
    prices: &[StartPrice<'_>],
) -> Option<Decimal> {
    if asset == valuation {
        return Some(Decimal::ONE);
    }
    prices
        .iter()
        .filter_map(|(name, underlying, price)| {
            let value = match (&underlying.base, &underlying.quote) {
                (base, quote) if base == asset && quote == valuation => *price,
                (base, quote) if base == valuation && quote == asset => Decimal::ONE / *price,
                _ => return None,
            };
            Some((*name, value))
        })
        .min_by(|(a, _), (b, _)| a.cmp(b))
        .map(|(_, value)| value)
}

#[cfg(test)]
// Test code: panicking on a bad fixture is acceptable.
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::execution::builder::generate_mock_exchange_instruments;
    use rust_decimal_macros::dec;
    use rustrade_execution::margin::MarginEffect;
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

    fn loan(snapshot: &UnindexedAccountSnapshot, asset: &str) -> Option<MarginDetails> {
        snapshot
            .balances
            .iter()
            .find(|balance| balance.asset == AssetNameExchange::new(asset))
            .and_then(|balance| balance.balance.margin)
    }

    /// Spot margin lending up to `max_leverage`, valued in USDT.
    fn lends(max_leverage: Decimal) -> SpotMarginConfig {
        SpotMarginConfig::new(
            AssetNameExchange::new("usdt"),
            max_leverage,
            MarginEffect::Borrow,
        )
        .unwrap()
    }

    /// The venue's instruments, plus BTC quoted in ETH.
    fn with_btc_eth()
    -> FnvHashMap<InstrumentNameExchange, Instrument<ExchangeId, AssetNameExchange>> {
        let mut instruments = venue_instruments();
        instruments.extend(generate_mock_exchange_instruments(
            &IndexedInstruments::new([instrument(EXCHANGE, "btc", "eth")]),
            EXCHANGE,
        ));
        instruments
    }

    fn short_btc(quantity: Decimal) -> PositionSeed {
        seed("binance_spot-btc_usdt", Side::Sell, quantity, dec!(30000))
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
            .resolve(EXCHANGE, &venue_instruments(), None)
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

        let (snapshot, _) = start.resolve(EXCHANGE, &venue_instruments(), None).unwrap();

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

        let (snapshot, _) = SimStart::default()
            .resolve(EXCHANGE, &instruments, None)
            .unwrap();

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

        let (snapshot, _) = start.resolve(EXCHANGE, &instruments, None).unwrap();

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

        let (snapshot, _) = start.resolve(EXCHANGE, &venue_instruments(), None).unwrap();

        assert_eq!(balance(&snapshot, "btc"), Some(dec!(2)));
    }

    #[test]
    fn a_start_that_cannot_hold_is_refused_with_the_reason() {
        let instruments = venue_instruments();
        let refused = |start: SimStart| start.resolve(EXCHANGE, &instruments, None).unwrap_err();
        let btc = || seed("binance_spot-btc_usdt", Side::Buy, dec!(2), dec!(30000));

        assert_eq!(
            refused(SimStart::new(
                vec![StartBalance::new(AssetNameExchange::new("btc"), dec!(5))],
                vec![btc()],
            )),
            SimStartError::BaseBalanceConflict {
                asset: AssetNameExchange::new("btc"),
                listed: dec!(5),
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
                .resolve(EXCHANGE, &instruments, None)
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
                vec![usdt(dec!(0)).with_loan(dec!(1), dec!(0))],
                vec![]
            )),
            SimStartError::LoanWithoutMargin {
                asset: AssetNameExchange::new("usdt"),
                exchange: EXCHANGE,
            }
        );
        // Interest alone is a loan too.
        assert_eq!(
            refused(SimStart::new(
                vec![usdt(dec!(0)).with_loan(dec!(0), dec!(1))],
                vec![]
            )),
            SimStartError::LoanWithoutMargin {
                asset: AssetNameExchange::new("usdt"),
                exchange: EXCHANGE,
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

    fn asset(name: &str) -> AssetNameExchange {
        AssetNameExchange::new(name)
    }

    fn over_leveraged(
        debt: Decimal,
        equity: Decimal,
        max_leverage: Decimal,
        unvalued: &[&str],
    ) -> SimStartError {
        SimStartError::OverLeveraged {
            valuation_asset: asset("usdt"),
            debt,
            equity,
            max_leverage,
            unvalued: unvalued.iter().map(|name| asset(name)).collect(),
        }
    }

    #[test]
    fn a_spot_short_on_a_venue_that_lends_owes_its_quantity_and_holds_none_of_it() {
        // Equity is 40000 less the 30000 owed, and leverage 4 lends 3 × 10000: exactly the debt.
        let start = SimStart::new(vec![usdt(dec!(40000))], vec![short_btc(dec!(1))]);

        let (snapshot, seeds) = start
            .clone()
            .resolve(EXCHANGE, &venue_instruments(), Some(&lends(dec!(4))))
            .unwrap();

        assert_eq!(balance(&snapshot, "btc"), Some(Decimal::ZERO));
        assert_eq!(
            loan(&snapshot, "btc"),
            Some(MarginDetails::new(dec!(1), dec!(0)))
        );
        assert_eq!(
            balance(&snapshot, "usdt"),
            Some(dec!(40000)),
            "the proceeds are listed, not credited"
        );
        assert_eq!(
            loan(&snapshot, "eth"),
            Some(MarginDetails::default()),
            "a margin account reports every balance's loan"
        );
        assert_eq!(seeds, start.positions);
    }

    #[test]
    fn spot_positions_on_one_base_asset_net_to_one_holding_or_one_loan() {
        let instruments = with_btc_eth();
        let resolved = |positions| {
            SimStart::new(vec![usdt(dec!(100000))], positions)
                .resolve(EXCHANGE, &instruments, Some(&lends(dec!(3))))
                .unwrap()
                .0
        };
        let btc_eth = |quantity| seed("binance_spot-btc_eth", Side::Buy, quantity, dec!(15));

        let net_short = resolved(vec![short_btc(dec!(2)), btc_eth(dec!(1))]);
        assert_eq!(balance(&net_short, "btc"), Some(Decimal::ZERO));
        assert_eq!(
            loan(&net_short, "btc"),
            Some(MarginDetails::new(dec!(1), dec!(0)))
        );

        let net_long = resolved(vec![short_btc(dec!(1)), btc_eth(dec!(3))]);
        assert_eq!(balance(&net_long, "btc"), Some(dec!(2)));
        assert_eq!(loan(&net_long, "btc"), Some(MarginDetails::default()));
    }

    #[test]
    fn a_listed_base_asset_must_hold_its_positions_net_quantity_net_of_its_principal() {
        let margin = lends(dec!(4));
        let resolve = |btc: StartBalance| {
            SimStart::new(vec![usdt(dec!(100000)), btc], vec![short_btc(dec!(1))]).resolve(
                EXCHANGE,
                &venue_instruments(),
                Some(&margin),
            )
        };

        // Gross: 2 borrowed and 1 of it still held, with interest owed, which the position does
        // not include.
        let (snapshot, _) =
            resolve(StartBalance::new(asset("btc"), dec!(1)).with_loan(dec!(2), dec!(0.001)))
                .unwrap();
        assert_eq!(balance(&snapshot, "btc"), Some(dec!(1)));
        assert_eq!(
            loan(&snapshot, "btc"),
            Some(MarginDetails::new(dec!(2), dec!(0.001)))
        );

        assert_eq!(
            resolve(StartBalance::new(asset("btc"), dec!(0))).unwrap_err(),
            SimStartError::BaseBalanceConflict {
                asset: asset("btc"),
                listed: dec!(0),
                positions: dec!(-1),
            }
        );
        // 1 held and 1 owed is flat, not the short.
        assert_eq!(
            resolve(StartBalance::new(asset("btc"), dec!(1)).with_loan(dec!(1), dec!(0)))
                .unwrap_err(),
            SimStartError::BaseBalanceConflict {
                asset: asset("btc"),
                listed: dec!(0),
                positions: dec!(-1),
            }
        );
    }

    #[test]
    fn a_listed_loan_opens_owing_it_so_a_start_can_hold_a_long_bought_on_margin() {
        // 30000 of BTC, bought with 10000 of equity and 20000 borrowed.
        let start = |max_leverage| {
            SimStart::new(
                vec![usdt(dec!(0)).with_loan(dec!(20000), dec!(0))],
                vec![seed(
                    "binance_spot-btc_usdt",
                    Side::Buy,
                    dec!(1),
                    dec!(30000),
                )],
            )
            .resolve(EXCHANGE, &venue_instruments(), Some(&lends(max_leverage)))
        };

        let (snapshot, _) = start(dec!(3)).unwrap();
        assert_eq!(
            loan(&snapshot, "usdt"),
            Some(MarginDetails::new(dec!(20000), dec!(0)))
        );
        assert_eq!(balance(&snapshot, "btc"), Some(dec!(1)));

        assert_eq!(
            start(dec!(2)).unwrap_err(),
            over_leveraged(dec!(20000), dec!(10000), dec!(2), &[])
        );
    }

    #[test]
    fn a_start_owing_more_than_the_venue_lends_is_refused_with_how_it_was_valued() {
        let refused = |balances, max_leverage| {
            SimStart::new(balances, vec![short_btc(dec!(1))])
                .resolve(EXCHANGE, &venue_instruments(), Some(&lends(max_leverage)))
                .unwrap_err()
        };

        // Leverage 3 lends 2 × 10000, short of the 30000 owed. ETH, traded but not held, is not
        // named.
        assert_eq!(
            refused(vec![usdt(dec!(40000))], dec!(3)),
            over_leveraged(dec!(30000), dec!(10000), dec!(3), &[])
        );

        // Interest owed is debt, and comes out of equity.
        assert_eq!(
            refused(
                vec![
                    usdt(dec!(40000)),
                    StartBalance::new(asset("btc"), dec!(0)).with_loan(dec!(1), dec!(0.1)),
                ],
                dec!(4),
            ),
            over_leveraged(dec!(33000), dec!(7000), dec!(4), &[])
        );
    }

    #[test]
    fn an_asset_the_start_gives_no_price_for_adds_nothing_to_equity_and_is_named() {
        let resolve = |quote| {
            SimStart::new(
                vec![usdt(quote), StartBalance::new(asset("eth"), dec!(10))],
                vec![short_btc(dec!(1))],
            )
            .resolve(EXCHANGE, &venue_instruments(), Some(&lends(dec!(4))))
        };

        // At the limit without the ETH, so holding it refuses nothing.
        assert!(resolve(dec!(40000)).is_ok());
        assert_eq!(
            resolve(dec!(39000)).unwrap_err(),
            over_leveraged(dec!(30000), dec!(9000), dec!(4), &["eth"])
        );
    }

    #[test]
    fn a_debt_the_start_gives_no_price_for_is_refused() {
        let instruments = with_btc_eth();
        let refused = |balances, positions| {
            SimStart::new(balances, positions)
                .resolve(EXCHANGE, &instruments, Some(&lends(dec!(3))))
                .unwrap_err()
        };
        let unpriced = |name| SimStartError::UnpricedDebt {
            asset: asset(name),
            valuation_asset: asset("usdt"),
        };

        // Sold for ETH, so priced in ETH, and ETH itself is priced in nothing.
        assert_eq!(
            refused(
                vec![StartBalance::new(asset("eth"), dec!(15))],
                vec![seed("binance_spot-btc_eth", Side::Sell, dec!(1), dec!(15))],
            ),
            unpriced("btc")
        );
        // Interest alone is a debt. BTC quoted in ETH prices ETH in BTC, not in USDT.
        assert_eq!(
            refused(
                vec![
                    usdt(dec!(1000)),
                    StartBalance::new(asset("eth"), dec!(0)).with_loan(dec!(0), dec!(1)),
                ],
                vec![seed("binance_spot-btc_eth", Side::Buy, dec!(1), dec!(15))],
            ),
            unpriced("eth")
        );
    }

    #[test]
    fn a_pair_the_other_way_round_prices_an_asset_at_the_inverse_of_its_entry() {
        // A USDT bought at 0.8 EUR, so a EUR is worth 1.25 USDT.
        let instruments = generate_mock_exchange_instruments(
            &IndexedInstruments::new([instrument(EXCHANGE, "usdt", "eur")]),
            EXCHANGE,
        );
        let start = SimStart::new(
            vec![StartBalance::new(asset("eur"), dec!(200)).with_loan(dec!(100), dec!(0))],
            vec![seed(
                "binance_spot-usdt_eur",
                Side::Buy,
                dec!(100),
                dec!(0.8),
            )],
        );

        // Equity: 100 USDT, and 100 EUR net at 1.25. Debt: 100 EUR at 1.25.
        assert_eq!(
            start
                .resolve(EXCHANGE, &instruments, Some(&lends(dec!(1.5))))
                .unwrap_err(),
            over_leveraged(dec!(125), dec!(225), dec!(1.5), &[])
        );
    }

    #[test]
    fn of_two_pairs_pricing_an_asset_the_first_by_the_venue_s_name_prices_it() {
        // BTC at 30000 by btc_usdt, but at 25000 by usdt_btc's 0.00004.
        let mut instruments = venue_instruments();
        instruments.extend(generate_mock_exchange_instruments(
            &IndexedInstruments::new([instrument(EXCHANGE, "usdt", "btc")]),
            EXCHANGE,
        ));
        let start = SimStart::new(
            vec![StartBalance::new(asset("btc"), dec!(1.5)).with_loan(dec!(0.5), dec!(0))],
            vec![
                seed("binance_spot-btc_usdt", Side::Buy, dec!(1), dec!(30000)),
                seed(
                    "binance_spot-usdt_btc",
                    Side::Buy,
                    dec!(1000),
                    dec!(0.00004),
                ),
            ],
        );

        // Equity: 1000 USDT, and 1 BTC net at 30000. Debt: 0.5 BTC at 30000.
        assert_eq!(
            start
                .resolve(EXCHANGE, &instruments, Some(&lends(dec!(1.4))))
                .unwrap_err(),
            over_leveraged(dec!(15000), dec!(31000), dec!(1.4), &[])
        );
    }

    #[test]
    fn an_entry_price_of_zero_prices_nothing() {
        let start = SimStart::new(
            vec![usdt(dec!(40000))],
            vec![seed("binance_spot-btc_usdt", Side::Sell, dec!(1), dec!(0))],
        );

        assert_eq!(
            start
                .resolve(EXCHANGE, &venue_instruments(), Some(&lends(dec!(3))))
                .unwrap_err(),
            SimStartError::UnpricedDebt {
                asset: asset("btc"),
                valuation_asset: asset("usdt"),
            }
        );
    }

    #[test]
    fn a_negative_loan_is_refused() {
        let refused = |borrowed, interest| {
            SimStart::new(vec![usdt(dec!(0)).with_loan(borrowed, interest)], vec![])
                .resolve(EXCHANGE, &venue_instruments(), Some(&lends(dec!(3))))
                .unwrap_err()
        };
        let negative = |borrowed, interest| SimStartError::NegativeLoan {
            asset: asset("usdt"),
            borrowed,
            interest,
        };

        assert_eq!(refused(dec!(-1), dec!(0)), negative(dec!(-1), dec!(0)));
        assert_eq!(refused(dec!(0), dec!(-1)), negative(dec!(0), dec!(-1)));
    }

    #[test]
    fn a_start_balance_owing_nothing_is_written_without_its_loan() {
        let plain = usdt(dec!(5));
        let json = serde_json::to_string(&plain).unwrap();
        assert!(!json.contains("loan"), "{json}");
        assert_eq!(serde_json::from_str::<StartBalance>(&json).unwrap(), plain);

        let owing = usdt(dec!(5)).with_loan(dec!(1), dec!(0.5));
        let json = serde_json::to_string(&owing).unwrap();
        assert_eq!(serde_json::from_str::<StartBalance>(&json).unwrap(), owing);
    }
}
