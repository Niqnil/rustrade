use crate::engine::state::instrument::{InstrumentStates, data::InstrumentDataState};
use rust_decimal::Decimal;
use rustrade_execution::trade::{Trade, TradeId};
use rustrade_instrument::{
    asset::AssetIndex,
    instrument::{InstrumentIndex, kind::InstrumentKind},
};
use serde::{Deserialize, Serialize};
use tracing::{debug, info};

/// A fill's fee charged in a third asset, neither the instrument's base nor its quote, and how
/// the engine valued it in quote.
///
/// BNB on a `BTCUSDT` fill, with Binance's "pay fees with BNB" on, is the usual case. The venue
/// reports the fee in BNB, and the execution indexer cannot give it a quote equivalent
/// ([`AssetFees::fees_quote`](rustrade_execution::trade::AssetFees::fees_quote) is `None`). The
/// engine values it with a price it holds, see
/// [`EngineState::update_from_account`](super::EngineState::update_from_account).
///
/// Carried by [`EngineOutput::FeeConversion`](crate::engine::EngineOutput::FeeConversion), one per
/// such fill, whether it was converted or not.
#[non_exhaustive]
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize)]
pub struct FeeConversion<AssetKey = AssetIndex, InstrumentKey = InstrumentIndex> {
    /// The instrument the fill is on.
    pub instrument: InstrumentKey,
    /// The fill, as the venue identified it.
    pub trade_id: TradeId,
    /// The asset the fee was charged in.
    pub asset: AssetKey,
    /// The fee, in [`asset`](Self::asset), as the venue charged it.
    pub fees: Decimal,
    /// Whether the fee was valued in quote, and with what.
    pub outcome: FeeConversionOutcome<InstrumentKey>,
}

/// Whether a [`FeeConversion`] valued its fee in quote.
#[non_exhaustive]
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize)]
pub enum FeeConversionOutcome<InstrumentKey = InstrumentIndex> {
    /// Valued: the fill's fee counts in PnL at `fees_quote`.
    Converted {
        /// The spot instrument whose price valued the fee.
        pricing_instrument: InstrumentKey,
        /// What one unit of the fee asset was worth in the fill's quote asset: the pricing
        /// instrument's price, or its reciprocal when that instrument quotes the fee asset
        /// (`USDT/BNB` for a BNB fee on a `BTCUSDT` fill).
        price: Decimal,
        /// The fee's quote equivalent, `fees × price`.
        fees_quote: Decimal,
    },
    /// Not valued: the engine has no spot instrument pairing the fee asset with the fill's quote
    /// asset on the fill's exchange. The fee is left out of PnL.
    NoPricingInstrument,
    /// Not valued: the pricing instrument had no price yet, a price that is not positive, or one
    /// at which the fee's quote value overflows a `Decimal`. The fee is left out of PnL.
    NoPrice {
        /// The spot instrument that would have valued the fee.
        pricing_instrument: InstrumentKey,
    },
}

impl FeeConversion {
    /// Logs the conversion: at DEBUG when the fee was valued, at INFO when it was left out of PnL.
    /// The output and the tear sheet's count are the signal a caller acts on, so a run that pays
    /// every fee in an asset it cannot price logs no WARN per fill.
    pub(crate) fn log(&self) {
        match &self.outcome {
            FeeConversionOutcome::Converted {
                pricing_instrument,
                price,
                fees_quote,
            } => debug!(
                instrument = %self.instrument,
                trade_id = %self.trade_id,
                fee_asset = %self.asset,
                fee = %self.fees,
                %pricing_instrument,
                %price,
                %fees_quote,
                "valued a fill's third-asset fee in quote"
            ),
            unconverted => info!(
                instrument = %self.instrument,
                trade_id = %self.trade_id,
                fee_asset = %self.asset,
                fee = %self.fees,
                outcome = ?unconverted,
                "could not value a fill's third-asset fee in quote; it is left out of PnL"
            ),
        }
    }
}

impl<InstrumentKey> FeeConversionOutcome<InstrumentKey> {
    /// The fee's quote equivalent, `None` unless [`Converted`](Self::Converted).
    pub fn fees_quote(&self) -> Option<Decimal> {
        match self {
            Self::Converted { fees_quote, .. } => Some(*fees_quote),
            Self::NoPricingInstrument | Self::NoPrice { .. } => None,
        }
    }
}

/// What valuing one fill's third-asset fee gave: the quote equivalent to apply the fill with, and
/// the record to report, if any.
pub(crate) struct ValuedFee {
    pub(crate) fees_quote: Option<Decimal>,
    pub(crate) conversion: Option<FeeConversion>,
}

impl<InstrumentData> InstrumentStates<InstrumentData>
where
    InstrumentData: InstrumentDataState,
{
    /// Values `trade`'s fee in its quote asset when it was charged in a third asset and has no
    /// quote equivalent yet. `None` when there is nothing to value: a fee in base or quote, or one
    /// already given a quote equivalent.
    ///
    /// A zero fee is worth zero whatever the price, so it is valued without one and not reported.
    pub(crate) fn value_third_asset_fee(
        &self,
        trade: &Trade<AssetIndex, InstrumentIndex>,
    ) -> Option<ValuedFee> {
        let fee = &trade.fees;
        let underlying = &self
            .instrument_index(&trade.instrument)
            .instrument
            .underlying;
        if fee.fees_quote.is_some() || fee.asset == underlying.base || fee.asset == underlying.quote
        {
            return None;
        }

        if fee.fees.is_zero() {
            return Some(ValuedFee {
                fees_quote: Some(Decimal::ZERO),
                conversion: None,
            });
        }

        let outcome = match self.fee_asset_pricing(fee.asset, underlying.quote) {
            None => FeeConversionOutcome::NoPricingInstrument,
            Some((pricing_instrument, price)) => {
                match price.and_then(|price| Some((price, fee.fees.checked_mul(price)?))) {
                    Some((price, fees_quote)) => FeeConversionOutcome::Converted {
                        pricing_instrument,
                        price,
                        fees_quote,
                    },
                    None => FeeConversionOutcome::NoPrice { pricing_instrument },
                }
            }
        };

        Some(ValuedFee {
            fees_quote: outcome.fees_quote(),
            conversion: Some(FeeConversion {
                instrument: trade.instrument,
                trade_id: trade.id.clone(),
                asset: fee.asset,
                fees: fee.fees,
                outcome,
            }),
        })
    }

    /// The spot instrument that prices `asset` in `quote`, and what one unit of `asset` is worth
    /// in `quote` by it, if it has a positive price: an instrument of `asset` in `quote`, or the
    /// reciprocal of one of `quote` in `asset`. Of several, the lowest [`InstrumentIndex`], so the
    /// choice is the same on every run.
    ///
    /// Assets are indexed per exchange, so only an instrument on the fill's own exchange matches.
    fn fee_asset_pricing(
        &self,
        asset: AssetIndex,
        quote: AssetIndex,
    ) -> Option<(InstrumentIndex, Option<Decimal>)> {
        self.0
            .values()
            .filter_map(|state| {
                let instrument = &state.instrument;
                if !matches!(instrument.kind, InstrumentKind::Spot) {
                    return None;
                }
                let pair = (instrument.underlying.base, instrument.underlying.quote);
                let inverted = if pair == (asset, quote) {
                    false
                } else if pair == (quote, asset) {
                    true
                } else {
                    return None;
                };
                let price = state
                    .data
                    .price()
                    .filter(|price| *price > Decimal::ZERO)
                    .and_then(|price| match inverted {
                        true => Decimal::ONE.checked_div(price),
                        false => Some(price),
                    });
                Some((state.key, price))
            })
            .min_by_key(|(key, _)| *key)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use crate::{
        Timed,
        engine::state::{
            EngineState, global::DefaultGlobalData, instrument::data::DefaultInstrumentMarketData,
        },
    };
    use chrono::{DateTime, Utc};
    use rust_decimal_macros::dec;
    use rustrade_execution::{
        AccountEvent, AccountEventKind,
        order::id::{OrderId, StrategyId},
        trade::AssetFees,
    };
    use rustrade_instrument::{
        Side, Underlying,
        asset::{Asset, ExchangeAsset, name::AssetNameInternal},
        exchange::{ExchangeId, ExchangeIndex},
        index::IndexedInstruments,
        instrument::{
            Instrument,
            kind::{InstrumentKind, perpetual::PerpetualContract},
            name::{InstrumentNameExchange, InstrumentNameInternal},
            quote::InstrumentQuoteAsset,
        },
        test_utils::instrument,
    };

    const EXCHANGE: ExchangeId = ExchangeId::BinanceSpot;
    const OTHER: ExchangeId = ExchangeId::Coinbase;

    type State = EngineState<DefaultGlobalData, DefaultInstrumentMarketData>;

    struct Setup {
        instruments: IndexedInstruments,
        state: State,
    }

    impl Setup {
        fn new(instruments: IndexedInstruments) -> Self {
            let state = EngineState::builder(&instruments, DefaultGlobalData, |_| {
                DefaultInstrumentMarketData::default()
            })
            .time_engine_start(time(0))
            .build();
            Self { instruments, state }
        }

        fn index(&self, exchange: ExchangeId, name: &str) -> InstrumentIndex {
            self.instruments
                .find_instrument_index(
                    exchange,
                    &InstrumentNameInternal::new_from_exchange(exchange, name),
                )
                .unwrap()
        }

        fn asset(&self, name: &str) -> AssetIndex {
            self.instruments
                .find_asset_index(EXCHANGE, &AssetNameInternal::from(name))
                .unwrap()
        }

        fn set_price(&mut self, exchange: ExchangeId, name: &str, price: Decimal) {
            let index = self.index(exchange, name);
            self.state
                .instruments
                .instrument_index_mut(&index)
                .data
                .last_traded_price = Some(Timed::new(price, time(0)));
        }

        /// A fill of 1 BTC on `btc_usdt` at `price`, paying `fee` in `fee_asset`.
        fn fill(
            &mut self,
            id: &str,
            side: Side,
            price: Decimal,
            fee_asset: &str,
            fee: Decimal,
        ) -> crate::engine::state::instrument::FillsApplied<AssetIndex> {
            let trade = Trade::new(
                TradeId::new(id),
                OrderId::new(id),
                self.index(EXCHANGE, "btc_usdt"),
                StrategyId::new("strategy"),
                time(1),
                side,
                price,
                dec!(1),
                None,
                AssetFees::new(self.asset(fee_asset), fee, None),
            );
            self.state.update_from_account(&AccountEvent::new(
                ExchangeIndex::new(0),
                AccountEventKind::Trade(trade),
            ))
        }

        fn btc_usdt(
            &self,
        ) -> &crate::engine::state::instrument::InstrumentState<DefaultInstrumentMarketData>
        {
            self.state
                .instruments
                .instrument_index(&self.index(EXCHANGE, "btc_usdt"))
        }
    }

    fn time(secs: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(1_700_000_000 + secs, 0).unwrap()
    }

    fn bnb() -> ExchangeAsset<Asset> {
        ExchangeAsset::new(EXCHANGE, Asset::new_from_exchange("bnb"))
    }

    fn perpetual(exchange: ExchangeId, base: &str, quote: &str) -> Instrument<ExchangeId, Asset> {
        let spot = instrument(exchange, base, quote);
        Instrument::new(
            exchange,
            InstrumentNameInternal::new_from_exchange(exchange, format!("{base}_{quote}_perp")),
            InstrumentNameExchange::from(format!("{base}_{quote}_perp")),
            Underlying::new(spot.underlying.base.clone(), spot.underlying.quote.clone()),
            InstrumentQuoteAsset::UnderlyingQuote,
            InstrumentKind::Perpetual(PerpetualContract {
                contract_size: Decimal::ONE,
                settlement_asset: spot.underlying.quote,
            }),
            None,
        )
    }

    /// A BNB fee on `BTCUSDT` is valued at `BNBUSDT`'s price, counts in PnL, and is reported.
    #[test]
    fn a_third_asset_fee_is_valued_at_its_pricing_instruments_price() {
        let mut setup = Setup::new(IndexedInstruments::new([
            instrument(EXCHANGE, "btc", "usdt"),
            instrument(EXCHANGE, "bnb", "usdt"),
        ]));
        setup.set_price(EXCHANGE, "bnb_usdt", dec!(600));

        let applied = setup.fill("enter", Side::Buy, dec!(50000), "bnb", dec!(0.01));

        let conversion = applied.fee_conversion.unwrap();
        assert_eq!(
            conversion,
            FeeConversion {
                instrument: setup.index(EXCHANGE, "btc_usdt"),
                trade_id: TradeId::new("enter"),
                asset: setup.asset("bnb"),
                fees: dec!(0.01),
                outcome: FeeConversionOutcome::Converted {
                    pricing_instrument: setup.index(EXCHANGE, "bnb_usdt"),
                    price: dec!(600),
                    fees_quote: dec!(6),
                },
            }
        );

        // The exit's fee is valued the same way, so realised PnL is net of both: 1000 - 6 - 6.
        let exited = setup
            .fill("exit", Side::Sell, dec!(51000), "bnb", dec!(0.01))
            .exited
            .unwrap();
        assert_eq!(exited.pnl_realised, dec!(988));
        assert_eq!(exited.fees_enter.fees_quote, Some(dec!(6)));
        assert_eq!(exited.fees_enter.fees, dec!(0.01));
        assert_eq!(setup.btc_usdt().tear_sheet.fills_fee_unconverted, 0);
    }

    /// An instrument quoting the fee asset values it at the reciprocal of its price.
    #[test]
    fn an_instrument_quoting_the_fee_asset_values_it_at_the_reciprocal() {
        let mut setup = Setup::new(IndexedInstruments::new([
            instrument(EXCHANGE, "btc", "usdt"),
            instrument(EXCHANGE, "usdt", "bnb"),
        ]));
        setup.set_price(EXCHANGE, "usdt_bnb", dec!(0.002));

        let applied = setup.fill("enter", Side::Buy, dec!(50000), "bnb", dec!(0.01));

        assert_eq!(
            applied.fee_conversion.unwrap().outcome,
            FeeConversionOutcome::Converted {
                pricing_instrument: setup.index(EXCHANGE, "usdt_bnb"),
                price: dec!(500),
                fees_quote: dec!(5),
            }
        );
    }

    /// With no spot instrument pairing the fee asset with the quote on the fill's exchange, the
    /// fee is left out of PnL, reported, and counted. A perpetual, and a pair on another exchange,
    /// do not price it.
    #[test]
    fn without_a_pricing_instrument_the_fee_is_left_out_and_counted() {
        let mut setup = Setup::new(
            IndexedInstruments::builder()
                .add_instrument(instrument(EXCHANGE, "btc", "usdt"))
                .add_instrument(perpetual(EXCHANGE, "bnb", "usdt"))
                .add_instrument(instrument(OTHER, "bnb", "usdt"))
                .add_asset(bnb())
                .build(),
        );
        setup.set_price(EXCHANGE, "bnb_usdt_perp", dec!(600));
        setup.set_price(OTHER, "bnb_usdt", dec!(600));

        let applied = setup.fill("enter", Side::Buy, dec!(50000), "bnb", dec!(0.01));
        assert_eq!(
            applied.fee_conversion.unwrap().outcome,
            FeeConversionOutcome::NoPricingInstrument
        );

        let exited = setup
            .fill("exit", Side::Sell, dec!(51000), "bnb", dec!(0.01))
            .exited
            .unwrap();
        assert_eq!(exited.pnl_realised, dec!(1000));
        assert_eq!(exited.fees_enter.fees_quote, None);
        assert_eq!(setup.btc_usdt().tear_sheet.fills_fee_unconverted, 2);
    }

    /// A pricing instrument with no positive price leaves the fee out, naming the instrument.
    #[test]
    fn a_pricing_instrument_with_no_positive_price_leaves_the_fee_out() {
        for price in [None, Some(Decimal::ZERO), Some(dec!(-1))] {
            let mut setup = Setup::new(IndexedInstruments::new([
                instrument(EXCHANGE, "btc", "usdt"),
                instrument(EXCHANGE, "bnb", "usdt"),
            ]));
            if let Some(price) = price {
                setup.set_price(EXCHANGE, "bnb_usdt", price);
            }

            let applied = setup.fill("enter", Side::Buy, dec!(50000), "bnb", dec!(0.01));

            assert_eq!(
                applied.fee_conversion.unwrap().outcome,
                FeeConversionOutcome::NoPrice {
                    pricing_instrument: setup.index(EXCHANGE, "bnb_usdt"),
                },
                "price {price:?}"
            );
            assert_eq!(setup.btc_usdt().tear_sheet.fills_fee_unconverted, 1);
        }
    }

    /// A fee whose quote value overflows a `Decimal` is left out, as one with no price.
    #[test]
    fn a_fee_whose_quote_value_overflows_is_left_out() {
        let mut setup = Setup::new(IndexedInstruments::new([
            instrument(EXCHANGE, "btc", "usdt"),
            instrument(EXCHANGE, "bnb", "usdt"),
        ]));
        setup.set_price(EXCHANGE, "bnb_usdt", dec!(2));

        let applied = setup.fill("enter", Side::Buy, dec!(50000), "bnb", Decimal::MAX);

        assert_eq!(
            applied.fee_conversion.unwrap().outcome,
            FeeConversionOutcome::NoPrice {
                pricing_instrument: setup.index(EXCHANGE, "bnb_usdt"),
            }
        );
    }

    /// Of two instruments that could price the fee, the lowest index does, whatever its price.
    #[test]
    fn of_two_pricing_instruments_the_lowest_index_prices_the_fee() {
        let mut setup = Setup::new(IndexedInstruments::new([
            instrument(EXCHANGE, "btc", "usdt"),
            instrument(EXCHANGE, "bnb", "usdt"),
            instrument(EXCHANGE, "usdt", "bnb"),
        ]));
        setup.set_price(EXCHANGE, "bnb_usdt", dec!(600));
        setup.set_price(EXCHANGE, "usdt_bnb", dec!(0.002));
        let direct = setup.index(EXCHANGE, "bnb_usdt");
        let inverse = setup.index(EXCHANGE, "usdt_bnb");
        let (first, price) = match direct < inverse {
            true => (direct, dec!(600)),
            false => (inverse, dec!(500)),
        };

        let applied = setup.fill("enter", Side::Buy, dec!(50000), "bnb", dec!(0.01));

        assert_eq!(
            applied.fee_conversion.unwrap().outcome,
            FeeConversionOutcome::Converted {
                pricing_instrument: first,
                price,
                fees_quote: price * dec!(0.01),
            }
        );
    }

    /// A zero fee is worth zero without a price, and is not reported.
    #[test]
    fn a_zero_third_asset_fee_is_valued_at_zero_without_a_report() {
        let mut setup = Setup::new(
            IndexedInstruments::builder()
                .add_instrument(instrument(EXCHANGE, "btc", "usdt"))
                .add_asset(bnb())
                .build(),
        );

        let applied = setup.fill("enter", Side::Buy, dec!(50000), "bnb", Decimal::ZERO);

        assert_eq!(applied.fee_conversion, None);
        let position = setup.btc_usdt().position.positions.values().next().unwrap();
        assert_eq!(position.fees_enter.fees_quote, Some(Decimal::ZERO));
        assert_eq!(setup.btc_usdt().tear_sheet.fills_fee_unconverted, 0);
    }

    /// A fee in quote, or one already given a quote equivalent, is applied as it is, unreported.
    #[test]
    fn a_fee_in_quote_or_already_valued_is_not_converted() {
        let mut setup = Setup::new(
            IndexedInstruments::builder()
                .add_instrument(instrument(EXCHANGE, "btc", "usdt"))
                .add_instrument(instrument(EXCHANGE, "bnb", "usdt"))
                .build(),
        );
        setup.set_price(EXCHANGE, "bnb_usdt", dec!(600));
        let trade = |setup: &Setup, asset: &str, fees_quote| {
            Trade::new(
                TradeId::new("enter"),
                OrderId::new("enter"),
                setup.index(EXCHANGE, "btc_usdt"),
                StrategyId::new("strategy"),
                time(1),
                Side::Buy,
                dec!(50000),
                dec!(1),
                None,
                AssetFees::new(setup.asset(asset), dec!(0.01), fees_quote),
            )
        };

        for (asset, fees_quote) in [("usdt", Some(dec!(0.01))), ("bnb", Some(dec!(7)))] {
            assert!(
                setup
                    .state
                    .instruments
                    .value_third_asset_fee(&trade(&setup, asset, fees_quote))
                    .is_none(),
                "{asset}"
            );
        }
        // A base fee has no quote equivalent from a hand-built trade, but is still not a third
        // asset's.
        assert!(
            setup
                .state
                .instruments
                .value_third_asset_fee(&trade(&setup, "btc", None))
                .is_none()
        );
    }
}
