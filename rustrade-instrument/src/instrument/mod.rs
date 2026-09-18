use crate::{
    Underlying,
    asset::Asset,
    instrument::{
        data_venue::DataVenue,
        kind::{
            InstrumentKind, cfd::CfdContract, future::FutureContract, option::OptionContract,
            perpetual::PerpetualContract,
        },
        market_data::{MarketDataInstrument, kind::MarketDataInstrumentKind},
        name::{InstrumentNameExchange, InstrumentNameInternal},
        quote::InstrumentQuoteAsset,
        spec::{InstrumentSpec, InstrumentSpecQuantity, OrderQuantityUnits},
    },
};
use derive_more::{Constructor, Display};
use serde::{Deserialize, Serialize};
use std::fmt::Formatter;

/// Defines an [`Instrument`]s [`InstrumentKind`] (eg/ Spot, Perpetual, etc).
pub mod kind;

/// Defines the [`DataVenue`] an [`Instrument`]s market data is sourced from, when that differs
/// from the venue it is executed on.
pub mod data_venue;

/// Defines the [`InstrumentNameExchange`] and [`InstrumentNameExchange`] types, used as
/// `SmolStr` identifiers for an [`Instrument`].
pub mod name;

/// Defines the [`InstrumentSpec`], including specifications for an [`Instrument`]s
/// price, quantity and notional value.
///
/// eg/ `InstrumentSpecPrice.tick_size`, `OrderQuantityUnits`, etc.
pub mod spec;

/// Defines a simplified [`MarketDataInstrument`], with only the necessary data to subscribe to
/// market data feeds.
pub mod market_data;

/// Defines the [`InstrumentQuoteAsset`] (underlying base or quote) for an [`Instrument`].
pub mod quote;

/// Unique identifier for an `Instrument` traded on an execution.
///
/// Used to key data events in a memory efficient way.
#[derive(
    Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize, Display,
)]
pub struct InstrumentId(pub u64);

#[derive(
    Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize, Constructor,
)]
pub struct InstrumentIndex(pub usize);

impl InstrumentIndex {
    pub fn index(&self) -> usize {
        self.0
    }
}

impl std::fmt::Display for InstrumentIndex {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "InstrumentIndex({})", self.0)
    }
}

/// Comprehensive Instrument model, containing all the data required to subscribe to market data
/// and generate correct orders.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize)]
pub struct Instrument<ExchangeKey, AssetKey> {
    pub exchange: ExchangeKey,
    pub name_internal: InstrumentNameInternal,
    pub name_exchange: InstrumentNameExchange,
    pub underlying: Underlying<AssetKey>,
    pub quote: InstrumentQuoteAsset,
    #[serde(alias = "instrument_kind")]
    pub kind: InstrumentKind<AssetKey>,
    pub spec: Option<InstrumentSpec<AssetKey>>,

    /// Venue this `Instrument`s **market data** is sourced from, when that differs from
    /// `exchange` — see [`DataVenue`], and read it via [`Self::data_exchange`] /
    /// [`Self::data_name_exchange`] so the fallback to `exchange`/`name_exchange` is applied in
    /// one place.
    ///
    /// Declared **last** by convention, not by necessity — worth stating precisely so a future
    /// maintainer does not infer a tighter constraint than exists. `Instrument` derives `Ord` and
    /// `IndexedInstrumentsBuilder` sorts the collection before assigning each `InstrumentIndex`, so
    /// field order is in principle part of how indices are numbered; this field is nonetheless
    /// unreachable as a sort key in both of the cases that matter. A collection whose instruments
    /// all carry `None` — every collection built before this field existed — compares `Equal` here
    /// wherever the field sits, so adding it renumbers nothing regardless of position. And
    /// `IndexedInstrumentsBuilder::try_build` rejects a duplicate `name_internal`, so in any
    /// collection that builds at all, `Ord` between two distinct instruments is already decided by
    /// an earlier field and never reaches this one to break a tie. Last is simply the least
    /// surprising place for it.
    ///
    /// `default = "Option::default"` rather than a bare `default`: the bare form makes serde infer
    /// an `ExchangeKey: Default` bound on the whole `Deserialize` impl, which `Keyed<ExchangeIndex,
    /// ExchangeId>` — the key every *indexed* `Instrument` carries — does not satisfy. Naming the
    /// path keeps the bound off.
    ///
    /// Enforced by the compiler rather than by a test: reverting to the bare form fails to *build*
    /// wherever an indexed `Instrument` is deserialised, so no runtime test below pins it.
    #[serde(default = "Option::default")]
    pub data_venue: Option<DataVenue<ExchangeKey>>,
}

impl<ExchangeKey, AssetKey> Instrument<ExchangeKey, AssetKey> {
    /// Construct a new `Instrument` with the provided data.
    ///
    /// This constructor assumes the [`InstrumentNameInternal`] can be constructed in the default
    /// style via the [`InstrumentNameInternal::new_from_exchange`] constructor.
    pub fn new<NameInternal, NameExchange>(
        exchange: ExchangeKey,
        name_internal: NameInternal,
        name_exchange: NameExchange,
        underlying: Underlying<AssetKey>,
        quote: InstrumentQuoteAsset,
        kind: InstrumentKind<AssetKey>,
        spec: Option<InstrumentSpec<AssetKey>>,
    ) -> Self
    where
        NameInternal: Into<InstrumentNameInternal>,
        NameExchange: Into<InstrumentNameExchange>,
    {
        Self {
            exchange,
            name_internal: name_internal.into(),
            name_exchange: name_exchange.into(),
            quote,
            underlying,
            kind,
            spec,
            data_venue: None,
        }
    }

    /// Construct a new `Spot` `Instrument` with the provided data.
    ///
    /// This constructor assumes the [`InstrumentNameInternal`] can be constructed in the default
    /// style via the [`InstrumentNameInternal::new_from_exchange`] constructor.
    pub fn spot<NameInternal, NameExchange>(
        exchange: ExchangeKey,
        name_internal: NameInternal,
        name_exchange: NameExchange,
        underlying: Underlying<AssetKey>,
        spec: Option<InstrumentSpec<AssetKey>>,
    ) -> Self
    where
        NameInternal: Into<InstrumentNameInternal>,
        NameExchange: Into<InstrumentNameExchange>,
    {
        Self {
            exchange,
            name_internal: name_internal.into(),
            name_exchange: name_exchange.into(),
            quote: InstrumentQuoteAsset::UnderlyingQuote,
            underlying,
            kind: InstrumentKind::Spot,
            spec,
            data_venue: None,
        }
    }

    /// Source this `Instrument`s market data from a different venue than it is executed on.
    ///
    /// See [`DataVenue`] for the fallback semantics and — importantly — the suitability obligation
    /// that comes with deciding on one venue's prices and filling on another.
    pub fn with_data_venue(mut self, data_venue: DataVenue<ExchangeKey>) -> Self {
        self.data_venue = Some(data_venue);
        self
    }

    /// Venue this `Instrument`s market data is sourced from.
    ///
    /// Falls back to `exchange` when no [`DataVenue`] is set, so this is the correct accessor for
    /// every instrument, single-venue or not.
    pub fn data_exchange(&self) -> &ExchangeKey {
        self.data_venue
            .as_ref()
            .map_or(&self.exchange, |data_venue| &data_venue.exchange)
    }

    /// Symbol the market data venue uses for this `Instrument`.
    ///
    /// Falls back to `name_exchange` when the data venue spells it the same way, or when there is
    /// no [`DataVenue`] at all.
    pub fn data_name_exchange(&self) -> &InstrumentNameExchange {
        self.data_venue
            .as_ref()
            .and_then(|data_venue| data_venue.name_exchange.as_ref())
            .unwrap_or(&self.name_exchange)
    }

    /// Map this Instruments `ExchangeKey` to a new key, using the provided lookup closure.
    ///
    /// The closure is applied to the execution `exchange` **and** to any [`DataVenue`]s exchange,
    /// which is why it takes a lookup rather than a single pre-resolved key: those are two
    /// different venues, so one key cannot serve both. A signature that accepted one key could
    /// only stamp the execution venue's key onto the data venue — silently pointing market-data
    /// state at the wrong exchange.
    pub fn map_exchange_key<FnFindExchange, NewExchangeKey>(
        self,
        find_exchange: FnFindExchange,
    ) -> Instrument<NewExchangeKey, AssetKey>
    where
        FnFindExchange: Fn(&ExchangeKey) -> NewExchangeKey,
    {
        let Instrument {
            exchange,
            name_internal,
            name_exchange,
            underlying,
            quote,
            kind,
            spec,
            data_venue,
        } = self;

        let data_venue =
            data_venue.map(|data_venue| data_venue.map_exchange_key(|key| find_exchange(key)));

        Instrument {
            exchange: find_exchange(&exchange),
            name_internal,
            name_exchange,
            underlying,
            quote,
            kind,
            spec,
            data_venue,
        }
    }

    /// Map this Instruments `AssetKey` to a new key, using the provided lookup closure.
    pub fn map_asset_key_with_lookup<FnFindAsset, NewAssetKey, Error>(
        self,
        find_asset: FnFindAsset,
    ) -> Result<Instrument<ExchangeKey, NewAssetKey>, Error>
    where
        FnFindAsset: Fn(&AssetKey) -> Result<NewAssetKey, Error>,
    {
        let Instrument {
            exchange,
            name_internal,
            name_exchange,
            underlying,
            quote,
            kind,
            spec,
            data_venue,
        } = self;

        let base_new_key = find_asset(&underlying.base)?;
        let quote_new_key = find_asset(&underlying.quote)?;

        let kind = match kind {
            InstrumentKind::Spot => InstrumentKind::Spot,
            InstrumentKind::Perpetual(contract) => InstrumentKind::Perpetual(PerpetualContract {
                contract_size: contract.contract_size,
                settlement_asset: find_asset(&contract.settlement_asset)?,
            }),
            InstrumentKind::Future(contract) => InstrumentKind::Future(FutureContract {
                contract_size: contract.contract_size,
                settlement_asset: find_asset(&contract.settlement_asset)?,
                expiry: contract.expiry,
            }),
            InstrumentKind::Option(contract) => InstrumentKind::Option(OptionContract {
                contract_size: contract.contract_size,
                settlement_asset: find_asset(&contract.settlement_asset)?,
                kind: contract.kind,
                exercise: contract.exercise,
                expiry: contract.expiry,
                strike: contract.strike,
            }),
            InstrumentKind::Cfd(contract) => InstrumentKind::Cfd(CfdContract {
                contract_size: contract.contract_size,
                settlement_asset: find_asset(&contract.settlement_asset)?,
            }),
        };

        let spec = match spec {
            Some(spec) => {
                let InstrumentSpec {
                    price,
                    quantity:
                        InstrumentSpecQuantity {
                            unit,
                            min,
                            increment,
                        },
                    notional,
                } = spec;

                let unit = match unit {
                    OrderQuantityUnits::Asset(asset) => {
                        OrderQuantityUnits::Asset(find_asset(&asset)?)
                    }
                    OrderQuantityUnits::Contract => OrderQuantityUnits::Contract,
                    OrderQuantityUnits::Quote => OrderQuantityUnits::Quote,
                };

                Some(InstrumentSpec {
                    price,
                    quantity: InstrumentSpecQuantity {
                        unit,
                        min,
                        increment,
                    },
                    notional,
                })
            }
            None => None,
        };

        Ok(Instrument {
            exchange,
            name_internal,
            name_exchange,
            underlying: Underlying::new(base_new_key, quote_new_key),
            quote,
            kind,
            spec,
            // The `DataVenue` holds no `AssetKey` — it names a venue and a symbol, and the
            // underlying assets are the instrument's own regardless of who prices it.
            data_venue,
        })
    }
}

impl<ExchangeKey> From<&Instrument<ExchangeKey, Asset>> for MarketDataInstrument {
    fn from(value: &Instrument<ExchangeKey, Asset>) -> Self {
        Self {
            base: value.underlying.base.name_internal.clone(),
            quote: value.underlying.quote.name_internal.clone(),
            kind: MarketDataInstrumentKind::from(&value.kind),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use crate::{
        exchange::ExchangeId,
        instrument::spec::{InstrumentSpecNotional, InstrumentSpecPrice},
    };
    use rust_decimal::Decimal;

    /// An `AAPL` traded on one venue, optionally priced by another.
    fn instrument(data_venue: Option<DataVenue<ExchangeId>>) -> Instrument<ExchangeId, Asset> {
        let instrument = Instrument::spot(
            ExchangeId::AlpacaBroker,
            "alpaca_broker-aapl",
            "AAPL",
            Underlying::new(
                Asset::new_from_exchange("aapl"),
                Asset::new_from_exchange("usd"),
            ),
            None,
        );

        match data_venue {
            Some(data_venue) => instrument.with_data_venue(data_venue),
            None => instrument,
        }
    }

    /// `data_venue` is declared **last**, and that is load-bearing rather than stylistic:
    /// `Instrument` derives `Ord` from field order and `IndexedInstrumentsBuilder` sorts the
    /// collection before assigning each `InstrumentIndex`, so the field's *position* is part of how
    /// instruments are numbered. Move it above `spec` and this test fails while every existing
    /// collection silently renumbers — indices that are serialized into engine state, the audit
    /// replica and backtest replay streams.
    #[test]
    fn data_venue_sorts_last_so_it_renumbers_no_existing_collection() {
        // Identical up to `spec`, which is declared BEFORE `data_venue`. `Option: None < Some`, so
        // each field on its own would order these opposite ways: `spec` puts the specless
        // instrument first, `data_venue` puts the venueless one first. Only one of them can decide,
        // and it must be the earlier field.
        let no_spec_with_data_venue =
            instrument(Some(DataVenue::new_same_name(ExchangeId::LseEquities)));
        let mut spec_without_data_venue = instrument(None);
        spec_without_data_venue.spec = Some(InstrumentSpec::new(
            InstrumentSpecPrice::new(Decimal::ONE, Decimal::ONE),
            InstrumentSpecQuantity::new(OrderQuantityUnits::Quote, Decimal::ONE, Decimal::ONE),
            InstrumentSpecNotional::new(Decimal::ONE),
        ));

        assert!(
            no_spec_with_data_venue < spec_without_data_venue,
            "an earlier field must decide the order, whatever `data_venue` says"
        );

        // And among instruments equal in every earlier field, it is the tie-break — so declaring it
        // appends to the sort key rather than displacing any part of it.
        assert!(instrument(None) < no_spec_with_data_venue);
    }

    #[test]
    fn a_single_venue_instrument_reports_its_execution_venue_as_its_data_venue() {
        // The fallback is what lets every caller read `data_exchange()` unconditionally, rather
        // than each one re-deriving `data_venue.unwrap_or(exchange)` and one of them getting it
        // wrong.
        let instrument = instrument(None);

        assert_eq!(instrument.data_exchange(), &ExchangeId::AlpacaBroker);
        assert_eq!(
            instrument.data_name_exchange(),
            &InstrumentNameExchange::new("AAPL")
        );
    }

    #[test]
    fn a_data_venue_without_a_symbol_overrides_the_venue_but_keeps_the_execution_symbol() {
        // The common case: both venues spell it the same way, so only the venue is stated.
        let instrument = instrument(Some(DataVenue::new_same_name(ExchangeId::LseEquities)));

        assert_eq!(instrument.data_exchange(), &ExchangeId::LseEquities);
        assert_eq!(
            instrument.data_name_exchange(),
            &InstrumentNameExchange::new("AAPL")
        );
        // The execution side is untouched — this is one instrument on one ledger, not two.
        assert_eq!(instrument.exchange, ExchangeId::AlpacaBroker);
        assert_eq!(
            instrument.name_exchange,
            InstrumentNameExchange::new("AAPL")
        );
    }

    #[test]
    fn a_data_venue_symbol_overrides_the_execution_symbol() {
        // The case the field exists for: LSE quotes BP as `BP.L`, a US broker as `BP`. Subscribing
        // under the execution symbol would silently receive nothing, or another instrument's
        // prices.
        let instrument = instrument(Some(DataVenue::new(
            ExchangeId::LseEquities,
            Some(InstrumentNameExchange::new("BP.L")),
        )));

        assert_eq!(instrument.data_exchange(), &ExchangeId::LseEquities);
        assert_eq!(
            instrument.data_name_exchange(),
            &InstrumentNameExchange::new("BP.L")
        );
        assert_eq!(
            instrument.name_exchange,
            InstrumentNameExchange::new("AAPL")
        );
    }

    #[test]
    fn map_exchange_key_maps_the_data_venue_too() {
        // The whole reason `map_exchange_key` takes a lookup rather than one pre-resolved key: the
        // two venues are different exchanges, so a single key cannot serve both. Were the data
        // venue left unmapped (or stamped with the execution venue's key), market-data state would
        // point at the wrong exchange with nothing to signal it.
        let instrument = instrument(Some(DataVenue::new(
            ExchangeId::LseEquities,
            Some(InstrumentNameExchange::new("BP.L")),
        )));

        let mapped = instrument.map_exchange_key(|exchange| exchange.as_str().to_owned());

        assert_eq!(mapped.exchange, "alpaca_broker");
        assert_eq!(
            mapped
                .data_venue
                .as_ref()
                .map(|venue| venue.exchange.as_str()),
            Some("lse_equities")
        );
        // The symbol rides along unchanged — only the key is mapped.
        assert_eq!(
            mapped.data_name_exchange(),
            &InstrumentNameExchange::new("BP.L")
        );
    }

    #[test]
    fn an_instrument_without_a_data_venue_deserialises_from_a_config_that_predates_the_field() {
        // `data_venue` is additive, so every config written before it existed must still load. If
        // this breaks, the field is a breaking change to on-disk configs rather than to source.
        let json = r#"{
            "exchange": "binance_spot",
            "name_internal": "binance_spot-btc_usdt",
            "name_exchange": "BTCUSDT",
            "underlying": {
                "base": { "name_internal": "btc", "name_exchange": "BTC" },
                "quote": { "name_internal": "usdt", "name_exchange": "USDT" }
            },
            "quote": "underlying_quote",
            "kind": "spot",
            "spec": null
        }"#;

        let instrument =
            serde_json::from_str::<Instrument<ExchangeId, Asset>>(json).expect("must deserialise");

        assert_eq!(instrument.data_venue, None);
        assert_eq!(instrument.data_exchange(), &ExchangeId::BinanceSpot);
    }

    #[test]
    fn a_data_venue_round_trips_through_serde() {
        let instrument = instrument(Some(DataVenue::new(
            ExchangeId::LseEquities,
            Some(InstrumentNameExchange::new("BP.L")),
        )));

        let json = serde_json::to_string(&instrument).expect("must serialise");
        let round_tripped =
            serde_json::from_str::<Instrument<ExchangeId, Asset>>(&json).expect("must deserialise");

        assert_eq!(round_tripped, instrument);
    }
}
