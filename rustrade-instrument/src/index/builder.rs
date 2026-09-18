use crate::{
    Keyed,
    asset::{Asset, AssetIndex, ExchangeAsset},
    exchange::{ExchangeId, ExchangeIndex},
    index::{
        IndexedInstruments, error::IndexError, find_asset_by_exchange_and_name_internal,
        find_exchange_by_exchange_id,
    },
    instrument::{Instrument, InstrumentIndex, spec::OrderQuantityUnits},
};
use rust_decimal::Decimal;
use std::collections::HashMap;

#[derive(Debug, Default)]
pub struct IndexedInstrumentsBuilder {
    exchanges: Vec<ExchangeId>,
    instruments: Vec<Instrument<ExchangeId, Asset>>,
    assets: Vec<ExchangeAsset<Asset>>,
}

impl IndexedInstrumentsBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers an [`Instrument`], together with the exchanges and assets it implies.
    ///
    /// Nothing is validated here — duplicates are tolerated and de-duplicated, and every invariant
    /// (unique `name_internal`, positive contract multipliers, resolvable asset references) is
    /// enforced by [`Self::try_build`].
    ///
    /// # What a `data_venue` registers, and what it deliberately does not
    /// An instrument carrying a [`DataVenue`](crate::instrument::data_venue::DataVenue) registers
    /// **two** exchanges — the one it executes on and the one it is priced on. So
    /// [`IndexedInstruments::exchanges`] is wider than the set of `Instrument::exchange` values, and
    /// a data-only venue receives the [`ExchangeIndex`] it needs in order to carry connectivity
    /// state; without one, the first market event from that venue has no state to resolve.
    ///
    /// Its **assets are not registered**. Every asset an instrument implies — underlying base and
    /// quote, settlement asset, and any `OrderQuantityUnits::Asset` — is registered against the
    /// *execution* venue alone, so a data-only venue contributes zero entries to
    /// [`IndexedInstruments::assets`]. An [`ExchangeAsset`] keys balance state, and a venue that
    /// only publishes prices holds no balance to key.
    ///
    /// That asymmetry is load bearing in both directions, and worth knowing before relying on
    /// either half:
    /// - It is why a data-only venue seeds no phantom balances it could never report, and why the
    ///   asset index space is not doubled for every dual-venue instrument.
    /// - It is also why any lookup that reaches an instrument *through* its assets cannot honour a
    ///   data venue. `index_market_data_subscription_batches` is the one in tree: asked for a data
    ///   venue it fails with an asset-index error, and asked for the execution venue it succeeds
    ///   with the correct instrument index against the wrong feed.
    pub fn add_instrument(mut self, instrument: Instrument<ExchangeId, Asset>) -> Self {
        // Add ExchangeId
        self.exchanges.push(instrument.exchange);

        // Add the market-data venue, when the instrument is priced somewhere other than where it
        // is executed. Registering it is what gives a data-only venue an `ExchangeIndex`, and
        // therefore an entry in `ConnectivityStates` -- without which the first market event from
        // that venue hits a missing-exchange lookup.
        //
        // Its ASSETS are deliberately NOT registered. An `ExchangeAsset` keys balance state
        // (`generate_empty_indexed_asset_states`), and a data-only venue holds no balances: it
        // publishes prices and has no account. Registering them would seed phantom balances for a
        // venue that can never report one, and double the asset index space for every dual-venue
        // instrument. The instrument's own asset keys resolve against its execution venue, which
        // is where its balances actually live.
        if let Some(data_venue) = &instrument.data_venue {
            self.exchanges.push(data_venue.exchange);
        }

        // Add Underlying base
        self.assets.push(ExchangeAsset::new(
            instrument.exchange,
            instrument.underlying.base.clone(),
        ));

        // Add Underlying quote
        self.assets.push(ExchangeAsset::new(
            instrument.exchange,
            instrument.underlying.quote.clone(),
        ));

        // Add the settlement asset for every kind that has one (all but Spot)
        if let Some(settlement_asset) = instrument.kind.settlement_asset() {
            self.assets.push(ExchangeAsset::new(
                instrument.exchange,
                settlement_asset.clone(),
            ));
        }

        // Add Instrument OrderQuantityUnits if it's defined in asset units
        // --> likely a duplicate asset, but if so will be filtered during Self::build()
        if let Some(spec) = instrument.spec.as_ref()
            && let OrderQuantityUnits::Asset(asset) = &spec.quantity.unit
        {
            self.assets
                .push(ExchangeAsset::new(instrument.exchange, asset.clone()));
        }

        // Add Instrument
        self.instruments.push(instrument);

        self
    }

    /// Builds an [`IndexedInstruments`], panicking if the added [`Instrument`]s are not a valid
    /// collection.
    ///
    /// # Panics
    /// Panics if two added `Instrument`s share an
    /// [`InstrumentNameInternal`](crate::instrument::name::InstrumentNameInternal), or if any
    /// added `Instrument` carries a non-positive `contract_size` — see [`Self::try_build`], which
    /// returns both as an [`IndexError`] instead.
    pub fn build(self) -> IndexedInstruments {
        // Deliberate panic: `build` is the infallible convenience over `try_build`, and every
        // failure it can raise is a caller error that must not be silently tolerated (see
        // `try_build`'s rustdoc). Panics with the error's `Display`, not `expect`'s `Debug`, so
        // the operator reads the offending instrument rather than a quoted struct dump.
        #[allow(clippy::panic)] // Documented in this method's `# Panics` section.
        self.try_build()
            .unwrap_or_else(|error| panic!("failed to build IndexedInstruments: {error}"))
    }

    /// Builds an [`IndexedInstruments`], returning an [`IndexError`] if the added [`Instrument`]s
    /// are not a valid collection.
    ///
    /// # Errors
    /// Returns [`IndexError::DuplicateInstrumentNameInternal`] if two added `Instrument`s share an
    /// [`InstrumentNameInternal`](crate::instrument::name::InstrumentNameInternal), or
    /// [`IndexError::InvalidContractSize`] if any added `Instrument` carries a non-positive
    /// `contract_size`.
    ///
    /// `InstrumentNameInternal` must be unique across the collection: downstream state maps are
    /// keyed on it while being read **positionally** by [`InstrumentIndex`], so a duplicate
    /// collapses two instruments into one map entry and silently shifts every index past the
    /// collision onto the wrong instrument — attaching positions, PnL and orders to the wrong
    /// place, with no error until the final index panics. Note that the `Instrument` dedup below
    /// does **not** cover this: it removes only instruments that are equal in *every* field, so
    /// two genuinely different instruments that merely share a name survive it.
    pub fn try_build(mut self) -> Result<IndexedInstruments, IndexError> {
        // Sort & dedup
        self.exchanges.sort();
        self.exchanges.dedup();
        self.instruments.sort();
        self.instruments.dedup();
        self.assets.sort();
        self.assets.dedup();

        // Enforce that every contract multiplier is positive. Applied through
        // `InstrumentKind::contract_size`, so it covers every kind uniformly -- `Spot` reports a
        // hard `Decimal::ONE` and can never trip it, and carving the contract-bearing kinds out
        // individually would take more code than checking them all. See
        // `IndexError::InvalidContractSize` for why neither degenerate value fails downstream.
        for instrument in &self.instruments {
            let contract_size = instrument.kind.contract_size();
            if contract_size <= Decimal::ZERO {
                return Err(IndexError::InvalidContractSize(format!(
                    "{} ({:?}) on {} carries contract_size {contract_size}, but a contract \
                     multiplier must be positive - it scales notional, PnL and fees, so zero \
                     silently zeroes all three and a negative value inverts the sign of PnL",
                    instrument.name_internal,
                    instrument.kind,
                    // `as_str`, not `Display`: the canonical snake_case spelling users write in
                    // configs, rather than the bare variant name.
                    instrument.exchange.as_str(),
                )));
            }
        }

        // Enforce the InstrumentNameInternal uniqueness invariant that index-keyed state assumes.
        // Checked after the dedup above so exact duplicate `Instrument`s -- which are legitimate
        // input, already collapsed to one -- do not trip it.
        let mut names = HashMap::with_capacity(self.instruments.len());
        for instrument in &self.instruments {
            if let Some(previous) = names.insert(&instrument.name_internal, instrument) {
                // The whole `Instrument` is retained, not just its `name_exchange`, because the two
                // colliding instruments frequently share that name: a spot and a CFD on one symbol
                // is the collision this check most plausibly catches, and reporting it as
                // "AAPL and AAPL" asserts they are distinct while printing nothing that
                // distinguishes them. `describe_collision` is what does.
                return Err(IndexError::DuplicateInstrumentNameInternal(format!(
                    "{} is shared by the distinct instruments {} and {} on {} - \
                     every Instrument requires a unique name_internal",
                    instrument.name_internal,
                    describe_collision(previous),
                    describe_collision(instrument),
                    // `as_str`, not `Display`: the canonical snake_case spelling users write in
                    // configs, rather than the bare variant name.
                    instrument.exchange.as_str(),
                )));
            }
        }

        // Index Exchanges
        let exchanges = self
            .exchanges
            .into_iter()
            .enumerate()
            .map(|(index, exchange)| Keyed::new(ExchangeIndex::new(index), exchange))
            .collect::<Vec<_>>();

        // Index Assets
        let assets = self
            .assets
            .into_iter()
            .enumerate()
            .map(|(index, exchange_asset)| Keyed::new(AssetIndex::new(index), exchange_asset))
            .collect::<Vec<_>>();

        // Index Instruments (also maps any Instrument AssetKeys -> AssetIndex)
        let instruments = self
            .instruments
            .into_iter()
            .enumerate()
            .map(|(index, instrument)| {
                let exchange_id = instrument.exchange;

                // Applied to the execution venue AND to any `DataVenue`, so a data-only venue is
                // indexed on the same terms. Holds because `add_instrument` registers both.
                let instrument = instrument.map_exchange_key(|id| {
                    #[allow(clippy::expect_used)]
                    // Invariant: add_instrument populates exchanges alongside each instrument
                    let exchange_key = find_exchange_by_exchange_id(&exchanges, id)
                        .expect("every exchange related to every instrument has been added");

                    Keyed::new(exchange_key, *id)
                });

                #[allow(clippy::expect_used)]
                // Invariant: add_instrument populates assets alongside each instrument
                let instrument = instrument
                    .map_asset_key_with_lookup(|asset: &Asset| {
                        find_asset_by_exchange_and_name_internal(
                            &assets,
                            exchange_id,
                            &asset.name_internal,
                        )
                    })
                    .expect("every asset related to every instrument has been added");

                Keyed::new(InstrumentIndex::new(index), instrument)
            })
            .collect();

        Ok(IndexedInstruments {
            exchanges,
            instruments,
            assets,
        })
    }
}

/// Renders the axes two `Instrument`s sharing a `name_internal` can legitimately differ on while
/// still sharing a `name_exchange`: `kind` (a spot and a CFD on one symbol) and `data_venue` (one
/// symbol priced on two venues). Both are compared by the full-equality dedup, so a pair differing
/// only in one of them survives it and reaches the uniqueness check — where naming neither reads as
/// "AAPL and AAPL".
fn describe_collision(instrument: &Instrument<ExchangeId, Asset>) -> String {
    match &instrument.data_venue {
        // `as_str`, not `Display`: the canonical snake_case spelling users write in configs,
        // rather than the bare variant name.
        Some(data_venue) => format!(
            "{} ({:?}, priced on {})",
            instrument.name_exchange,
            instrument.kind,
            data_venue.exchange.as_str(),
        ),
        None => format!("{} ({:?})", instrument.name_exchange, instrument.kind),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use crate::{
        Underlying,
        instrument::{
            data_venue::DataVenue,
            kind::{InstrumentKind, cfd::CfdContract, perpetual::PerpetualContract},
            name::{InstrumentNameExchange, InstrumentNameInternal},
            quote::InstrumentQuoteAsset,
            spec::{
                InstrumentSpec, InstrumentSpecNotional, InstrumentSpecPrice, InstrumentSpecQuantity,
            },
        },
        test_utils::{exchange_asset, instrument},
    };
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    #[test]
    fn test_builder_basic_spot() {
        // Add single spot instrument
        let indexed = IndexedInstrumentsBuilder::default()
            .add_instrument(instrument(ExchangeId::BinanceSpot, "btc", "usdt"))
            .build();

        // Verify state
        assert_eq!(indexed.exchanges().len(), 1);
        assert_eq!(indexed.assets().len(), 2); // BTC and USDT
        assert_eq!(indexed.instruments().len(), 1);

        // Verify exchanges indexes
        assert_eq!(indexed.exchanges()[0].value, ExchangeId::BinanceSpot);

        // Verify asset indexes
        assert_eq!(
            indexed.assets()[0].value,
            exchange_asset(ExchangeId::BinanceSpot, "btc"),
        );
        assert_eq!(
            indexed.assets()[1].value,
            exchange_asset(ExchangeId::BinanceSpot, "usdt"),
        );

        // Very instrument indexes
        assert_eq!(
            indexed.instruments()[0].value,
            Instrument {
                exchange: Keyed::new(ExchangeIndex(0), ExchangeId::BinanceSpot),
                data_venue: None,
                name_exchange: InstrumentNameExchange::new("btc_usdt"),
                name_internal: InstrumentNameInternal::new("binance_spot-btc_usdt"),
                underlying: Underlying {
                    base: AssetIndex(0),
                    quote: AssetIndex(1),
                },
                quote: InstrumentQuoteAsset::UnderlyingQuote,
                kind: InstrumentKind::Spot,
                spec: None
            }
        );
    }

    #[test]
    fn test_builder_deduplication() {
        // Add same spot instrument twice
        let indexed = IndexedInstrumentsBuilder::default()
            .add_instrument(instrument(ExchangeId::BinanceSpot, "BTC", "USDT"))
            .add_instrument(instrument(ExchangeId::BinanceSpot, "BTC", "USDT"))
            .build();

        // Should deduplicate exchanges and assets, but not instruments
        assert_eq!(indexed.exchanges().len(), 1); // Exchange are de-duped
        assert_eq!(indexed.assets().len(), 2); // BTC and USDT and de-duped
        assert_eq!(indexed.instruments().len(), 1); // Instruments are de-duped
    }

    #[test]
    fn test_builder_rejects_duplicate_name_internal() {
        // Two genuinely DIFFERENT instruments that share a `name_internal`. The full-equality
        // `Instrument` dedup does not collapse these, so without the guard they would reach
        // `InstrumentStates` as one map entry, silently shifting every later `InstrumentIndex`
        // onto the wrong instrument.
        let shared_name = "binance_spot-btc_usdt";

        let build = |name_exchange| {
            Instrument::new(
                ExchangeId::BinanceSpot,
                shared_name,
                name_exchange,
                Underlying::new(
                    Asset::new_from_exchange("btc"),
                    Asset::new_from_exchange("usdt"),
                ),
                InstrumentQuoteAsset::UnderlyingQuote,
                InstrumentKind::Spot,
                None,
            )
        };

        let error = IndexedInstrumentsBuilder::default()
            .add_instrument(build("BTCUSDT"))
            .add_instrument(build("BTC-USDT"))
            .try_build()
            .expect_err("duplicate name_internal must be rejected");

        let IndexError::DuplicateInstrumentNameInternal(message) = &error else {
            panic!("unexpected error variant: {error:?}")
        };

        // The message must name the duplicate and both instruments, or it cannot be acted on.
        assert!(message.contains(shared_name), "{message}");
        assert!(message.contains("BTCUSDT"), "{message}");
        assert!(message.contains("BTC-USDT"), "{message}");
        assert!(message.contains("binance_spot"), "{message}");
    }

    #[test]
    fn test_duplicate_name_internal_message_distinguishes_instruments_sharing_a_name_exchange() {
        // The collision this guard most plausibly catches: a spot and a CFD on one symbol, which is
        // the reason `Cfd` is a distinct kind at all. Both carry the same `name_exchange`, so a
        // message built from names alone reads "AAPL and AAPL on ibkr" -- asserting the two are
        // distinct while printing nothing that says how.
        let shared_name = "ibkr-aapl";

        let build = |kind| {
            Instrument::new(
                ExchangeId::Ibkr,
                shared_name,
                "AAPL",
                Underlying::new(
                    Asset::new_from_exchange("aapl"),
                    Asset::new_from_exchange("usd"),
                ),
                InstrumentQuoteAsset::UnderlyingQuote,
                kind,
                None,
            )
        };

        let error = IndexedInstrumentsBuilder::default()
            .add_instrument(build(InstrumentKind::Spot))
            .add_instrument(build(InstrumentKind::Cfd(CfdContract {
                contract_size: Decimal::ONE,
                settlement_asset: Asset::new_from_exchange("usd"),
            })))
            .try_build()
            .expect_err("duplicate name_internal must be rejected");

        let IndexError::DuplicateInstrumentNameInternal(message) = &error else {
            panic!("unexpected error variant: {error:?}")
        };

        assert!(message.contains(shared_name), "{message}");
        // `kind` is the only field that differs, so it is the only thing that makes the message
        // actionable.
        assert!(message.contains("Spot"), "{message}");
        assert!(message.contains("Cfd"), "{message}");
    }

    #[test]
    fn test_duplicate_name_internal_message_distinguishes_instruments_sharing_everything_but_a_data_venue()
     {
        // The other axis a colliding pair can differ on while sharing `name_exchange` AND `kind`:
        // one symbol configured twice, priced on a different venue each time. `data_venue` is part
        // of `Instrument`s equality, so the dedup does not collapse these either -- and a message
        // built from `name_exchange` and `kind` alone would read "AAPL (Spot) and AAPL (Spot)".
        let shared_name = "ibkr-aapl";

        let build = |data_venue| {
            Instrument::new(
                ExchangeId::Ibkr,
                shared_name,
                "AAPL",
                Underlying::new(
                    Asset::new_from_exchange("aapl"),
                    Asset::new_from_exchange("usd"),
                ),
                InstrumentQuoteAsset::UnderlyingQuote,
                InstrumentKind::Spot,
                None,
            )
            .with_data_venue(DataVenue::new_same_name(data_venue))
        };

        let error = IndexedInstrumentsBuilder::default()
            .add_instrument(build(ExchangeId::LseEquities))
            .add_instrument(build(ExchangeId::BinanceSpot))
            .try_build()
            .expect_err("duplicate name_internal must be rejected");

        let IndexError::DuplicateInstrumentNameInternal(message) = &error else {
            panic!("unexpected error variant: {error:?}")
        };

        assert!(message.contains(shared_name), "{message}");
        // The data venue is the only field that differs, so naming both is the only thing that
        // tells the operator which of the two configured entries to change.
        assert!(message.contains("lse_equities"), "{message}");
        assert!(message.contains("binance_spot"), "{message}");
    }

    /// Builds a one-instrument collection whose only variable is the CFD multiplier.
    fn cfd_with_contract_size(contract_size: Decimal) -> Result<IndexedInstruments, IndexError> {
        IndexedInstrumentsBuilder::default()
            .add_instrument(Instrument::new(
                ExchangeId::Ibkr,
                "ibkr-spx500_usd",
                "SPX500",
                Underlying::new(
                    Asset::new_from_exchange("spx500"),
                    Asset::new_from_exchange("usd"),
                ),
                InstrumentQuoteAsset::UnderlyingQuote,
                InstrumentKind::Cfd(CfdContract {
                    contract_size,
                    settlement_asset: Asset::new_from_exchange("usd"),
                }),
                None,
            ))
            .try_build()
    }

    #[test]
    fn a_zero_contract_size_is_rejected_rather_than_silently_zeroing_notional_pnl_and_fees() {
        // Zero is the dangerous value precisely because nothing downstream fails on it: every
        // notional, fee and PnL becomes zero, so the run completes and reads as a strategy that
        // found no edge.
        let error = cfd_with_contract_size(Decimal::ZERO)
            .expect_err("a zero contract multiplier must be rejected");

        let IndexError::InvalidContractSize(message) = &error else {
            panic!("unexpected error variant: {error:?}")
        };

        // The message must identify which instrument carried it, or an operator with a hundred
        // configured instruments cannot act on it.
        assert!(message.contains("ibkr-spx500_usd"), "{message}");
        assert!(message.contains("ibkr"), "{message}");
    }

    #[test]
    fn a_negative_contract_size_is_rejected_rather_than_inverting_the_sign_of_pnl() {
        let error = cfd_with_contract_size(dec!(-25))
            .expect_err("a negative contract multiplier must be rejected");

        assert!(
            matches!(error, IndexError::InvalidContractSize(_)),
            "unexpected error variant: {error:?}"
        );
    }

    #[test]
    fn a_positive_contract_size_builds() {
        // The control: the guard must reject only the degenerate values. €25-per-point is an
        // ordinary index CFD multiplier, not an edge case.
        let indexed = cfd_with_contract_size(dec!(25)).expect("a positive multiplier is valid");

        assert_eq!(indexed.instruments().len(), 1);
        assert_eq!(
            indexed.instruments()[0].value.kind.contract_size(),
            dec!(25)
        );
    }

    #[test]
    fn the_contract_size_guard_covers_every_kind_that_carries_one() {
        // The check reads `InstrumentKind::contract_size`, so it is uniform by construction -- but
        // that is the property worth pinning, since a future kind gaining a multiplier inherits
        // the guard for free only as long as it reports through that accessor.
        let usd = || Asset::new_from_exchange("usd");

        let kinds = [
            InstrumentKind::Perpetual(PerpetualContract {
                contract_size: Decimal::ZERO,
                settlement_asset: usd(),
            }),
            InstrumentKind::Cfd(CfdContract {
                contract_size: Decimal::ZERO,
                settlement_asset: usd(),
            }),
        ];

        for kind in kinds {
            let error = IndexedInstrumentsBuilder::default()
                .add_instrument(Instrument::new(
                    ExchangeId::Ibkr,
                    "ibkr-aapl",
                    "AAPL",
                    Underlying::new(Asset::new_from_exchange("aapl"), usd()),
                    InstrumentQuoteAsset::UnderlyingQuote,
                    kind.clone(),
                    None,
                ))
                .try_build()
                .expect_err("a zero contract multiplier must be rejected on every kind");

            assert!(
                matches!(error, IndexError::InvalidContractSize(_)),
                "{kind:?} was not guarded: {error:?}"
            );
        }

        // `Spot` reports a hard `Decimal::ONE` and therefore cannot trip the guard -- pinned so a
        // future change to that accessor cannot make every spot collection unbuildable.
        assert!(
            IndexedInstrumentsBuilder::default()
                .add_instrument(instrument(ExchangeId::BinanceSpot, "btc", "usdt"))
                .try_build()
                .is_ok()
        );
    }

    #[test]
    fn try_new_applies_the_same_collection_invariants_as_the_builder() {
        // `IndexedInstruments::try_new` is the entry point config-derived collections actually
        // take, and it folds into the builder -- but nothing pinned that, so a future
        // reimplementation could bypass `try_build` and lose both guards silently.
        let duplicate = |name_exchange| {
            Instrument::new(
                ExchangeId::BinanceSpot,
                "binance_spot-btc_usdt",
                name_exchange,
                Underlying::new(
                    Asset::new_from_exchange("btc"),
                    Asset::new_from_exchange("usdt"),
                ),
                InstrumentQuoteAsset::UnderlyingQuote,
                InstrumentKind::Spot,
                None,
            )
        };

        assert!(matches!(
            IndexedInstruments::try_new([duplicate("BTCUSDT"), duplicate("BTC-USDT")]),
            Err(IndexError::DuplicateInstrumentNameInternal(_))
        ));

        assert!(matches!(
            IndexedInstruments::try_new([Instrument::new(
                ExchangeId::Ibkr,
                "ibkr-spx500_usd",
                "SPX500",
                Underlying::new(
                    Asset::new_from_exchange("spx500"),
                    Asset::new_from_exchange("usd"),
                ),
                InstrumentQuoteAsset::UnderlyingQuote,
                InstrumentKind::Cfd(CfdContract {
                    contract_size: Decimal::ZERO,
                    settlement_asset: Asset::new_from_exchange("usd"),
                }),
                None,
            )]),
            Err(IndexError::InvalidContractSize(_))
        ));

        // And the valid case still builds, so the assertions above are not passing on a
        // constructor that rejects everything.
        let indexed = IndexedInstruments::try_new([
            instrument(ExchangeId::BinanceSpot, "btc", "usdt"),
            instrument(ExchangeId::BinanceSpot, "eth", "usdt"),
        ])
        .expect("two distinct spot instruments are a valid collection");

        assert_eq!(indexed.instruments().len(), 2);
        assert_eq!(indexed.exchanges().len(), 1);
        // BTC, ETH, USDT -- the shared quote is indexed once.
        assert_eq!(indexed.assets().len(), 3);
    }

    #[test]
    #[should_panic(expected = "invalid contract_size")]
    fn new_panics_on_an_invalid_collection_rather_than_indexing_it() {
        // The infallible convenience must not be a way around the guard.
        let _ = IndexedInstruments::new([Instrument::new(
            ExchangeId::Ibkr,
            "ibkr-spx500_usd",
            "SPX500",
            Underlying::new(
                Asset::new_from_exchange("spx500"),
                Asset::new_from_exchange("usd"),
            ),
            InstrumentQuoteAsset::UnderlyingQuote,
            InstrumentKind::Cfd(CfdContract {
                contract_size: Decimal::ZERO,
                settlement_asset: Asset::new_from_exchange("usd"),
            }),
            None,
        )]);
    }

    #[test]
    #[should_panic(expected = "duplicate InstrumentNameInternal")]
    fn test_builder_build_panics_on_duplicate_name_internal() {
        let build = |name_exchange| {
            Instrument::new(
                ExchangeId::BinanceSpot,
                "binance_spot-btc_usdt",
                name_exchange,
                Underlying::new(
                    Asset::new_from_exchange("btc"),
                    Asset::new_from_exchange("usdt"),
                ),
                InstrumentQuoteAsset::UnderlyingQuote,
                InstrumentKind::Spot,
                None,
            )
        };

        let _ = IndexedInstrumentsBuilder::default()
            .add_instrument(build("BTCUSDT"))
            .add_instrument(build("BTC-USDT"))
            .build();
    }

    #[test]
    fn test_builder_exact_duplicate_instruments_are_not_a_name_collision() {
        // The same instrument added twice is legitimate input -- the pre-existing dedup collapses
        // it to one -- so the uniqueness guard must not fire. Pins the guard's position after the
        // dedup rather than before it.
        let indexed = IndexedInstrumentsBuilder::default()
            .add_instrument(instrument(ExchangeId::BinanceSpot, "btc", "usdt"))
            .add_instrument(instrument(ExchangeId::BinanceSpot, "btc", "usdt"))
            .try_build()
            .expect("exact duplicates are deduped, not a name collision");

        assert_eq!(indexed.instruments().len(), 1);
    }

    /// An `AAPL` executed on Alpaca but priced by LSE — the same-kind data-venue substitution the
    /// field exists for.
    fn dual_venue_instrument() -> Instrument<ExchangeId, Asset> {
        Instrument::spot(
            ExchangeId::AlpacaBroker,
            "alpaca_broker-aapl",
            "AAPL",
            Underlying::new(
                Asset::new_from_exchange("aapl"),
                Asset::new_from_exchange("usd"),
            ),
            None,
        )
        .with_data_venue(DataVenue::new_same_name(ExchangeId::LseEquities))
    }

    #[test]
    fn a_data_only_venue_is_indexed_so_it_can_carry_connectivity_state() {
        // The registration that makes a data-only venue expressible at all: it appears on no
        // instrument's `exchange`, so without this it would have no `ExchangeIndex` and no
        // `ConnectivityState`, and its first market event would hit a missing-exchange lookup.
        let indexed = IndexedInstrumentsBuilder::default()
            .add_instrument(dual_venue_instrument())
            .try_build()
            .expect("a dual-venue instrument is a valid collection");

        let exchanges = indexed
            .exchanges()
            .iter()
            .map(|keyed| keyed.value)
            .collect::<Vec<_>>();

        assert!(
            exchanges.contains(&ExchangeId::AlpacaBroker),
            "{exchanges:?}"
        );
        assert!(
            exchanges.contains(&ExchangeId::LseEquities),
            "{exchanges:?}"
        );
    }

    #[test]
    fn a_data_only_venue_contributes_no_assets() {
        // A data-only venue holds no balances, and `ExchangeAsset` keys balance state. Registering
        // its assets would seed balances for an account that does not exist, and double the asset
        // index space for every dual-venue instrument.
        let indexed = IndexedInstrumentsBuilder::default()
            .add_instrument(dual_venue_instrument())
            .try_build()
            .expect("a dual-venue instrument is a valid collection");

        // AAPL and USD, on the execution venue only.
        assert_eq!(indexed.assets().len(), 2);
        assert!(
            indexed
                .assets()
                .iter()
                .all(|keyed| keyed.value.exchange == ExchangeId::AlpacaBroker),
            "{:?}",
            indexed.assets()
        );
    }

    #[test]
    fn a_dual_venue_instrument_carries_the_data_venues_own_exchange_index() {
        // The indexing must resolve the data venue against the data venue -- stamping the
        // execution venue's key onto it would point market-data state at the wrong exchange, with
        // nothing at runtime to signal it.
        let indexed = IndexedInstrumentsBuilder::default()
            .add_instrument(dual_venue_instrument())
            .try_build()
            .expect("a dual-venue instrument is a valid collection");

        let instrument = &indexed.instruments()[0].value;

        let data_venue = instrument
            .data_venue
            .as_ref()
            .expect("the data venue survives indexing");

        assert_eq!(data_venue.exchange.value, ExchangeId::LseEquities);
        assert_ne!(data_venue.exchange.key, instrument.exchange.key);

        // And the key is the one the exchange index actually assigned to LSE.
        let expected = indexed
            .exchanges()
            .iter()
            .find(|keyed| keyed.value == ExchangeId::LseEquities)
            .expect("LSE is indexed")
            .key;
        assert_eq!(data_venue.exchange.key, expected);
    }

    #[test]
    fn a_data_venue_pointing_at_the_execution_venue_does_not_duplicate_the_exchange() {
        // Degenerate but legal: stating the data venue explicitly when it is the same venue. The
        // exchange dedup must absorb it, or the venue would be indexed twice and every later
        // ExchangeIndex would shift.
        let builder = IndexedInstrumentsBuilder::default().add_instrument(
            instrument(ExchangeId::BinanceSpot, "btc", "usdt")
                .with_data_venue(DataVenue::new_same_name(ExchangeId::BinanceSpot)),
        );

        // Asserted before the build, because the post-build count alone would also hold if
        // `add_instrument` never registered the data venue at all -- which is the bug this test
        // must not pass through. The registration is unconditional; the dedup is what makes the
        // degenerate case harmless.
        assert_eq!(
            builder.exchanges,
            vec![ExchangeId::BinanceSpot; 2],
            "the data venue is registered unconditionally, whatever venue it names",
        );

        let indexed = builder
            .try_build()
            .expect("a self-referential data venue is valid, if redundant");

        assert_eq!(indexed.exchanges().len(), 1);
    }

    #[test]
    fn test_builder_multiple_exchanges() {
        // Add instruments from different exchanges
        let indexed = IndexedInstrumentsBuilder::default()
            .add_instrument(instrument(ExchangeId::BinanceSpot, "BTC", "USDT"))
            .add_instrument(instrument(ExchangeId::Coinbase, "BTC", "USD"))
            .build();

        // Should maintain separate indices for same asset on different exchanges
        assert_eq!(indexed.exchanges().len(), 2);
        assert_eq!(indexed.assets().len(), 4); // BTC on both exchanges, USDT and USD
        assert_eq!(indexed.instruments().len(), 2);
    }

    #[test]
    fn test_builder_asset_unit_handling() {
        // Create instrument with asset-based order quantity
        let base_asset = Asset::new_from_exchange("BTC");
        let quote_asset = Asset::new_from_exchange("USDT");

        let instrument = Instrument::new(
            ExchangeId::BinanceSpot,
            "binance_spot_btc_usdt",
            "BTC-USDT",
            Underlying::new(base_asset.clone(), quote_asset.clone()),
            InstrumentQuoteAsset::UnderlyingQuote,
            InstrumentKind::Spot,
            Some(InstrumentSpec {
                price: InstrumentSpecPrice {
                    min: dec!(0.1),
                    tick_size: dec!(0.1),
                },
                quantity: InstrumentSpecQuantity {
                    unit: OrderQuantityUnits::Asset(base_asset.clone()),
                    min: dec!(0.001),
                    increment: dec!(0.001),
                },
                notional: InstrumentSpecNotional { min: dec!(10) },
            }),
        );

        let indexed = IndexedInstrumentsBuilder::default()
            .add_instrument(instrument)
            .build();

        // Should index the asset used in OrderQuantityUnits
        assert_eq!(indexed.assets().len(), 2);
        assert_eq!(
            indexed.assets()[0].value,
            exchange_asset(ExchangeId::BinanceSpot, "BTC")
        );
    }

    #[test]
    fn test_builder_ordering() {
        // Add instruments in any order
        let indexed = IndexedInstrumentsBuilder::default()
            .add_instrument(instrument(ExchangeId::BinanceSpot, "BTC", "USDT"))
            .add_instrument(instrument(ExchangeId::Coinbase, "ETH", "USD"))
            .build();

        // Verify exchanges are ordered by input sequence
        assert_eq!(indexed.exchanges()[0].value, ExchangeId::BinanceSpot);
        assert_eq!(indexed.exchanges()[1].value, ExchangeId::Coinbase);

        // Verify exchanges are ordered by input sequence
        assert_eq!(
            indexed.assets()[0].value,
            exchange_asset(ExchangeId::BinanceSpot, "BTC")
        );
        assert_eq!(
            indexed.assets()[1].value,
            exchange_asset(ExchangeId::BinanceSpot, "USDT")
        );
        assert_eq!(
            indexed.assets()[2].value,
            exchange_asset(ExchangeId::Coinbase, "ETH")
        );
        assert_eq!(
            indexed.assets()[3].value,
            exchange_asset(ExchangeId::Coinbase, "USD")
        );

        // Verify instruments are ordered by input sequence
        assert_eq!(
            indexed.instruments()[0].value,
            Instrument {
                exchange: Keyed::new(ExchangeIndex(0), ExchangeId::BinanceSpot),
                data_venue: None,
                name_exchange: InstrumentNameExchange::new("BTC_USDT"),
                name_internal: InstrumentNameInternal::new("binance_spot-btc_usdt"),
                underlying: Underlying {
                    base: AssetIndex(0),
                    quote: AssetIndex(1),
                },
                quote: InstrumentQuoteAsset::UnderlyingQuote,
                kind: InstrumentKind::Spot,
                spec: None
            }
        );

        assert_eq!(
            indexed.instruments()[1].value,
            Instrument {
                exchange: Keyed::new(ExchangeIndex(1), ExchangeId::Coinbase),
                data_venue: None,
                name_exchange: InstrumentNameExchange::new("ETH_USD"),
                name_internal: InstrumentNameInternal::new("coinbase-eth_usd"),
                underlying: Underlying {
                    base: AssetIndex(2),
                    quote: AssetIndex(3),
                },
                quote: InstrumentQuoteAsset::UnderlyingQuote,
                kind: InstrumentKind::Spot,
                spec: None
            }
        );
    }
}
