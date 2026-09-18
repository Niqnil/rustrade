use crate::{
    error::DataError,
    event::DataKind,
    instrument::MarketInstrumentData,
    streams::{
        builder::dynamic::DynamicStreams,
        consumer::{MarketStreamEvent, MarketStreamResult},
        reconnect::stream::ReconnectingStream,
    },
    subscription::{SubKind, Subscription},
};
use futures::Stream;
use itertools::Itertools;
use rustrade_instrument::{
    Keyed,
    exchange::ExchangeId,
    index::{IndexedInstruments, error::IndexError},
    instrument::{InstrumentIndex, market_data::MarketDataInstrument},
};
use tracing::warn;

/// Initialise an indexed [`DynamicStreams`] using batches of indexed [`Subscription`] batches.
///
/// This function:
/// 1. Generates indexed market data Subscriptions from all Instrument-SubKind combinations found
///    in the provided `IndexedInstruments` and `SubKind` slice.
/// 2. Initialise an indexed [`DynamicStreams`] .
/// 3. Combines all market streams into a single `Stream` via
///    [`select_all`](futures_util::stream::select_all::select_all)
/// 4. Handles recoverable errors by logging them at `warn` level.
///
/// See [`generate_indexed_market_data_subscription_batches`] for how indexed `Subscriptions` can
/// be conveniently generated from an [`IndexedInstruments`] collection.
///
/// See [`index_market_data_subscription_batches`] for how unindexed `Subscriptions` can be
/// indexed using an [`IndexedInstruments`] collection.
pub async fn init_indexed_multi_exchange_market_stream(
    instruments: &IndexedInstruments,
    sub_kinds: &[SubKind],
) -> Result<impl Stream<Item = MarketStreamEvent<InstrumentIndex, DataKind>> + use<>, DataError> {
    // Generate indexed market data Subscriptions
    let subscriptions = generate_indexed_market_data_subscription_batches(instruments, sub_kinds);

    // Initialise an indexed MarketStream via DynamicStreams
    let stream = DynamicStreams::init(subscriptions)
        .await?
        .select_all::<MarketStreamResult<InstrumentIndex, DataKind>>()
        .with_error_handler(|error| warn!(?error, "MarketStream generated error"));

    Ok(stream)
}

/// Generates batches of indexed market data `Subscriptions` from a collection of
/// `IndexedInstruments`.
///
/// This function:
/// 1. Groups instruments by [`ExchangeId`].
/// 2. Generates indexed `Subscriptions` for each Instrument-SubKind combination.
/// 4. Returns the `Subscriptions` grouped by [`ExchangeId`].
///
/// # Arguments
/// * `instruments` - Collection of `IndexedInstruments` to generate `Subscriptions` for
/// * `sub_kinds` - Slice of `SubKinds` to generate for each instrument
pub fn generate_indexed_market_data_subscription_batches(
    instruments: &IndexedInstruments,
    sub_kinds: &[SubKind],
) -> Vec<Vec<Subscription<ExchangeId, MarketInstrumentData<InstrumentIndex>, SubKind>>> {
    // Generate Iterator<Item = Keyed<ExchangeId, MarketInstrumentData<InstrumentIndex>>>
    let instruments = instruments.instruments().iter().map(|keyed| {
        // The DATA venue, not the execution venue: a subscription asks whoever publishes the
        // prices, which for a dual-venue instrument is not who fills its orders. Falls back to
        // `exchange` for every single-venue instrument, so this is the correct read for both.
        let exchange = keyed.value.data_exchange().value;
        let instrument = MarketInstrumentData::from(keyed);
        Keyed::new(exchange, instrument)
    });

    // Chunk instruments by ExchangeId
    let instruments = instruments.sorted_unstable_by_key(|exchange| exchange.key);

    // Generate Subscriptions
    instruments
        .chunk_by(|exchange| exchange.key)
        .into_iter()
        .map(|(_exchange, instruments)| {
            instruments
                .into_iter()
                .flat_map(
                    |Keyed {
                         key: exchange,
                         value: instrument,
                     }| {
                        sub_kinds
                            .iter()
                            .map(move |kind| Subscription::new(exchange, instrument.clone(), *kind))
                    },
                )
                .collect::<Vec<_>>()
        })
        .collect()
}

/// Indexes batches of market data `Subscriptions` using a collection of `IndexedInstruments`.
///
/// This function maps unindexed market data `Subscriptions` to indexed ones by:
/// 1. Finding the `AssetIndex` for the base and quote assets.
/// 2. Finding the `InstrumentIndex` associated with the `Subscription` `ExchangeId`, `SubKind` and
///    assets.
/// 3. Creating new `Subscriptions` with indexed instruments.
///
///
/// # Arguments
/// * `instruments` - Collection of `IndexedInstruments` used for indexing
/// * `batches` - Iterator of `Subscription` batches to be indexed
///
/// # Limitation: this function cannot honour a [`DataVenue`]
/// Both lookups below resolve against `Subscription::exchange` as the instrument's **execution**
/// venue — its assets via `find_asset_index`, and the instrument itself via `exchange.value ==
/// exchange`. An instrument carrying a [`DataVenue`] is priced by one venue and executed on another,
/// and its assets are registered only under the latter, so **hand-written subscriptions do not
/// support dual-venue instruments**. The two spellings fail differently, and only one of them fails
/// loudly:
///
/// - Naming the **data venue** returns [`DataError`]: that venue has no registered assets, so
///   `find_asset_index` cannot resolve the base or quote.
/// - Naming the **execution venue** returns `Ok`. The `InstrumentIndex` is the right one for that
///   instrument — a caller cannot get the *wrong* instrument out of this function — but the
///   subscription it is attached to names the execution venue's feed and symbol, which is the venue
///   the `DataVenue` exists to say the instrument is *not* priced on. Nothing here can detect that:
///   the lookup is by `(exchange, kind, base, quote)` and every one of those matches.
///
/// Use [`generate_indexed_market_data_subscription_batches`] instead; it reads the data venue
/// directly off each instrument and needs no name-based reverse lookup.
///
/// [`DataVenue`]: rustrade_instrument::instrument::data_venue::DataVenue
pub fn index_market_data_subscription_batches<SubBatchIter, SubIter, Sub>(
    instruments: &IndexedInstruments,
    batches: SubBatchIter,
) -> Result<
    Vec<Vec<Subscription<ExchangeId, Keyed<InstrumentIndex, MarketDataInstrument>>>>,
    DataError,
>
where
    SubBatchIter: IntoIterator<Item = SubIter>,
    SubIter: IntoIterator<Item = Sub>,
    Sub: Into<Subscription<ExchangeId, MarketDataInstrument, SubKind>>,
{
    batches
        .into_iter()
        .map(|batch| batch
            .into_iter()
            .map(|sub| {
                let sub = sub.into();

                let base_index = instruments.find_asset_index(sub.exchange, &sub.instrument.base)?;
                let quote_index = instruments.find_asset_index(sub.exchange, &sub.instrument.quote)?;

                let find_instrument = |exchange, kind, base, quote| {
                    instruments
                        .instruments()
                        .iter()
                        .find_map(|indexed| {
                            (
                                indexed.value.exchange.value == exchange
                                    && indexed.value.kind.eq_market_data_instrument_kind(kind)
                                    && indexed.value.underlying.base == base
                                    && indexed.value.underlying.quote == quote
                            ).then_some(indexed.key)
                        })
                        .ok_or(IndexError::InstrumentIndex(format!(
                            "Instrument: ({}, {}, {}, {}) must be present in indexed instruments: {:?}",
                            exchange, kind, base, quote, instruments.instruments()
                        )))
                };

                let instrument_index = find_instrument(sub.exchange, &sub.instrument.kind, base_index, quote_index)?;

                Ok(Subscription {
                    exchange: sub.exchange,
                    instrument: Keyed::new(instrument_index, sub.instrument),
                    kind: sub.kind,
                })
            })
            .collect()
        )
        .collect()
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use rustrade_instrument::{
        Underlying,
        asset::Asset,
        index::error::IndexError,
        instrument::{
            Instrument, data_venue::DataVenue, market_data::kind::MarketDataInstrumentKind,
            name::InstrumentNameExchange,
        },
    };

    /// `BP` executed on IBKR, priced by LSE — where the two venues disagree on the symbol.
    fn bp_priced_by_lse() -> Instrument<ExchangeId, Asset> {
        Instrument::spot(
            ExchangeId::Ibkr,
            "ibkr-bp",
            "BP",
            Underlying::new(
                Asset::new_from_exchange("bp"),
                Asset::new_from_exchange("usd"),
            ),
            None,
        )
        .with_data_venue(DataVenue::new(
            ExchangeId::LseEquities,
            Some(InstrumentNameExchange::new("BP.L")),
        ))
    }

    #[test]
    fn a_subscription_is_built_against_the_data_venue_and_its_symbol() {
        // Both halves matter, and both fail silently if wrong: subscribing on IBKR asks a venue
        // that was never meant to supply prices here, and subscribing as `BP` asks LSE for a
        // symbol it does not publish. Neither errors -- they just never tick.
        let instruments = IndexedInstruments::new([bp_priced_by_lse()]);

        let batches = generate_indexed_market_data_subscription_batches(
            &instruments,
            &[SubKind::PublicTrades],
        );

        let subscriptions = batches.into_iter().flatten().collect::<Vec<_>>();
        assert_eq!(subscriptions.len(), 1);

        assert_eq!(subscriptions[0].exchange, ExchangeId::LseEquities);
        assert_eq!(
            subscriptions[0].instrument.name_exchange,
            InstrumentNameExchange::new("BP.L")
        );
    }

    #[test]
    fn a_single_venue_subscription_is_unchanged_by_the_data_venue_fallback() {
        // The control: every instrument in the tree today has no data venue, so the fallback must
        // reproduce exactly the previous behaviour.
        let instrument = Instrument::spot(
            ExchangeId::BinanceSpot,
            "binance_spot-btc_usdt",
            "BTCUSDT",
            Underlying::new(
                Asset::new_from_exchange("btc"),
                Asset::new_from_exchange("usdt"),
            ),
            None,
        );
        let instruments = IndexedInstruments::new([instrument]);

        let batches = generate_indexed_market_data_subscription_batches(
            &instruments,
            &[SubKind::PublicTrades],
        );

        let subscriptions = batches.into_iter().flatten().collect::<Vec<_>>();
        assert_eq!(subscriptions.len(), 1);

        assert_eq!(subscriptions[0].exchange, ExchangeId::BinanceSpot);
        assert_eq!(
            subscriptions[0].instrument.name_exchange,
            InstrumentNameExchange::new("BTCUSDT")
        );
    }

    #[test]
    fn instruments_are_batched_by_data_venue_not_execution_venue() {
        // Batches are per-venue connections. Two instruments executed on different venues but
        // priced by the same one belong on ONE connection -- batching by execution venue would
        // open two connections to LSE and, on a capped feed, burn two subscription slots.
        let aapl = Instrument::spot(
            ExchangeId::AlpacaBroker,
            "alpaca_broker-aapl",
            "AAPL",
            Underlying::new(
                Asset::new_from_exchange("aapl"),
                Asset::new_from_exchange("usd"),
            ),
            None,
        )
        .with_data_venue(DataVenue::new_same_name(ExchangeId::LseEquities));

        let instruments = IndexedInstruments::new([aapl, bp_priced_by_lse()]);

        let batches = generate_indexed_market_data_subscription_batches(
            &instruments,
            &[SubKind::PublicTrades],
        );

        assert_eq!(batches.len(), 1, "{batches:?}");
        assert_eq!(batches[0].len(), 2);
        assert!(
            batches[0]
                .iter()
                .all(|sub| sub.exchange == ExchangeId::LseEquities),
            "{:?}",
            batches[0]
        );
    }

    #[test]
    fn hand_written_subscription_naming_the_data_venue_is_rejected() {
        // Half the documented limitation -- the half that fails loudly. Assets are registered under
        // the execution venue only, so LSE resolves neither leg. The value here is the *shape* of
        // the failure: a caller must not be able to receive an `InstrumentIndex` that resolves to an
        // instrument priced by a venue other than the one they subscribed to, because every
        // downstream event would then be attributed to the wrong instrument's state.
        let instruments = IndexedInstruments::new([bp_priced_by_lse()]);

        let subscription = Subscription::new(
            ExchangeId::LseEquities,
            MarketDataInstrument::new("bp", "usd", MarketDataInstrumentKind::Spot),
            SubKind::PublicTrades,
        );

        let result = index_market_data_subscription_batches(&instruments, [[subscription]]);

        // The *variant* is the claim, not merely that it failed. Were assets later registered under
        // the data venue, the instrument lookup would fail instead and a bare `is_err` would keep
        // passing with the rationale above silently false.
        assert!(
            matches!(result, Err(DataError::Index(IndexError::AssetIndex(_)))),
            "the data venue registers no assets, so neither leg can resolve: {result:?}"
        );
    }

    /// KNOWN GAP, pinned so that fixing it is a visible test change rather than a silent one.
    ///
    /// The other half of the limitation, and the half the rustdoc has to spell out because it does
    /// *not* fail loudly: naming the execution venue matches on every component of the lookup key,
    /// so the caller gets `Ok` and a subscription pointed at the venue the `DataVenue` exists to say
    /// this instrument is not priced on. Rework this function to understand data venues and this
    /// assertion should be inverted -- the caller cannot express "IBKR's feed for an LSE-priced
    /// instrument" meaningfully, so `Err` is the defensible answer once the lookup can tell.
    #[test]
    fn hand_written_subscription_naming_the_execution_venue_succeeds_on_the_wrong_feed() {
        let instruments = IndexedInstruments::new([bp_priced_by_lse()]);

        let subscription = Subscription::new(
            ExchangeId::Ibkr,
            MarketDataInstrument::new("bp", "usd", MarketDataInstrumentKind::Spot),
            SubKind::PublicTrades,
        );

        let batches = index_market_data_subscription_batches(&instruments, [[subscription]])
            .expect(
                "naming the execution venue resolves: its assets and instrument are registered",
            );

        let indexed = &batches[0][0];

        // The instrument is the right one -- the documented guarantee that survives.
        assert_eq!(indexed.instrument.key, InstrumentIndex(0));

        // The feed is not. This is the wrong-feed subscription the rustdoc warns about.
        assert_eq!(indexed.exchange, ExchangeId::Ibkr);
        assert_ne!(
            indexed.exchange,
            instruments.instruments()[0].value.data_exchange().value,
            "the subscription names a venue the instrument is explicitly not priced on"
        );
    }
}
