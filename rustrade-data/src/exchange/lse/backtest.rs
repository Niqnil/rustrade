use crate::{
    error::DataError,
    event::DataKind,
    exchange::lse::{error::LseError, market, vault::LseVaultClient},
    streams::{
        consumer::MarketStreamEvent,
        merge::{merge_time_sorted, tag_events},
    },
    subscription::candle::{Candle, CandleInterval},
};
use async_stream::try_stream;
use chrono::{DateTime, Utc};
use futures::{Stream, StreamExt};
use rustrade_instrument::{
    exchange::ExchangeId, index::IndexedInstruments, instrument::InstrumentIndex,
};
use smol_str::SmolStr;
use std::sync::Arc;

/// One instrument's candle source: which vault symbol to fetch, and what to tag it as.
///
/// # Prefer [`resolve`](Self::resolve)
/// It derives both keys from the registry the engine was built with and checks the quote asset,
/// which is the only place a `.L` listing registered in `gbp` rather than `gbx` — pence booked as
/// pounds, every notional 100× wrong — is visible. [`new`](Self::new) takes the caller's word for
/// both and carries no protection; it exists for callers who have no registry to check against.
///
/// # The keys must match the instruments the engine was built with
/// With [`new`](Self::new), `instrument` and `exchange` are **not** derived from the symbol,
/// because only the caller knows how they registered the instrument. Both must match that
/// registration:
///
/// - `instrument` is the [`InstrumentIndex`] the instrument received in `IndexedInstruments`.
///   Positions and unrealised PnL are strictly index-scoped, so a wrong index attributes this
///   symbol's prices to a different instrument — silently.
/// - `exchange` must be the [`ExchangeId`] that instrument was registered under, since engine state
///   panics on a market event from an exchange it does not know. [`LseDataset::exchange_id`] gives
///   the variant a given dataset belongs to.
///
/// [`LseDataset::exchange_id`]: super::market::LseDataset::exchange_id
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct LseCandleSource {
    /// Vault **display symbol** — `EUR/USD`, `AAPL`, `BP.L`, `ES.F`. Not a dataset slug.
    pub symbol: SmolStr,
    /// The engine-side key for this instrument.
    pub instrument: InstrumentIndex,
    /// The exchange this instrument was registered under.
    pub exchange: ExchangeId,
}

impl LseCandleSource {
    /// Create a candle source for one instrument, taking both engine-side keys on trust.
    ///
    /// Prefer [`resolve`](Self::resolve) whenever the registry is in hand: it derives `instrument`
    /// from that registry and rejects a quote-asset disagreement, neither of which this can do.
    /// This exists for callers assembling sources without an `IndexedInstruments` — and it means a
    /// fabricated index, or a `.L` symbol registered in pounds, reaches the engine unremarked.
    pub fn new(
        symbol: impl Into<SmolStr>,
        instrument: InstrumentIndex,
        exchange: ExchangeId,
    ) -> Self {
        Self {
            symbol: symbol.into(),
            instrument,
            exchange,
        }
    }

    /// Create a candle source by resolving the symbol against the engine's instrument registry.
    ///
    /// The checked constructor, and the one to reach for: `instrument` is derived from
    /// `instruments` rather than supplied, so a fabricated or typo'd index is unrepresentable, and
    /// the symbol's quote asset is verified against the registration. That verification is the
    /// candle path's only defence against a London listing registered in `gbp` rather than `gbx`,
    /// which books pence as pounds and inflates every notional, fee, PnL and balance by 100× with
    /// nothing downstream able to notice. See
    /// [`market::instrument_index_for`].
    ///
    /// # Errors
    /// - [`LseError::UnknownInstrument`] if no instrument on `exchange` is registered under
    ///   `symbol` as its exchange-side name.
    /// - [`LseError::QuoteAssetMismatch`] if one is, but prices it in a different asset than the
    ///   provider quotes that symbol in.
    pub fn resolve(
        instruments: &IndexedInstruments,
        exchange: ExchangeId,
        symbol: impl Into<SmolStr>,
    ) -> Result<Self, LseError> {
        let symbol = symbol.into();
        let instrument = market::instrument_index_for(instruments, exchange, &symbol)?;

        Ok(Self {
            symbol,
            instrument,
            exchange,
        })
    }
}

/// Replay historical candles for N instruments as ONE time-ordered market stream.
///
/// This is the bridge between the per-symbol vault fetch and a consumer that wants a single feed —
/// a backtest harness above all, which exposes exactly one stream. Each source is fetched
/// independently, tagged with its own [`InstrumentIndex`], and k-way merged on `time_exchange`.
///
/// Nothing is buffered beyond one event per source, so memory is O(N) in the number of instruments
/// and O(1) in the length of the range. The whole thing is lazy: no request is issued until the
/// stream is polled.
///
/// # `time_exchange` is the candle's `close_time`
/// A bar enters the timeline when its period **ends**. Stamping the open would let a strategy act
/// on a completed bar at the instant its period began — lookahead, silently. The vault reports only
/// the open instant; `close_time` is derived library-side by
/// [`fetch_candles`](LseVaultClient::fetch_candles).
///
/// # Ordering
/// Each source is ascending by `close_time`, which is what the merge requires. Equal timestamps
/// across instruments resolve to the earlier entry in `sources`, so a given `sources` ordering
/// replays identically every time.
///
/// That per-source ordering is the vault's observed behaviour rather than something
/// [`fetch_candles`](LseVaultClient::fetch_candles) enforces: it filters each page to the requested
/// range but never reorders one. A provider that answered a page out of order would therefore
/// breach [`merge_time_sorted`]'s caller obligation and put a non-monotonic clock in front of a
/// backtest — silently, because a merge cannot recover ordering its inputs do not have.
///
/// # Cost, and how the provider's allowance is respected
/// One paged fetch per source. The merge polls every source on every `poll_next` and cannot emit
/// until all have buffered, so N fetches making progress at once is guaranteed rather than
/// incidental — and re-running the replay re-fetches everything. For repeated runs over the same
/// range, fetch once to local storage and replay from that.
///
/// The provider's allowance (`calls_per_minute`, `vault_concurrency`, both reported by
/// [`usage`](LseVaultClient::usage)) is **not** multiplied by N here: every fetch shares `client`,
/// and therefore its
/// [request gate](LseVaultClient#request-rationing-is-per-client-not-per-call) — requests in flight
/// stay under [`with_concurrency`](LseVaultClient::with_concurrency) and their starts stay
/// [`with_pace`](LseVaultClient::with_pace) apart no matter how many sources are passed. A large N
/// therefore makes the replay *slower*, not louder. Pass a client built by `new`, not one per
/// source: two independently-built clients ration independently.
///
/// # Errors
/// A failed fetch on any source is forwarded immediately, ahead of buffered events from the others:
/// once one instrument's series is incomplete, the merged replay is no longer the dataset that was
/// asked for. [`LseError`] is flattened into
/// [`DataError::Lse`] — handle `LseError` at the [`fetch_candles`](LseVaultClient::fetch_candles)
/// level if you need to match on the cause.
///
/// # ⚠️ Licensing
/// The data this replays is **not redistributable**. See the [module docs](super).
pub fn replay_candles(
    client: Arc<LseVaultClient>,
    sources: Vec<LseCandleSource>,
    interval: CandleInterval,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> impl Stream<Item = Result<MarketStreamEvent<InstrumentIndex, DataKind>, DataError>> + Send + 'static
{
    merge_time_sorted(sources.into_iter().map(move |source| {
        tag_events(
            owned_candles(
                Arc::clone(&client),
                source.symbol.clone(),
                interval,
                start,
                end,
            ),
            source.exchange,
            source.instrument,
            |candle: &Candle| candle.close_time,
            DataKind::Candle,
        )
    }))
}

/// Drive [`fetch_candles`](LseVaultClient::fetch_candles) from an owned client and symbol.
///
/// `fetch_candles` borrows both, producing a stream tied to their lifetimes. Consumers such as
/// `BacktestMarketData` require a `'static` stream, so the borrow is moved inside the generator
/// here rather than pushed onto the caller.
fn owned_candles(
    client: Arc<LseVaultClient>,
    symbol: SmolStr,
    interval: CandleInterval,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> impl Stream<Item = Result<Candle, DataError>> + Send + 'static {
    try_stream! {
        let candles = client.fetch_candles(&symbol, interval, start, end);
        futures::pin_mut!(candles);

        while let Some(candle) = candles.next().await {
            yield candle.map_err(DataError::from)?;
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panicking on a bad fixture is acceptable
mod tests {
    use super::*;
    use crate::event::MarketEvent;
    use std::time::Duration;

    /// `(instrument index, time_exchange)` of each replayed candle.
    fn observed(
        events: &[Result<MarketStreamEvent<InstrumentIndex, DataKind>, DataError>],
    ) -> Vec<(usize, DateTime<Utc>)> {
        events
            .iter()
            .map(|event| match event {
                Ok(MarketStreamEvent::Item(MarketEvent {
                    time_exchange,
                    instrument,
                    kind: DataKind::Candle(_),
                    ..
                })) => (instrument.index(), *time_exchange),
                other => panic!("expected a candle Item, got {other:?}"),
            })
            .collect()
    }

    /// `count` one-minute candle rows on 2024-01-02, opening at `first_minute` and stepping by
    /// `step` minutes.
    fn rows(count: i64, first_minute: i64, step: i64) -> String {
        let rows: Vec<String> = (0..count)
            .map(|index| {
                let minute = first_minute + index * step;
                format!(
                    r#"{{"ts":"2024-01-02 {:02}:{:02}:00","open":1.0,"high":1.0,"low":1.0,"close":1.0,"volume":1}}"#,
                    minute / 60,
                    minute % 60
                )
            })
            .collect();

        format!("[{}]", rows.join(","))
    }

    /// Two symbols on interleaved minutes must come back strictly time-ordered and correctly
    /// attributed — the property a multi-instrument backtest depends on.
    #[tokio::test]
    async fn replay_merges_two_symbols_into_one_time_ordered_stream() {
        let server = wiremock::MockServer::start().await;

        // AAPL opens on even minutes, MSFT on odd, so a correct merge must alternate. Each symbol
        // serves one page; the fallback below then ends its pagination.
        for (symbol, first_minute) in [("AAPL", 0), ("MSFT", 1)] {
            wiremock::Mock::given(wiremock::matchers::method("GET"))
                .and(wiremock::matchers::path("/vault/candles"))
                .and(wiremock::matchers::query_param("symbol", symbol))
                .respond_with(
                    wiremock::ResponseTemplate::new(200)
                        .set_body_raw(rows(2, first_minute, 2), "application/json"),
                )
                .up_to_n_times(1)
                .mount(&server)
                .await;
        }

        // A short page is not a terminal signal (the row cap is applied silently), so pagination
        // continues until an empty page arrives.
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .respond_with(
                wiremock::ResponseTemplate::new(200).set_body_raw("[]", "application/json"),
            )
            .mount(&server)
            .await;

        let client = Arc::new(
            LseVaultClient::new("key")
                .unwrap()
                .with_base_url(format!("{}/vault", server.uri()))
                // Pacing is exercised in `vault`'s own tests; disabling it here keeps this suite
                // from sleeping the default interval between every page of every source.
                .with_pace(Duration::ZERO),
        );

        let events = replay_candles(
            client,
            vec![
                LseCandleSource::new("AAPL", InstrumentIndex::new(0), ExchangeId::LseEquities),
                LseCandleSource::new("MSFT", InstrumentIndex::new(1), ExchangeId::LseEquities),
            ],
            CandleInterval::Min1,
            "2024-01-01T00:00:00Z".parse().unwrap(),
            "2024-01-03T00:00:00Z".parse().unwrap(),
        )
        .collect::<Vec<_>>()
        .await;

        let observed = observed(&events);
        // Strictly ascending, and alternating between the two instruments.
        assert!(
            observed.windows(2).all(|pair| pair[0].1 <= pair[1].1),
            "merged replay must be time-ordered: {observed:?}"
        );
        assert_eq!(
            observed.iter().map(|(index, _)| *index).collect::<Vec<_>>(),
            vec![0, 1, 0, 1]
        );
    }

    #[tokio::test]
    async fn replay_of_no_sources_is_empty() {
        let client = Arc::new(LseVaultClient::new("key").unwrap());

        let events = replay_candles(
            client,
            vec![],
            CandleInterval::Min1,
            "2024-01-01T00:00:00Z".parse().unwrap(),
            "2024-01-03T00:00:00Z".parse().unwrap(),
        )
        .collect::<Vec<_>>()
        .await;

        assert!(events.is_empty());
    }

    /// A failed source must surface rather than silently shortening the replay.
    #[tokio::test]
    async fn replay_surfaces_a_source_failure() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .respond_with(
                wiremock::ResponseTemplate::new(500)
                    .set_body_raw(r#"{"detail":"upstream unavailable"}"#, "application/json"),
            )
            .mount(&server)
            .await;

        let client = Arc::new(
            LseVaultClient::new("key")
                .unwrap()
                .with_base_url(format!("{}/vault", server.uri()))
                // Pacing is exercised in `vault`'s own tests; disabling it here keeps this suite
                // from sleeping the default interval between every page of every source.
                .with_pace(Duration::ZERO),
        );

        let events = replay_candles(
            client,
            vec![LseCandleSource::new(
                "AAPL",
                InstrumentIndex::new(0),
                ExchangeId::LseEquities,
            )],
            CandleInterval::Min1,
            "2024-01-01T00:00:00Z".parse().unwrap(),
            "2024-01-03T00:00:00Z".parse().unwrap(),
        )
        .collect::<Vec<_>>()
        .await;

        assert!(matches!(events.first(), Some(Err(DataError::Lse { .. }))));
    }

    /// The candle path's half of the GBX protection.
    ///
    /// `new` takes both keys on trust, so before `resolve` existed nothing on this path could see a
    /// London listing registered in pounds — the export path's check was unreachable from here, it
    /// being behind `lse-parquet`. These assert the checked constructor closes that.
    #[test]
    fn resolve_derives_the_index_and_rejects_a_london_listing_registered_in_pounds() {
        use rustrade_instrument::Underlying;
        use rustrade_instrument::index::builder::IndexedInstrumentsBuilder;
        use rustrade_instrument::instrument::name::{
            InstrumentNameExchange, InstrumentNameInternal,
        };
        use rustrade_instrument::instrument::quote::InstrumentQuoteAsset;
        use rustrade_instrument::instrument::{Instrument, kind::InstrumentKind};
        use rustrade_instrument::test_utils::asset;

        let register = |symbol: &str, quote: &str| {
            let name = InstrumentNameExchange::from(symbol);
            IndexedInstrumentsBuilder::default()
                .add_instrument(Instrument::new(
                    ExchangeId::LseEquities,
                    InstrumentNameInternal::new_from_exchange(
                        ExchangeId::LseEquities,
                        name.clone(),
                    ),
                    name,
                    Underlying::new(asset(&symbol.to_lowercase()), asset(quote)),
                    InstrumentQuoteAsset::UnderlyingQuote,
                    InstrumentKind::Spot,
                    None,
                ))
                .build()
        };

        // `BP.L` prints ~548 where BP trades around £5.48, and the provider sends no unit -- so the
        // registered asset IS the unit, and booking that as GBP inflates every notional, fee, PnL
        // and balance by 100x with nothing downstream able to tell.
        let instruments = register("BP.L", "gbp");
        let error = LseCandleSource::resolve(&instruments, ExchangeId::LseEquities, "BP.L")
            .expect_err("a pence-quoted listing registered in pounds must not build a source");
        assert!(matches!(error, LseError::QuoteAssetMismatch { .. }));

        // The correct registration builds, and carries the index the registry issued rather than
        // one the caller supplied.
        let instruments = register("BP.L", "gbx");
        let source =
            LseCandleSource::resolve(&instruments, ExchangeId::LseEquities, "BP.L").unwrap();
        assert_eq!(source.instrument, InstrumentIndex::new(0));
        assert_eq!(source.symbol, "BP.L");
        assert_eq!(source.exchange, ExchangeId::LseEquities);

        // The typo that would otherwise leave one instrument silently receiving nothing.
        let error = LseCandleSource::resolve(&instruments, ExchangeId::LseEquities, "BP")
            .expect_err("an unregistered symbol must not build a source");
        assert!(matches!(error, LseError::UnknownInstrument { .. }));
        assert!(error.to_string().contains("BP.L"));
    }
}
