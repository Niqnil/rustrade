use crate::{
    error::DataError,
    event::DataKind,
    exchange::lse::{
        client::LseClient,
        error::LseError,
        historical::LseCandleRequest,
        market::{self, LseMarketCode},
    },
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

/// One instrument's candle source: which LSE market and symbol to fetch, and what to tag it as.
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
/// - `exchange` must be the [`ExchangeId`] the instrument is priced on: its `DataVenue`'s exchange
///   if it declares one, otherwise the exchange it was registered under. Either is one the engine
///   knows, and engine state panics on a market event from an exchange it does not.
///
/// # Spelling
/// `symbol` is the name the instrument is registered under: the websocket spelling (`EUR/USD`,
/// `VOD.L`, `SP500.F`, `BTC/USD`). The fetch translates it to the REST spelling with
/// [`rest_symbol_for`](market::rest_symbol_for), whose caveat on crypto applies: a crypto pair's
/// REST candles are priced against Tether.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct LseCandleSource {
    /// The market the instrument is listed in.
    pub market: LseMarketCode,
    /// The registered (websocket) spelling, e.g. `EUR/USD`, `AAPL`, `BP.L`, `SP500.F`.
    pub symbol: SmolStr,
    /// The engine-side key for this instrument.
    pub instrument: InstrumentIndex,
    /// The exchange this instrument is priced on: its `DataVenue`'s exchange if it declares one,
    /// otherwise the exchange it is registered under.
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
        market: LseMarketCode,
        symbol: impl Into<SmolStr>,
        instrument: InstrumentIndex,
        exchange: ExchangeId,
    ) -> Self {
        Self {
            market,
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
    /// The symbol is matched on the data side, so an instrument executed on one venue and priced
    /// here through a `DataVenue` resolves as well as one registered on `exchange` directly.
    ///
    /// # Errors
    /// - [`LseError::UnknownInstrument`] if no instrument is priced on `exchange` under `symbol`.
    /// - [`LseError::AmbiguousInstrument`] if more than one is.
    /// - [`LseError::QuoteAssetMismatch`] if one is, but prices it in a different asset than the
    ///   provider quotes that symbol in.
    /// - [`LseError::InvalidInput`] if `symbol` is not in `market`'s websocket form.
    pub fn resolve(
        instruments: &IndexedInstruments,
        exchange: ExchangeId,
        market: LseMarketCode,
        symbol: impl Into<SmolStr>,
    ) -> Result<Self, LseError> {
        let symbol = symbol.into();
        let instrument = market::instrument_index_for(instruments, exchange, &symbol)?;
        // Checked here rather than at fetch time, so a name the translation cannot read fails while
        // the replay is being assembled, not partway through it.
        market::rest_symbol_for(market, &symbol)?;

        Ok(Self {
            market,
            symbol,
            instrument,
            exchange,
        })
    }
}

/// Replay historical candles for N instruments as ONE time-ordered market stream.
///
/// This is the bridge between the per-symbol candle fetch and a consumer that wants a single feed —
/// a backtest harness above all, which exposes exactly one stream. Each source is fetched
/// independently, tagged with its own [`InstrumentIndex`], and k-way merged on `time_exchange`.
///
/// Nothing is buffered beyond one event per source, so memory is O(N) in the number of instruments
/// and O(1) in the length of the range. The whole thing is lazy: no request is issued until the
/// stream is polled.
///
/// # `time_exchange` is the candle's `close_time`
/// A bar enters the timeline when its period **ends**. Stamping the open would let a strategy act
/// on a completed bar at the instant its period began — lookahead, silently. The provider reports
/// only the open instant; `close_time` is derived library-side by
/// [`fetch_candles`](LseClient::fetch_candles).
///
/// # Ordering
/// Each source is ascending by `close_time`, which is what the merge requires. Equal timestamps
/// across instruments resolve to the earlier entry in `sources`, so a given `sources` ordering
/// replays identically every time.
///
/// [`fetch_candles`](LseClient::fetch_candles) refuses a series that steps backwards
/// ([`LseError::NonMonotonicCandles`]) rather than reorder it, so [`merge_time_sorted`]'s caller
/// obligation holds or the replay fails observably.
///
/// # Cost, and how the provider's allowance is respected
/// One chunked fetch per source. The merge polls every source on every `poll_next` and cannot emit
/// until all have buffered, so N fetches making progress at once is guaranteed rather than
/// incidental — and re-running the replay re-fetches everything. For repeated runs over the same
/// range, fetch once to local storage and replay from that.
///
/// The provider's limits (calls per minute, one download at a time, both reported by
/// [`usage`](LseClient::usage)) are **not** multiplied by N here: every fetch shares `client`,
/// and therefore its
/// [request gate](LseClient#request-rationing-is-per-client-not-per-call) — requests in flight
/// stay under [`with_concurrency`](LseClient::with_concurrency) and their starts stay
/// [`with_pace`](LseClient::with_pace) apart no matter how many sources are passed. A large N
/// therefore makes the replay *slower*, not louder. Pass a client built by `new`, not one per
/// source: two independently-built clients ration independently.
///
/// # Errors
/// A failed fetch on any source is forwarded immediately, ahead of buffered events from the others:
/// once one instrument's series is incomplete, the merged replay is no longer the dataset that was
/// asked for. [`LseError`] is flattened into
/// [`DataError::Lse`] — handle `LseError` at the [`fetch_candles`](LseClient::fetch_candles)
/// level if you need to match on the cause.
///
/// # ⚠️ Licensing
/// The data this replays is **not redistributable**. See the [module docs](super).
pub fn replay_candles(
    client: Arc<LseClient>,
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
                source.market,
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

/// Drive [`fetch_candles`](LseClient::fetch_candles) from an owned client, translating the
/// registered spelling to the REST one.
///
/// `fetch_candles` borrows both, producing a stream tied to their lifetimes. Consumers such as
/// `BacktestMarketData` require a `'static` stream, so the borrow is moved inside the generator
/// here rather than pushed onto the caller.
fn owned_candles(
    client: Arc<LseClient>,
    market: LseMarketCode,
    symbol: SmolStr,
    interval: CandleInterval,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> impl Stream<Item = Result<Candle, DataError>> + Send + 'static {
    try_stream! {
        let rest = market::rest_symbol_for(market, &symbol).map_err(DataError::from)?;
        let candles =
            client.fetch_candles(LseCandleRequest::new(market, rest, interval, start, end));
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
    use crate::exchange::lse::historical::test_support::csv_answer;
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

    /// A CSV of `count` one-minute candles on 2024-01-02, opening at `first_minute` and stepping
    /// by `step` minutes.
    fn rows(count: i64, first_minute: i64, step: i64) -> String {
        let mut csv = String::from("time,open,high,low,close,volume,trades\n");
        for index in 0..count {
            let minute = first_minute + index * step;
            csv.push_str(&format!(
                "2024-01-02 {:02}:{:02}:00,1,1,1,1,1,1\n",
                minute / 60,
                minute % 60
            ));
        }
        csv
    }

    /// Two symbols on interleaved minutes must come back strictly time-ordered and correctly
    /// attributed — the property a multi-instrument backtest depends on.
    #[tokio::test]
    async fn replay_merges_two_symbols_into_one_time_ordered_stream() {
        let server = wiremock::MockServer::start().await;

        // AAPL opens on even minutes, MSFT on odd, so a correct merge must alternate. The two days
        // fit one chunk, so each symbol is one download.
        for (symbol, first_minute) in [("AAPL", 0), ("MSFT", 1)] {
            wiremock::Mock::given(wiremock::matchers::method("GET"))
                .and(wiremock::matchers::path("/v3/export"))
                .and(wiremock::matchers::query_param("market", "us_stocks"))
                .and(wiremock::matchers::query_param("symbol", symbol))
                .respond_with(csv_answer(&rows(2, first_minute, 2)))
                .expect(1)
                .mount(&server)
                .await;
        }

        let client = Arc::new(
            LseClient::new("key")
                .unwrap()
                .with_base_url(format!("{}/v3", server.uri()))
                // Pacing is exercised in the transport's own tests; disabling it here keeps this
                // suite from sleeping the default interval between every chunk of every source.
                .with_pace(Duration::ZERO),
        );

        let events = replay_candles(
            client,
            vec![
                LseCandleSource::new(
                    LseMarketCode::UsStocks,
                    "AAPL",
                    InstrumentIndex::new(0),
                    ExchangeId::LseEquities,
                ),
                LseCandleSource::new(
                    LseMarketCode::UsStocks,
                    "MSFT",
                    InstrumentIndex::new(1),
                    ExchangeId::LseEquities,
                ),
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

    /// A source registered under the websocket spelling is fetched under the REST one.
    #[tokio::test]
    async fn replay_fetches_the_rest_spelling_of_a_registered_name() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/v3/export"))
            .and(wiremock::matchers::query_param("market", "forex"))
            .and(wiremock::matchers::query_param("symbol", "EUR_USD"))
            .respond_with(csv_answer(&rows(1, 0, 1)))
            .expect(1)
            .mount(&server)
            .await;
        let client = Arc::new(
            LseClient::new("key")
                .unwrap()
                .with_base_url(format!("{}/v3", server.uri()))
                .with_pace(Duration::ZERO),
        );

        let events = replay_candles(
            client,
            vec![LseCandleSource::new(
                LseMarketCode::Forex,
                "EUR/USD",
                InstrumentIndex::new(0),
                ExchangeId::LseFx,
            )],
            CandleInterval::Min1,
            "2024-01-01T00:00:00Z".parse().unwrap(),
            "2024-01-03T00:00:00Z".parse().unwrap(),
        )
        .collect::<Vec<_>>()
        .await;

        assert_eq!(observed(&events).len(), 1);
    }

    #[test]
    fn resolve_refuses_a_name_its_market_cannot_translate() {
        use rustrade_instrument::Underlying;
        use rustrade_instrument::index::builder::IndexedInstrumentsBuilder;
        use rustrade_instrument::instrument::Instrument;

        // Registered under the REST spelling, which the websocket would never match.
        let instruments = IndexedInstrumentsBuilder::default()
            .add_instrument(Instrument::spot(
                ExchangeId::LseFx,
                "eurusd",
                "EUR_USD",
                Underlying::new("eur", "usd"),
                None,
            ))
            .build();

        let error = LseCandleSource::resolve(
            &instruments,
            ExchangeId::LseFx,
            LseMarketCode::Forex,
            "EUR_USD",
        )
        .expect_err("a forex name without `/` is not the websocket spelling");
        assert!(matches!(error, LseError::InvalidInput { .. }), "{error:?}");
    }

    #[tokio::test]
    async fn replay_of_no_sources_is_empty() {
        let client = Arc::new(LseClient::new("key").unwrap());

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
            .respond_with(wiremock::ResponseTemplate::new(500).set_body_raw(
                r#"{"code":"export-failed","message":"upstream unavailable"}"#,
                "application/json",
            ))
            .mount(&server)
            .await;

        let client = Arc::new(
            LseClient::new("key")
                .unwrap()
                .with_base_url(format!("{}/v3", server.uri()))
                // Pacing is exercised in the transport's own tests; disabling it here keeps this
                // suite from sleeping the default interval between every chunk of every source.
                .with_pace(Duration::ZERO),
        );

        let events = replay_candles(
            client,
            vec![LseCandleSource::new(
                LseMarketCode::UsStocks,
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
        let error = LseCandleSource::resolve(
            &instruments,
            ExchangeId::LseEquities,
            LseMarketCode::UkStocks,
            "BP.L",
        )
        .expect_err("a pence-quoted listing registered in pounds must not build a source");
        assert!(matches!(error, LseError::QuoteAssetMismatch { .. }));

        // The correct registration builds, and carries the index the registry issued rather than
        // one the caller supplied.
        let instruments = register("BP.L", "gbx");
        let source = LseCandleSource::resolve(
            &instruments,
            ExchangeId::LseEquities,
            LseMarketCode::UkStocks,
            "BP.L",
        )
        .unwrap();
        assert_eq!(source.instrument, InstrumentIndex::new(0));
        assert_eq!(source.symbol, "BP.L");
        assert_eq!(source.exchange, ExchangeId::LseEquities);

        // The typo that would otherwise leave one instrument silently receiving nothing.
        let error = LseCandleSource::resolve(
            &instruments,
            ExchangeId::LseEquities,
            LseMarketCode::UkStocks,
            "BP",
        )
        .expect_err("an unregistered symbol must not build a source");
        assert!(matches!(error, LseError::UnknownInstrument { .. }));
        assert!(error.to_string().contains("BP.L"));
    }

    /// An instrument executed on one venue and priced on LSE through a `DataVenue` resolves by its
    /// data-side symbol, which is the configuration `DataVenue` exists for. Two instruments priced
    /// under one LSE symbol are refused, since a source feeds one.
    #[test]
    fn resolve_matches_the_data_venue_and_rejects_a_symbol_pricing_two_instruments() {
        use rustrade_instrument::Underlying;
        use rustrade_instrument::index::builder::IndexedInstrumentsBuilder;
        use rustrade_instrument::instrument::Instrument;
        use rustrade_instrument::instrument::data_venue::DataVenue;
        use rustrade_instrument::instrument::name::InstrumentNameExchange;

        let priced_on_lse = |exchange, name_internal: &str| {
            Instrument::spot(
                exchange,
                name_internal,
                "BP",
                Underlying::new("bp", "gbx"),
                None,
            )
            .with_data_venue(DataVenue::new(
                ExchangeId::LseEquities,
                Some(InstrumentNameExchange::new("BP.L")),
            ))
        };

        let instruments = IndexedInstrumentsBuilder::default()
            .add_instrument(priced_on_lse(ExchangeId::Ibkr, "ibkr-bp"))
            .build();
        let source = LseCandleSource::resolve(
            &instruments,
            ExchangeId::LseEquities,
            LseMarketCode::UkStocks,
            "BP.L",
        )
        .unwrap();
        assert_eq!(
            instruments.instruments()[source.instrument.index()]
                .value
                .name_internal
                .as_ref(),
            "ibkr-bp"
        );
        assert_eq!(source.exchange, ExchangeId::LseEquities);

        // The execution-side symbol is not what LSE publishes, so it does not resolve.
        let error = LseCandleSource::resolve(
            &instruments,
            ExchangeId::LseEquities,
            LseMarketCode::UkStocks,
            "BP",
        )
        .expect_err("the execution-side symbol is not priced on LSE");
        assert!(matches!(error, LseError::UnknownInstrument { .. }));

        // The ambiguity spans both ways of being priced on LSE: one registered there directly,
        // with no `DataVenue`, and one priced there through its `DataVenue`.
        let registered_on_lse = Instrument::spot(
            ExchangeId::LseEquities,
            "lse-bp",
            "BP.L",
            Underlying::new("bp", "gbx"),
            None,
        );
        let instruments = IndexedInstrumentsBuilder::default()
            .add_instrument(priced_on_lse(ExchangeId::Ibkr, "ibkr-bp"))
            .add_instrument(registered_on_lse)
            .build();
        let error = LseCandleSource::resolve(
            &instruments,
            ExchangeId::LseEquities,
            LseMarketCode::UkStocks,
            "BP.L",
        )
        .expect_err("one symbol pricing two instruments must not build a source");
        assert!(
            matches!(error, LseError::AmbiguousInstrument { .. }),
            "{error:?}"
        );

        let instruments = IndexedInstrumentsBuilder::default()
            .add_instrument(priced_on_lse(ExchangeId::Ibkr, "ibkr-bp"))
            .add_instrument(priced_on_lse(ExchangeId::AlpacaBroker, "alpaca-bp"))
            .build();
        let error = LseCandleSource::resolve(
            &instruments,
            ExchangeId::LseEquities,
            LseMarketCode::UkStocks,
            "BP.L",
        )
        .expect_err("one symbol pricing two instruments must not build a source");
        let LseError::AmbiguousInstrument { instruments, .. } = &error else {
            panic!("unexpected error: {error:?}");
        };
        assert!(instruments.contains("ibkr-bp"), "{instruments}");
        assert!(instruments.contains("alpaca-bp"), "{instruments}");
    }
}
