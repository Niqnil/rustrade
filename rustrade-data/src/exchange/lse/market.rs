use super::Lse;
use crate::exchange::lse::error::LseError;
use crate::subscription::candle::CandleInterval;
use crate::{Identifier, instrument::MarketInstrumentData, subscription::Subscription};
use rustrade_instrument::asset::name::AssetNameInternal;
use rustrade_instrument::instrument::name::InstrumentNameExchange;
use rustrade_instrument::instrument::quote::InstrumentQuoteAsset;
use rustrade_instrument::{
    Keyed, Underlying, asset::name::AssetNameExchange, exchange::ExchangeId,
    index::IndexedInstruments, instrument::InstrumentIndex,
    instrument::market_data::MarketDataInstrument,
    instrument::market_data::kind::MarketDataInstrumentKind,
};
use smol_str::{SmolStr, StrExt, format_smolstr};

/// A London Strategic Edge price dataset.
///
/// The provider also publishes reference datasets (bond yields, credit indices, economics, …).
/// Those are not instruments and have no variant here — they are distinguishable in the catalog
/// mechanically, since every price dataset carries an empty `frequency` and `category` while every
/// reference dataset populates both.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum LseDataset {
    Stocks,
    Etf,
    Crypto,
    Fx,
    Index,
    Commodity,
    InterestRates,
    CurrencyIndex,
    Volatility,
    Futures,
}

impl LseDataset {
    /// Every price dataset, in catalog order.
    pub const ALL: [Self; 10] = [
        Self::Stocks,
        Self::Etf,
        Self::Crypto,
        Self::Fx,
        Self::Index,
        Self::Commodity,
        Self::InterestRates,
        Self::CurrencyIndex,
        Self::Volatility,
        Self::Futures,
    ];

    /// Returns the name this dataset carries in the provider's catalog.
    pub fn as_catalog_str(&self) -> &'static str {
        match self {
            Self::Stocks => "stocks",
            Self::Etf => "etf",
            Self::Crypto => "crypto",
            Self::Fx => "fx",
            Self::Index => "index",
            Self::Commodity => "commodity",
            Self::InterestRates => "interest_rates",
            Self::CurrencyIndex => "currency_index",
            Self::Volatility => "volatility",
            Self::Futures => "futures",
        }
    }

    /// Parses a catalog dataset name.
    ///
    /// # Errors
    /// Returns [`LseError::UnknownDataset`] for anything that is not a price dataset — including
    /// the provider's reference datasets, which are not instruments.
    pub fn from_catalog_str(dataset: &str) -> Result<Self, LseError> {
        Self::ALL
            .into_iter()
            .find(|candidate| candidate.as_catalog_str() == dataset)
            .ok_or_else(|| LseError::UnknownDataset(dataset.to_string()))
    }

    /// Returns the [`ExchangeId`] this dataset's market events are stamped with.
    pub fn exchange_id(&self) -> ExchangeId {
        match self {
            // Stocks and ETFs share one identifier: they are the same venue, the same symbology
            // and the same coverage, differing only in what the instrument is.
            Self::Stocks | Self::Etf => ExchangeId::LseEquities,
            Self::Crypto => ExchangeId::LseCrypto,
            Self::Fx => ExchangeId::LseFx,
            Self::Futures => ExchangeId::LseFutures,
            Self::Index
            | Self::Commodity
            | Self::InterestRates
            | Self::CurrencyIndex
            | Self::Volatility => ExchangeId::LseCfd,
        }
    }

    /// Whether this dataset can be exported at a candle resolution.
    ///
    /// The provider splits its price datasets into candle classes and *synthetic* classes. The
    /// synthetic ones serve candles over REST but are **tick-only on the export path** — measured:
    /// `{dataset: volatility, timeframe: 1d}` is rejected with `no candle data for 'volatility';
    /// it is tick-only`, and identically for `interest_rates`.
    ///
    /// # ⚠️ The catalog's `access` map does not answer this
    /// It lists `["candles", "export"]` for the synthetic classes too, so it says only that both
    /// capabilities exist — not that they compose.
    pub fn is_candle_class(&self) -> bool {
        match self {
            Self::Stocks
            | Self::Etf
            | Self::Crypto
            | Self::Fx
            | Self::Index
            | Self::Commodity
            | Self::Futures => true,
            Self::InterestRates | Self::CurrencyIndex | Self::Volatility => false,
        }
    }

    /// Returns the [`MarketDataInstrumentKind`] this dataset serves.
    ///
    /// # Note
    /// The executable counterpart, `InstrumentKind::Cfd`, additionally requires a settlement asset
    /// — the *account* currency, which is a property of the caller's account rather than of the
    /// data — so it cannot be produced here.
    pub fn market_data_kind(&self) -> MarketDataInstrumentKind {
        match self {
            // Spot: deliverable, and priced as a consolidated series rather than a broker's own.
            // FX stays spot rather than CFD: the retail-OTC caveat that would motivate `Cfd` is
            // already expressed un-ignorably by the vault omitting volume entirely.
            Self::Stocks | Self::Etf | Self::Crypto | Self::Fx => MarketDataInstrumentKind::Spot,
            // CFD: no venue, no contract size, no expiry. `futures` belongs here and NOT to
            // `Future` — these series are continuous front-month proxies with no contract chain
            // and no expiry, and `FutureContract` requires one. A fabricated expiry is not inert:
            // it becomes a subscription-binding key and drives contract-expiry settlement.
            Self::Index
            | Self::Commodity
            | Self::InterestRates
            | Self::CurrencyIndex
            | Self::Volatility
            | Self::Futures => MarketDataInstrumentKind::Cfd,
        }
    }

    /// The category label the WebSocket handshake reports for this dataset's symbols, where it
    /// reports one at all.
    ///
    /// # ⚠️ `None` is the common case, not the exception
    /// The handshake publishes no category for roughly half the symbols it offers, and some
    /// entries carry neither a category nor a name. `None` here therefore means *the provider does
    /// not label this dataset*, not *unknown* — so a symbol arriving without a category can never
    /// be treated as contradicting the dataset it was requested on.
    ///
    /// These labels are the provider's own spelling and are deliberately not derived from
    /// [`as_catalog_str`](Self::as_catalog_str): the two vocabularies differ (`etf` against
    /// `ETFs`, `index` against `Indices`) and neither is a transformation of the other.
    pub fn ws_category(&self) -> Option<&'static str> {
        match self {
            Self::Stocks => Some("Stocks"),
            Self::Etf => Some("ETFs"),
            Self::Crypto => Some("Crypto"),
            Self::Fx => Some("Forex"),
            Self::Index => Some("Indices"),
            Self::Commodity => Some("Commodities"),
            Self::InterestRates | Self::CurrencyIndex | Self::Volatility | Self::Futures => None,
        }
    }
}

/// A London Strategic Edge dataset slug — the path key of the provider's dataset-info endpoint.
///
/// A slug is a **discovery key, not an identity**. Candle endpoints are keyed on the display
/// symbol, so nothing in this integration needs a slug to fetch data.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct LseSlug(SmolStr);

impl LseSlug {
    /// Returns the slug as a `str`.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for LseSlug {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for LseSlug {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Symbol stems whose slug is shared by more than one distinct series.
///
/// The first eleven are Eurex futures that publish **both** a bare and a `.F` series holding
/// different data — different tick counts over different date ranges — which collapse onto one
/// slug, with the dataset-info endpoint answering for the `.F` series and the bare series having
/// no reachable slug at all. The last two are futures whose stripped symbol collides with an
/// unrelated equity ticker (`ES.F` with Eversource, `SI.F` with an unrelated listing), where the
/// endpoint answers with the future.
///
/// Both spellings of each stem are ambiguous: whichever the caller asked for, the slug identifies
/// the other series just as plausibly.
const AMBIGUOUS_SLUG_STEMS: [&str; 13] = [
    "fbtp", "fdax", "fdxm", "fesx", "fgbl", "fgbm", "fgbs", "fmeu", "fmwo", "foat", "fsmi", "es",
    "si",
];

/// Suffixes that look like a venue suffix but are not, and therefore must not select a quote
/// currency.
///
/// `A` and `B` are US **share classes** (`BRK.B`, `BF.A`, `HEI.A`); `F` marks a continuous futures
/// proxy. Listing them is documentation — [`quote_asset`] defaults to USD for any unrecognised
/// suffix — but the naive `.`-split venue parse they defeat is a real trap, so they are named.
///
/// Test-only because the default arm already handles them: nothing in the parse needs to consult
/// this list, only the test that pins the behaviour it describes.
#[cfg(test)]
const NON_VENUE_SUFFIXES: [&str; 3] = ["A", "B", "F"];

/// Returns the quote asset for a London Strategic Edge display symbol.
///
/// Pair-shaped symbols (`EUR/USD`, `XAU/USD`, `SPX500/USD`) state their quote directly. Bare
/// tickers do not, so the quote is derived from the venue suffix, defaulting to USD.
///
/// # ⚠️ `.L` symbols are quoted in PENCE, not pounds
/// London listings are quoted in pence and the provider's catalog reports no unit, so `.L` symbols
/// map to **GBX** (penny sterling), a *distinct asset from GBP*. This is not cosmetic: `BP.L`
/// prints ~548 where BP trades around £5.48, so quoting these in GBP would inflate notional, fees,
/// unrealised PnL and every balance by 100×, silently. Prices are passed through exactly as the
/// provider reports them — in pence — and it is the asset that carries the scale.
///
/// Note that GBX must be spelled distinctly rather than cased differently (`GBp`): asset names are
/// lowercased internally, so `GBp` and `GBP` would be one and the same asset.
///
/// # Venue suffixes
/// `.L` → GBX, `.T` → JPY, `.HK` → HKD, `.NS` → INR, `.AX` → AUD, `.KS` → KRW, `.TW` → TWD.
/// Everything else — including the share-class suffixes `.A`/`.B` and the continuous-futures `.F`
/// — is USD. Suffix matching is **case-insensitive**, so `bp.l` and `BP.L` agree; defaulting a
/// mis-cased London ticker to USD would be the 100× error above. The suffix is *not* stripped from
/// the base asset; see [`underlying`].
///
/// Currency is deliberately **not** derived from the catalog's `country` field, which records
/// issuer domicile rather than listing venue: Bermuda- and Ireland-domiciled names are USD-quoted,
/// and some entries carry no country at all.
pub fn quote_asset(symbol: &str) -> AssetNameExchange {
    if let Some((_, quote)) = symbol.split_once('/') {
        return AssetNameExchange::new(quote.to_uppercase_smolstr());
    }

    // The suffix is case-normalised before matching. Matching the literals directly would send a
    // lowercase `bp.l` to the USD default -- silently, and with exactly the 100x consequence the
    // warning above describes. Venue suffixes are at most two ASCII characters, so the uppercased
    // copy stays inline in the `SmolStr` and does not allocate.
    let quote = match symbol.rsplit_once('.') {
        Some((_, suffix)) => match suffix.to_uppercase_smolstr().as_str() {
            "L" => "GBX",
            "T" => "JPY",
            "HK" => "HKD",
            "NS" => "INR",
            "AX" => "AUD",
            "KS" => "KRW",
            "TW" => "TWD",
            // Unrecognised suffix or share class.
            _ => "USD",
        },
        // No suffix at all.
        None => "USD",
    };

    AssetNameExchange::new(quote)
}

/// Splits a London Strategic Edge display symbol into its base and quote assets.
///
/// Pair-shaped symbols split at the `/`. Bare tickers take the whole symbol as the base —
/// **including any venue suffix** — and derive the quote via [`quote_asset`].
///
/// The suffix is retained deliberately: stripping it would merge a London listing and a
/// same-ticker US listing into one asset on one exchange, despite different currencies and
/// different economics.
pub fn underlying(symbol: &str) -> Underlying<AssetNameExchange> {
    let quote = quote_asset(symbol);

    let base = match symbol.split_once('/') {
        Some((base, _)) => base,
        None => symbol,
    };

    Underlying::new(AssetNameExchange::new(base.to_uppercase_smolstr()), quote)
}

/// Resolve the [`InstrumentIndex`] a display symbol was registered under.
///
/// # Why derive it rather than accept one
/// [`InstrumentIndex`] is a public, unbounded `usize`, and engine state indexes positionally — a
/// fabricated index attributes a symbol's prices to a different instrument, or panics. Deriving it
/// from the registry the engine was actually built with makes both unrepresentable, and catches the
/// typo (`BP` for `BP.L`) that would otherwise yield an instrument that silently never marks.
///
/// # The quote asset is checked here too
/// This is where the provider's view of a symbol and the caller's registry meet, so it is where a
/// disagreement between them is visible. The provider publishes no unit alongside a price, which
/// makes the registered asset *the* unit: a `.L` listing registered against `gbp` rather than the
/// `gbx` [`quote_asset`] derives for it books pence as pounds, and every notional, fee, PnL and
/// balance downstream is 100× wrong with nothing to object. See [`quote_asset`] for why London
/// listings are a distinct asset rather than a scaled one.
///
/// The instrument's *pricing* asset is what is compared — [`InstrumentQuoteAsset::UnderlyingBase`]
/// selects the base — because that is the one the provider's number is denominated in.
///
/// # This is the ingest path's obligation, and the check reaches only its callers
/// [`LseCandleSource::resolve`](super::backtest::LseCandleSource::resolve) performs it on the
/// candle-replay path. On the export path `parquet::read_export` does **not**: it takes its
/// `instrument` argument on trust, and callers should obtain that argument here. Constructing an
/// [`InstrumentIndex`] by hand and passing it to either bypasses this
/// — [`LseCandleSource::new`](super::backtest::LseCandleSource::new) exists for callers who have no
/// registry to check against, and carries no protection.
///
/// It lives in this module rather than in `parquet` because it is registry symbology, not artifact
/// decoding: the candle path needs it and is **not** gated behind `lse-parquet`, so a consumer
/// building with `--features lse` alone could not otherwise reach it. Those two references are
/// deliberately not intra-doc links for the same reason — the item they name is absent from that
/// build.
///
/// # Errors
/// - [`LseError::UnknownInstrument`] if no instrument on `exchange` carries `symbol` as its
///   exchange-side name, listing what is registered there.
/// - [`LseError::QuoteAssetMismatch`] if it does, but prices it in a different asset than the
///   provider quotes that symbol in.
pub fn instrument_index_for(
    instruments: &IndexedInstruments,
    exchange: ExchangeId,
    symbol: &str,
) -> Result<InstrumentIndex, LseError> {
    let wanted = InstrumentNameExchange::new(symbol);

    let instrument = instruments
        .instruments()
        .iter()
        .find(|keyed| keyed.value.exchange.value == exchange && keyed.value.name_exchange == wanted)
        .ok_or_else(|| LseError::UnknownInstrument {
            symbol: symbol.to_owned(),
            exchange,
            registered: instruments
                .instruments()
                .iter()
                .filter(|keyed| keyed.value.exchange.value == exchange)
                .map(|keyed| keyed.value.name_exchange.to_string())
                .collect::<Vec<_>>()
                .join(", "),
        })?;

    let pricing_asset = match instrument.value.quote {
        InstrumentQuoteAsset::UnderlyingQuote => instrument.value.underlying.quote,
        InstrumentQuoteAsset::UnderlyingBase => instrument.value.underlying.base,
    };

    // An `AssetIndex` held by an indexed instrument was issued by this same registry, so the
    // lookup cannot miss; treating a miss as a mismatch would report the wrong problem, so it is
    // mapped to the registry's own error rather than folded in.
    let registered =
        instruments
            .find_asset(pricing_asset)
            .map_err(|error| LseError::UnknownInstrument {
                symbol: symbol.to_owned(),
                exchange,
                registered: error.to_string(),
            })?;

    // Compared on `name_internal`: that is the identity the engine keys assets by, so `GBX` and
    // `gbx` are one asset and `GBP` is a different one — which is exactly the distinction that
    // matters here.
    let expected = AssetNameInternal::new(quote_asset(symbol).name().clone());
    if registered.asset.name_internal != expected {
        return Err(LseError::QuoteAssetMismatch {
            symbol: symbol.to_owned(),
            exchange,
            expected: expected.to_string(),
            registered: registered.asset.name_internal.to_string(),
        });
    }

    Ok(instrument.key)
}

/// Derives the dataset slug for a London Strategic Edge display symbol.
///
/// The slug is the path key of the provider's dataset-info endpoint. It is a **discovery helper,
/// not an instrument identity** — candle endpoints key on the display symbol, so no data fetch
/// needs one.
///
/// The transformation is: lowercase, `/` → `_`, and drop a trailing `.F`.
///
/// # Errors
/// Returns [`LseError::AmbiguousSlug`] when more than one distinct series resolves to the derived
/// slug. This is the case the signature exists for: the dataset-info endpoint answers `200` for an
/// ambiguous slug, silently serving whichever series it prefers, so a plain string transformation
/// would return the wrong dataset with no error. Thirteen stems are ambiguous: eleven Eurex
/// futures that publish **both** a bare and a `.F` series holding different data, which collapse
/// onto one slug; plus `ES.F` and `SI.F`, whose stripped symbols collide with unrelated equity
/// tickers. Both spellings of each are rejected — whichever the caller asked for, the slug
/// identifies the other series just as plausibly.
///
/// # Caller obligation
/// A derived slug is not verified to exist. The `.F` rule is measured; the behaviour of other
/// dotted suffixes (`.L`, `.T`, `.A`, …) is not, and they are carried through verbatim. A slug
/// that does not exist fails observably as a `404` — unlike the ambiguous case above, which is why
/// only ambiguity is an error here.
pub fn slug(symbol: &str) -> Result<LseSlug, LseError> {
    // Case-normalise *before* stripping, in the order the doc states. Stripping first makes the
    // strip an exact-literal match that a lowercase `.f` survives, leaving a stem of `fbtp.f`
    // that matches nothing in `AMBIGUOUS_SLUG_STEMS` -- so the one input the ambiguity check
    // exists for is the one that walks past it.
    let normalised = symbol.replace('/', "_").to_lowercase_smolstr();
    let slug = normalised.strip_suffix(".f").unwrap_or(&normalised);

    if AMBIGUOUS_SLUG_STEMS.contains(&slug) {
        return Err(LseError::AmbiguousSlug {
            symbol: symbol.to_string(),
            slug: slug.to_string(),
        });
    }

    Ok(LseSlug(SmolStr::new(slug)))
}

/// Returns the provider's spelling of `interval`, or `None` if it does not serve that resolution.
///
/// [`CandleInterval`] is the venue-agnostic union of every resolution any connector in this crate
/// serves. This provider serves **14 of the 19**: it publishes no `2h`, `6h`, `8h`, `12h` or `3d`.
///
/// # ⚠️ The month spelling differs from the shared enum
/// The provider spells one month **`1mo`**, where [`CandleInterval::as_str`] — which follows
/// Binance's kline convention — spells it `1M`. Sending `1M` is not a silent mismatch (it is
/// rejected with a `400`), but the two must not be assumed interchangeable.
///
/// The match is exhaustive rather than defaulting, so adding a [`CandleInterval`] variant forces a
/// decision here instead of silently reporting it unsupported.
#[must_use]
pub fn candle_interval_str(interval: CandleInterval) -> Option<&'static str> {
    match interval {
        CandleInterval::Sec1 => Some("1s"),
        CandleInterval::Sec5 => Some("5s"),
        CandleInterval::Sec15 => Some("15s"),
        CandleInterval::Sec30 => Some("30s"),
        CandleInterval::Min1 => Some("1m"),
        CandleInterval::Min3 => Some("3m"),
        CandleInterval::Min5 => Some("5m"),
        CandleInterval::Min15 => Some("15m"),
        CandleInterval::Min30 => Some("30m"),
        CandleInterval::Hour1 => Some("1h"),
        CandleInterval::Hour4 => Some("4h"),
        CandleInterval::Day1 => Some("1d"),
        CandleInterval::Week1 => Some("1w"),
        // Note the spelling: `1mo`, not the shared enum's `1M`.
        CandleInterval::Month1 => Some("1mo"),
        // Not published by this provider.
        CandleInterval::Hour2
        | CandleInterval::Hour6
        | CandleInterval::Hour8
        | CandleInterval::Hour12
        | CandleInterval::Day3 => None,
    }
}

/// Whether the provider serves candles at `interval`.
#[must_use]
pub fn supports_candle_interval(interval: CandleInterval) -> bool {
    candle_interval_str(interval).is_some()
}

/// How a dataset spells the display symbol the WebSocket subscribes on.
///
/// The provider uses two spellings and each dataset uses exactly one, which is what the per-dataset
/// [`ExchangeId`] split buys at this boundary: the shape is known statically from the connector
/// type, so no runtime discriminator is needed to reconstruct a symbol.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum LseSymbolShape {
    /// `BASE/QUOTE` — FX, crypto and CFDs: `EUR/USD`, `BTC/USD`, `SPX500/USD`, `VIX/USD`.
    Pair,
    /// A bare ticker, venue suffix included — equities and futures: `AAPL`, `BP.L`, `ES.F`.
    Bare,
}

/// A London Strategic Edge WebSocket server, and the symbol spelling its dataset uses.
///
/// Implemented by each `LseServer*` marker type. It exists so the symbol reconstruction below can
/// be written once per instrument representation rather than once per representation *per server*,
/// and so each dataset's spelling is declared in exactly one place and cannot drift from it.
pub trait LseServer: crate::exchange::ExchangeServer {
    /// The spelling this dataset's display symbols take.
    const SYMBOL_SHAPE: LseSymbolShape;
}

/// A London Strategic Edge display symbol, as the WebSocket expects it.
///
/// This is the provider's own display symbol — the same key its REST vault serves candles on, not
/// a slug. See [`slug`] for why the two are not interchangeable.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct LseMarket(pub SmolStr);

impl AsRef<str> for LseMarket {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for LseMarket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Reconstruct a display symbol from a registered instrument's assets.
///
/// The provider normalises case server-side (`eur/usd` is confirmed as `EUR/USD`), but the symbol
/// is also the key of the instrument map a tick is resolved through, so it is uppercased here to
/// match what the server will echo back on the tick.
///
/// The base asset's venue suffix is deliberately *not* stripped — see [`underlying`], which retains
/// it for the same reason: `BP.L` and `BP` are different instruments in different currencies.
fn market_symbol(
    shape: LseSymbolShape,
    base: &AssetNameInternal,
    quote: &AssetNameInternal,
) -> LseMarket {
    match shape {
        LseSymbolShape::Pair => LseMarket(format_smolstr!(
            "{}/{}",
            base.name().to_uppercase_smolstr(),
            quote.name().to_uppercase_smolstr()
        )),
        LseSymbolShape::Bare => LseMarket(base.name().to_uppercase_smolstr()),
    }
}

impl<Server, Kind> Identifier<LseMarket> for Subscription<Lse<Server>, MarketDataInstrument, Kind>
where
    Server: LseServer,
{
    fn id(&self) -> LseMarket {
        market_symbol(
            Server::SYMBOL_SHAPE,
            &self.instrument.base,
            &self.instrument.quote,
        )
    }
}

impl<Server, InstrumentKey, Kind> Identifier<LseMarket>
    for Subscription<Lse<Server>, Keyed<InstrumentKey, MarketDataInstrument>, Kind>
where
    Server: LseServer,
{
    fn id(&self) -> LseMarket {
        market_symbol(
            Server::SYMBOL_SHAPE,
            &self.instrument.value.base,
            &self.instrument.value.quote,
        )
    }
}

/// Takes the exchange-side name as given, so it needs no per-dataset shape: the caller has already
/// supplied the provider's own symbol, and there is nothing to reconstruct.
///
/// Case is still normalised, for the same reason the reconstruction path normalises it: the symbol
/// is the instrument-map key a tick resolves through, and the provider echoes `EUR/USD` on the tick
/// however the subscribe spelled it. A lowercase `name_exchange` would be *confirmed*, then tick
/// under a key the map does not hold and deliver nothing — the silent failure the reconstruction
/// path already avoids, and there is no reason for this path to keep it. On a correctly spelled
/// exchange name the call is a no-op.
impl<Server, InstrumentKey, Kind> Identifier<LseMarket>
    for Subscription<Lse<Server>, MarketInstrumentData<InstrumentKey>, Kind>
{
    fn id(&self) -> LseMarket {
        LseMarket(self.instrument.name_exchange.name().to_uppercase_smolstr())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;

    #[test]
    fn test_quote_asset_pair_shaped_symbols() {
        for (symbol, expected) in [
            ("EUR/USD", "USD"),
            ("BTC/USD", "USD"),
            ("XAU/USD", "USD"),
            ("SPX500/USD", "USD"),
            ("EUR/GBP", "GBP"),
        ] {
            assert_eq!(quote_asset(symbol), AssetNameExchange::new(expected));
        }
    }

    #[test]
    fn test_quote_asset_venue_suffixes() {
        for (symbol, expected) in [
            ("BP.L", "GBX"),
            ("7203.T", "JPY"),
            ("0700.HK", "HKD"),
            ("INFY.NS", "INR"),
            ("BHP.AX", "AUD"),
            ("005930.KS", "KRW"),
            ("2330.TW", "TWD"),
        ] {
            assert_eq!(quote_asset(symbol), AssetNameExchange::new(expected));
        }
    }

    #[test]
    fn test_quote_asset_london_listings_are_pence_not_pounds() {
        // The 100x trap. GBX must be a genuinely distinct name from GBP -- asset names are
        // lowercased internally, so a `GBp`/`GBP` casing distinction would collapse to one asset.
        //
        // Every casing must reach GBX: falling through to the USD default is not a near miss but
        // the full 100x error, since GBX prices are pence and USD/GBP prices are units.
        for symbol in ["BP.L", "bp.l", "BP.l", "bp.L"] {
            let gbx = quote_asset(symbol);
            assert_eq!(gbx, AssetNameExchange::new("GBX"), "{symbol}");
            assert_ne!(gbx, AssetNameExchange::new("GBP"), "{symbol}");
            assert_ne!(gbx.name().to_lowercase_smolstr(), "gbp", "{symbol}");
        }
    }

    #[test]
    fn test_quote_asset_venue_suffixes_are_case_insensitive() {
        // The suffix arms are literals, so every venue -- not just `.L` -- would default to USD on
        // a lowercase spelling. Each of these is a currency error, not a cosmetic one.
        for (symbol, expected) in [
            ("7203.t", "JPY"),
            ("0700.hk", "HKD"),
            ("RELIANCE.ns", "INR"),
            ("BHP.ax", "AUD"),
            ("005930.ks", "KRW"),
            ("2330.tw", "TWD"),
        ] {
            assert_eq!(
                quote_asset(symbol),
                AssetNameExchange::new(expected),
                "{symbol} must resolve its venue regardless of case"
            );
        }
    }

    #[test]
    fn test_quote_asset_share_classes_are_not_venues() {
        // `.A` and `.B` are US share classes. A naive `.`-split venue parse maps `BRK.B` to a
        // venue that does not exist; these must fall through to USD.
        for symbol in ["BRK.B", "BF.A", "HEI.A"] {
            assert_eq!(quote_asset(symbol), AssetNameExchange::new("USD"));
        }

        for suffix in NON_VENUE_SUFFIXES {
            assert_eq!(
                quote_asset(&format!("XYZ.{suffix}")),
                AssetNameExchange::new("USD"),
                "{suffix} must not select a quote currency"
            );
        }
    }

    #[test]
    fn test_quote_asset_bare_tickers_default_to_usd() {
        for symbol in ["AAPL", "MSFT", "SPY", "ES.F"] {
            assert_eq!(quote_asset(symbol), AssetNameExchange::new("USD"));
        }
    }

    #[test]
    fn test_underlying_retains_the_venue_suffix() {
        // Stripping `.L` would merge the London listing with a same-ticker US listing into one
        // asset on one exchange, despite the two being quoted in different currencies.
        let london = underlying("BP.L");
        assert_eq!(london.base, AssetNameExchange::new("BP.L"));
        assert_eq!(london.quote, AssetNameExchange::new("GBX"));

        let us = underlying("BP");
        assert_eq!(us.base, AssetNameExchange::new("BP"));
        assert_eq!(us.quote, AssetNameExchange::new("USD"));

        assert_ne!(london.base, us.base);
    }

    #[test]
    fn test_underlying_pair_shaped_symbols() {
        let fx = underlying("EUR/USD");
        assert_eq!(fx.base, AssetNameExchange::new("EUR"));
        assert_eq!(fx.quote, AssetNameExchange::new("USD"));

        let cfd = underlying("SPX500/USD");
        assert_eq!(cfd.base, AssetNameExchange::new("SPX500"));
        assert_eq!(cfd.quote, AssetNameExchange::new("USD"));
    }

    #[test]
    fn test_slug_transformation() {
        for (symbol, expected) in [
            ("AAPL", "aapl"),
            ("EUR/USD", "eur_usd"),
            ("SPX500/USD", "spx500_usd"),
            ("NQ.F", "nq"),
            // The `.F` strip is part of the case-normalised transformation, not a literal match.
            ("NQ.f", "nq"),
            ("nq.f", "nq"),
        ] {
            assert_eq!(slug(symbol).unwrap().as_str(), expected, "{symbol}");
        }
    }

    #[test]
    fn test_slug_rejects_ambiguous_symbols() {
        // Written out independently of the constant rather than iterated from it: a test that reads
        // the list it is checking would still pass if a stem were accidentally deleted, which is the
        // one regression that matters here -- a dropped stem silently re-admits a symbol whose slug
        // resolves to the wrong series.
        let expected = [
            "fbtp", "fdax", "fdxm", "fesx", "fgbl", "fgbm", "fgbs", "fmeu", "fmwo", "foat", "fsmi",
            "es", "si",
        ];
        assert_eq!(
            AMBIGUOUS_SLUG_STEMS, expected,
            "the ambiguous-stem list changed; confirm the new set against the provider's catalog"
        );

        // Both spellings must be rejected: the bare and `.F` series are different data, and the
        // endpoint answers 200 for the slug regardless of which one the caller meant.
        for stem in expected {
            for symbol in [stem.to_uppercase(), format!("{}.F", stem.to_uppercase())] {
                let error = slug(&symbol).unwrap_err();
                assert!(
                    matches!(error, LseError::AmbiguousSlug { .. }),
                    "{symbol} should be ambiguous, got {error:?}"
                );
            }
        }
    }

    #[test]
    fn test_slug_ambiguity_is_case_insensitive() {
        // All four casings of the suffixed spelling, not just the canonical one. A lowercase `.f`
        // is the spelling an exact-literal strip lets through -- it leaves the stem as `fbtp.f`,
        // which matches no entry in the ambiguous list and so is silently accepted.
        for symbol in ["fbtp.F", "FBTP.F", "fbtp.f", "FBTP.f", "FbTp.F"] {
            let error = slug(symbol).unwrap_err();
            assert!(
                matches!(error, LseError::AmbiguousSlug { .. }),
                "{symbol} should be ambiguous, got {error:?}"
            );
        }

        // The bare spelling too, in every casing.
        for symbol in ["fbtp", "FBTP", "FbTp"] {
            assert!(
                matches!(slug(symbol).unwrap_err(), LseError::AmbiguousSlug { .. }),
                "{symbol} should be ambiguous"
            );
        }
    }

    #[test]
    fn test_dataset_exchange_and_kind_mapping() {
        use LseDataset::*;

        for (dataset, exchange, kind) in [
            (
                Stocks,
                ExchangeId::LseEquities,
                MarketDataInstrumentKind::Spot,
            ),
            (Etf, ExchangeId::LseEquities, MarketDataInstrumentKind::Spot),
            (
                Crypto,
                ExchangeId::LseCrypto,
                MarketDataInstrumentKind::Spot,
            ),
            (Fx, ExchangeId::LseFx, MarketDataInstrumentKind::Spot),
            (Index, ExchangeId::LseCfd, MarketDataInstrumentKind::Cfd),
            (Commodity, ExchangeId::LseCfd, MarketDataInstrumentKind::Cfd),
            (
                InterestRates,
                ExchangeId::LseCfd,
                MarketDataInstrumentKind::Cfd,
            ),
            (
                CurrencyIndex,
                ExchangeId::LseCfd,
                MarketDataInstrumentKind::Cfd,
            ),
            (
                Volatility,
                ExchangeId::LseCfd,
                MarketDataInstrumentKind::Cfd,
            ),
            // Futures are CFDs, not `Future`: continuous proxies with no contract chain or expiry.
            (
                Futures,
                ExchangeId::LseFutures,
                MarketDataInstrumentKind::Cfd,
            ),
        ] {
            assert_eq!(dataset.exchange_id(), exchange, "{dataset:?}");
            assert_eq!(dataset.market_data_kind(), kind, "{dataset:?}");
        }
    }

    #[test]
    fn test_dataset_catalog_str_roundtrip() {
        for dataset in LseDataset::ALL {
            assert_eq!(
                LseDataset::from_catalog_str(dataset.as_catalog_str()).unwrap(),
                dataset
            );
        }

        // Reference datasets are not instruments and must not parse as price datasets.
        for reference in ["bonds", "sovereign_yields", "economics", "credit_indices"] {
            assert!(matches!(
                LseDataset::from_catalog_str(reference).unwrap_err(),
                LseError::UnknownDataset(_)
            ));
        }
    }

    /// The resolutions the provider itself enumerates when it rejects an invalid `timeframe`:
    ///
    /// ```text
    /// invalid timeframe '7q'; valid: 1s, 5s, 15s, 30s, 1m, 3m, 5m, 15m, 30m, 1h, 4h, 1d, 1w, 1mo
    /// ```
    const PROVIDER_ADVERTISED_TIMEFRAMES: [&str; 14] = [
        "1s", "5s", "15s", "30s", "1m", "3m", "5m", "15m", "30m", "1h", "4h", "1d", "1w", "1mo",
    ];

    #[test]
    fn test_supported_intervals_match_the_providers_own_list() {
        // Pins the mapping to the provider's advertised set in both directions, so neither a new
        // `CandleInterval` variant nor a typo can drift it silently.
        let mapped = CandleInterval::ALL
            .into_iter()
            .filter_map(candle_interval_str)
            .collect::<Vec<_>>();

        assert_eq!(mapped, PROVIDER_ADVERTISED_TIMEFRAMES);
    }

    #[test]
    fn test_month_is_spelled_differently_from_the_shared_enum() {
        // The shared enum follows Binance's kline convention (`1M`); this provider wants `1mo`.
        assert_eq!(candle_interval_str(CandleInterval::Month1), Some("1mo"));
        assert_eq!(CandleInterval::Month1.as_str(), "1M");
    }

    #[test]
    fn test_unserved_resolutions_are_reported_unsupported() {
        for interval in [
            CandleInterval::Hour2,
            CandleInterval::Hour6,
            CandleInterval::Hour8,
            CandleInterval::Hour12,
            CandleInterval::Day3,
        ] {
            assert!(
                !supports_candle_interval(interval),
                "{interval} is not published by this provider"
            );
        }
    }

    #[test]
    fn test_sub_minute_resolutions_are_supported() {
        // These have no Binance kline equivalent and exist in the shared enum largely for this
        // provider, so a regression here would be quiet.
        for interval in [
            CandleInterval::Sec1,
            CandleInterval::Sec5,
            CandleInterval::Sec15,
            CandleInterval::Sec30,
        ] {
            assert!(supports_candle_interval(interval), "{interval}");
        }
    }

    #[test]
    fn test_every_dataset_maps_to_a_declared_support_arm() {
        // Each dataset's (exchange, kind) pair must be declared supported, or subscriptions built
        // from this mapping would be rejected at validation.
        for dataset in LseDataset::ALL {
            assert!(
                crate::subscription::exchange_supports_instrument_kind(
                    dataset.exchange_id(),
                    &dataset.market_data_kind()
                ),
                "{dataset:?} maps to an unsupported (exchange, kind) pair"
            );
        }
    }

    mod market_symbol_reconstruction {
        use super::*;
        use crate::exchange::lse::{
            LseCfd, LseServerCfd, LseServerCrypto, LseServerEquities, LseServerFutures, LseServerFx,
        };
        use crate::subscription::trade::PublicTrades;

        /// Build a subscription for `Server` and resolve it to the symbol it would subscribe on.
        ///
        /// The return type is what selects `Identifier<LseMarket>` over the `Identifier<LseChannel>`
        /// impl the same subscription also carries.
        fn market_of<Server>(base: &str, quote: &str, kind: MarketDataInstrumentKind) -> LseMarket
        where
            Server: LseServer,
        {
            let subscription: Subscription<Lse<Server>, MarketDataInstrument, PublicTrades> =
                Subscription::from((Lse::<Server>::default(), base, quote, kind, PublicTrades));

            subscription.id()
        }

        #[test]
        fn pair_shaped_datasets_reconstruct_base_slash_quote() {
            assert_eq!(
                market_of::<LseServerFx>("eur", "usd", MarketDataInstrumentKind::Spot).as_ref(),
                "EUR/USD"
            );
            assert_eq!(
                market_of::<LseServerCrypto>("btc", "usd", MarketDataInstrumentKind::Spot).as_ref(),
                "BTC/USD"
            );
            assert_eq!(
                market_of::<LseServerCfd>("spx500", "usd", MarketDataInstrumentKind::Cfd).as_ref(),
                "SPX500/USD"
            );
        }

        #[test]
        fn bare_shaped_datasets_reconstruct_the_ticker_alone() {
            assert_eq!(
                market_of::<LseServerEquities>("aapl", "usd", MarketDataInstrumentKind::Spot)
                    .as_ref(),
                "AAPL"
            );
        }

        /// The suffix is part of the base asset, and dropping it would subscribe to the US listing
        /// while the engine marks the London one — a different instrument in a different currency.
        #[test]
        fn a_venue_suffix_survives_symbol_reconstruction() {
            assert_eq!(
                market_of::<LseServerEquities>("bp.l", "gbx", MarketDataInstrumentKind::Spot)
                    .as_ref(),
                "BP.L"
            );
            assert_eq!(
                market_of::<LseServerFutures>("es.f", "usd", MarketDataInstrumentKind::Cfd)
                    .as_ref(),
                "ES.F"
            );
        }

        /// The reconstructed symbol is the instrument-map key a tick is resolved through, so it
        /// must match the casing the provider echoes back rather than the caller's spelling.
        #[test]
        fn reconstruction_uppercases_however_the_instrument_was_registered() {
            assert_eq!(
                market_of::<LseServerFx>("EuR", "uSd", MarketDataInstrumentKind::Spot).as_ref(),
                "EUR/USD"
            );
        }

        /// Reconstruction must invert the split the rest of the integration derives assets with,
        /// or the WebSocket and the REST vault would disagree about what one instrument is called.
        ///
        /// The shape comes from the dataset's own [`LseServer::SYMBOL_SHAPE`] rather than being
        /// inferred from the symbol. Inferring it is what [`underlying`] already does, so deriving
        /// both halves the same way would make the round trip agree with itself no matter what the
        /// connectors declare — and a connector declaring the wrong shape is precisely the failure
        /// that produces an unsubscribable symbol, since [`market_symbol`] dispatches on the
        /// declaration and not on the string.
        #[test]
        fn reconstruction_inverts_the_underlying_split() {
            fn round_trips<Server>(symbol: &str)
            where
                Server: LseServer,
            {
                let split = underlying(symbol);

                let rebuilt = market_symbol(
                    Server::SYMBOL_SHAPE,
                    &AssetNameInternal::new(split.base.name().clone()),
                    &AssetNameInternal::new(split.quote.name().clone()),
                );

                assert_eq!(
                    rebuilt.as_ref(),
                    symbol,
                    "{symbol} does not survive a round trip through {:?}, the shape its dataset \
                     declares",
                    Server::SYMBOL_SHAPE,
                );
            }

            round_trips::<LseServerFx>("EUR/USD");
            round_trips::<LseServerCrypto>("BTC/USD");
            round_trips::<LseServerCfd>("VIX/USD");
            round_trips::<LseServerEquities>("AAPL");
            round_trips::<LseServerEquities>("BP.L");
            round_trips::<LseServerFutures>("ES.F");
        }

        /// An exchange-side name is the provider's own symbol, so it is used as given rather than
        /// rebuilt through the dataset's shape.
        #[test]
        fn an_exchange_named_instrument_is_taken_verbatim() {
            assert_eq!(
                market_of_exchange_named("XAU/USD").as_ref(),
                "XAU/USD",
                "a correctly spelled exchange name must pass through untouched",
            );
        }

        /// The one normalisation that still applies, and why: the symbol is the instrument-map key
        /// a tick resolves through, and the provider echoes the uppercase spelling on the tick
        /// whatever the subscribe said. Left alone, a lowercase name would be confirmed and then
        /// tick under a key the map does not hold.
        #[test]
        fn an_exchange_named_instrument_is_uppercased_like_a_reconstructed_one() {
            assert_eq!(market_of_exchange_named("xau/usd").as_ref(), "XAU/USD");
            assert_eq!(market_of_exchange_named("bp.l").as_ref(), "BP.L");
        }

        fn market_of_exchange_named(name_exchange: &str) -> LseMarket {
            let subscription = Subscription {
                exchange: LseCfd::default(),
                instrument: MarketInstrumentData {
                    key: 0_usize,
                    name_exchange: InstrumentNameExchange::new(name_exchange),
                    kind: MarketDataInstrumentKind::Cfd,
                },
                kind: PublicTrades,
            };

            subscription.id()
        }
    }
}
