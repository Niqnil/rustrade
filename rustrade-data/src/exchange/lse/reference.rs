//! London Strategic Edge catalog records (`GET /vault/catalog`).
//!
//! The catalog is the provider's index of everything it publishes: 22,966 entries at the last
//! measurement, spanning the price datasets that [`LseDataset`] models, the reference families
//! (economics, bond yields, credit indices, …) that it deliberately does not, and `options`, which
//! is a price dataset that has no [`LseDataset`] variant either — see
//! [`LseCatalogEntry::price_dataset`].
//!
//! [`LseCatalogEntry`] is the record **exactly as the vault serves it** — a provider-shaped type in
//! the same spirit as `AlpacaStockSplit`, not a provider-agnostic abstraction. Nothing here is
//! wired into the engine: reference series are `live == 0` across the board and reach a backtest,
//! if at all, through an auxiliary event source rather than through `price()` or orders.
//!
//! # Classifying an entry
//!
//! [`LseCatalogEntry::class`] separates price datasets from reference series using the provider's
//! own fields, and the rule is not a heuristic: across all 22,966 entries, a populated `frequency`
//! **and** `category` identifies a reference series and an empty pair identifies a price dataset,
//! with **zero rows falling outside either bucket** (15,537 reference / 7,429 price). A new
//! dataset the provider adds tomorrow therefore classifies itself, with no list here to update.
//!
//! # ⚠️ `frequency` is a label, not a cadence contract
//!
//! It is kept as the provider's own string rather than parsed into an enum, for two measured
//! reasons. The vocabulary is **dirty**: ten distinct spellings including `biannually` *and*
//! `bi-annually`, `quarterly` *and* `quarter` — so a closed enum built from the obvious six values
//! would have silently mishandled seven rows. And the label **does not predict observed spacing**:
//! `weekly` series range from 52 to 248 observations per year, and `quarter` yields roughly 1.5,
//! not 4. Derive expected spacing from the observations themselves, never from this field.
//!
//! # ⚠️ Licensing — this data may not be redistributed
//!
//! Catalog contents are provider data. They may be used for your own research, trading and model
//! training, including commercially, but **not** redistributed or re-served to third parties in any
//! form. Do not commit catalog responses as fixtures or example datasets. Terms:
//! <https://londonstrategicedge.com/terms>

use crate::exchange::lse::PROVIDER_TIMESTAMP_FORMAT;
use crate::exchange::lse::error::LseError;
use crate::exchange::lse::market::LseDataset;
use crate::exchange::lse::vault::LseVaultClient;
use chrono::{DateTime, NaiveDateTime, Utc};
use serde::Deserialize;
use smol_str::SmolStr;

/// Path of the catalog endpoint, relative to the vault base URL.
const CATALOG_PATH: &str = "catalog";

/// Whether a catalog entry describes a tradeable price dataset or a reference series.
///
/// See the [module documentation](self) for the classification rule and the measurement behind it.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum LseDatasetClass {
    /// A price dataset — an instrument, modelled by [`LseDataset`].
    Price,
    /// A reference series — macro, bond-yield, credit or derivative reference data. Not an
    /// instrument, and never streamed: every reference entry measured carries `live == 0`.
    Reference,
}

/// One catalog record, as the vault serves it.
///
/// All seventeen fields were present and non-null on every one of the 22,966 entries measured, so
/// none is optional. The six descriptive fields nonetheless carry `#[serde(default)]`: they are
/// routinely the empty string already (`unit` on 7,805 entries, `source` on 8,057), so an absent
/// field and an empty one mean the same thing here and tolerating absence hides nothing.
///
/// Unknown fields are ignored, so a field the provider adds does not break decoding.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct LseCatalogEntry {
    /// Dataset name as the catalog spells it (`"crypto"`, `"economics"`, …).
    ///
    /// Kept as the provider's string rather than as an [`LseDataset`], which models price datasets
    /// only. Use [`price_dataset`](Self::price_dataset) to resolve one where it exists.
    pub dataset: SmolStr,
    /// Symbol within the dataset (`"BTC/USD"`, `"chlinfind"`, …).
    ///
    /// Unique only together with [`dataset`](Self::dataset), not on its own.
    pub symbol: SmolStr,
    /// Human-readable series name (`"Bitcoin"`).
    pub name: String,
    /// Observation count the provider reports for the series.
    ///
    /// **`u64`, not `u32`, and that is not defensive.** The largest measured value is 4,145,394,353
    /// — 96.5% of [`u32::MAX`] — on a live crypto tape that keeps growing, so a 32-bit count would
    /// overflow within months of the measurement.
    pub ticks: u64,
    /// First observation, spelled `2017-08-17 04:00:28.322000` (UTC, no zone marker, the
    /// fractional part optional). Parse with [`first_tick_time`](Self::first_tick_time).
    pub first_tick: String,
    /// Last observation, spelled as [`first_tick`](Self::first_tick) is. Parse with
    /// [`last_tick_time`](Self::last_tick_time).
    ///
    /// ⚠️ This is a **catalog summary statistic and it lags the live tape** — measured six hours
    /// behind on a continuously trading crypto symbol. It describes the catalog's own refresh, not
    /// feed liveness, and must not be used as a staleness check on a stream.
    pub last_tick: String,
    /// Span of the series in years, as the provider computes it.
    pub years: f64,
    /// Most recent value, in whatever [`unit`](Self::unit) the series is quoted in.
    ///
    /// A display statistic spanning nine orders of magnitude across the catalog
    /// (-20,458,587 to 74,226,775,000), carried as `f64` because that is the precision the provider
    /// serves it at. Never an execution input — fetch the series itself for that.
    pub last_value: f64,
    /// Most recent change, percent.
    pub change_pct: f64,
    /// Change over the trailing year, percent.
    pub change_1y: f64,
    /// Unit of quotation (`"percent"`, `"USD Million"`, `"index"`, …); empty for price datasets.
    #[serde(default)]
    pub unit: SmolStr,
    /// Upstream publisher. Populated only for `economics`, and **not a clean vocabulary** — two
    /// spellings of one source were measured (`"World Bank"` 728 entries, `"Worldbank"` 344).
    #[serde(default)]
    pub source: SmolStr,
    /// Reference category (`"Government bond yield"`, `"Credit index"`, …); empty for price
    /// datasets. Half of the [`class`](Self::class) rule.
    #[serde(default)]
    pub category: SmolStr,
    /// Publication frequency as the provider labels it; empty for price datasets. The other half of
    /// the [`class`](Self::class) rule.
    ///
    /// ⚠️ A dirty label that does not predict observed spacing — see the
    /// [module documentation](self) before relying on it.
    #[serde(default)]
    pub frequency: SmolStr,
    /// Country code.
    ///
    /// ⚠️ **The vault spells the United Kingdom `GB` here, while the `data-api` host's
    /// `/bond-yields` endpoint spells the same country `UK`** — the only divergence among the 34
    /// codes, and neither host errors on the other's spelling. Normalise before crossing hosts.
    #[serde(default)]
    pub country: SmolStr,
    /// Country name (`"United States"`).
    #[serde(default)]
    pub country_name: String,
    /// Whether the series streams: `1` for a live price dataset, `0` otherwise.
    ///
    /// Kept as the provider's integer rather than a `bool` so a third value would survive decoding
    /// instead of being coerced. Read it through [`is_live`](Self::is_live). Every one of the
    /// 15,537 reference entries measured carries `0`.
    pub live: u8,
}

impl LseCatalogEntry {
    /// Classifies the entry as a price dataset or a reference series.
    ///
    /// Uses the provider's own `frequency`/`category` pair; see the [module documentation](self)
    /// for the rule and the measurement behind it. An entry populating exactly one of the two —
    /// which no measured entry does — is reported as [`Reference`](LseDatasetClass::Reference),
    /// since a populated `category` is what distinguishes a non-instrument.
    #[must_use]
    pub fn class(&self) -> LseDatasetClass {
        if self.frequency.is_empty() && self.category.is_empty() {
            LseDatasetClass::Price
        } else {
            LseDatasetClass::Reference
        }
    }

    /// Resolves [`dataset`](Self::dataset) to an [`LseDataset`], where one exists.
    ///
    /// # `None` does not mean "reference"
    ///
    /// It means "no [`LseDataset`] variant", and the catalog has **two** sources of that:
    ///
    /// - every reference family, which is [`LseDataset::from_catalog_str`]'s documented contract
    ///   rather than a failure — those are not instruments and have no variant; and
    /// - **`options`**, which *is* a price dataset and still has no variant. Measured: of 7,429
    ///   price-classified entries, 4,243 resolve and the remaining 3,186 are all `options`.
    ///
    /// So pair this with [`class`](Self::class) rather than reading `None` as a classification.
    /// An entry with [`LseDatasetClass::Price`] and `None` here is an option contract.
    #[must_use]
    pub fn price_dataset(&self) -> Option<LseDataset> {
        LseDataset::from_catalog_str(&self.dataset).ok()
    }

    /// Whether the series streams live.
    #[must_use]
    pub fn is_live(&self) -> bool {
        self.live != 0
    }

    /// Parses [`first_tick`](Self::first_tick) as an instant.
    ///
    /// # Errors
    /// Returns [`LseError::Deserialize`] if the value is not in the provider's timestamp
    /// spelling, naming the field so the caller can tell which of the two failed.
    pub fn first_tick_time(&self) -> Result<DateTime<Utc>, LseError> {
        parse_catalog_timestamp(&self.first_tick, "first_tick")
    }

    /// Parses [`last_tick`](Self::last_tick) as an instant.
    ///
    /// ⚠️ Lags the live tape — see the field's own documentation.
    ///
    /// # Errors
    /// Returns [`LseError::Deserialize`] if the value is not in the provider's timestamp
    /// spelling, naming the field so the caller can tell which of the two failed.
    pub fn last_tick_time(&self) -> Result<DateTime<Utc>, LseError> {
        parse_catalog_timestamp(&self.last_tick, "last_tick")
    }
}

/// Parse one catalog timestamp, naming the field in any error.
fn parse_catalog_timestamp(raw: &str, field: &'static str) -> Result<DateTime<Utc>, LseError> {
    NaiveDateTime::parse_from_str(raw, PROVIDER_TIMESTAMP_FORMAT)
        .map(|naive| naive.and_utc())
        .map_err(|error| LseError::Deserialize {
            message: format!("invalid catalog {field} {raw:?}: {error}"),
        })
}

impl LseVaultClient {
    /// Fetches the whole catalog.
    ///
    /// The endpoint is unpaginated and returns every entry in one response — 22,966 at the last
    /// measurement, and **9.1 MB of JSON** — all of it buffered and decoded, so treat this as an
    /// occasional call rather than something to poll.
    ///
    /// It is a **discovery and validation helper**: it does not register instruments, which the
    /// caller declares explicitly as with every other connector.
    ///
    /// Timestamps are returned unparsed, so a single malformed `first_tick` surfaces through
    /// [`LseCatalogEntry::first_tick_time`] on that one entry instead of failing the whole
    /// fetch.
    ///
    /// # Errors
    /// Returns [`LseError::Api`] for a non-success status, [`LseError::RateLimited`] on a `429`,
    /// [`LseError::Http`] on a transport failure, and [`LseError::Deserialize`] if the response
    /// does not match [`LseCatalogEntry`].
    ///
    /// # ⚠️ Licensing
    /// The response is provider data and must not be redistributed or committed. See the
    /// [module documentation](self).
    pub async fn fetch_catalog(&self) -> Result<Vec<LseCatalogEntry>, LseError> {
        self.get_json(CATALOG_PATH, &[]).await
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;

    /// A price entry as the catalog serves one: `frequency`, `category`, `unit` and `source` all
    /// empty, `live == 1`. Values are structural only — no provider data is reproduced here.
    fn price_json() -> &'static str {
        r#"{
            "dataset": "crypto", "symbol": "AAA/BBB", "name": "Example",
            "ticks": 4145394353, "first_tick": "2017-08-17 04:00:28.322000",
            "last_tick": "2026-09-21 04:32:26.473000", "years": 9.1,
            "last_value": 1.5, "change_pct": 0.08, "change_1y": -29.5,
            "unit": "", "source": "", "category": "", "frequency": "",
            "country": "", "country_name": "", "live": 1
        }"#
    }

    /// A reference entry: `frequency` and `category` populated, `live == 0`.
    fn reference_json() -> &'static str {
        r#"{
            "dataset": "economics", "symbol": "exampleind", "name": "Example Indicator",
            "ticks": 412, "first_tick": "1990-01-02 00:00:00.000000",
            "last_tick": "2026-08-01 00:00:00.000000", "years": 36,
            "last_value": 3.25, "change_pct": 0.1, "change_1y": 2,
            "unit": "percent", "source": "World Bank", "category": "Inflation Index",
            "frequency": "monthly", "country": "GB", "country_name": "United Kingdom",
            "live": 0
        }"#
    }

    fn parse(json: &str) -> LseCatalogEntry {
        serde_json::from_str(json).expect("catalog entry should decode")
    }

    #[test]
    fn a_price_entry_classifies_as_price_and_resolves_its_dataset() {
        let entry = parse(price_json());

        assert_eq!(entry.class(), LseDatasetClass::Price);
        assert_eq!(entry.price_dataset(), Some(LseDataset::Crypto));
        assert!(entry.is_live());
    }

    #[test]
    fn a_reference_entry_classifies_as_reference_and_has_no_dataset() {
        let entry = parse(reference_json());

        assert_eq!(entry.class(), LseDatasetClass::Reference);
        // `economics` is not a price dataset, so `from_catalog_str`'s documented
        // `UnknownDataset` contract is preserved rather than inverted.
        assert_eq!(entry.price_dataset(), None);
        assert!(!entry.is_live());
    }

    #[test]
    fn the_measured_tick_count_leaves_too_little_u32_headroom_to_narrow_the_field() {
        // Pins the reason `ticks` is `u64`. The largest measured value sits at 96.5% of
        // `u32::MAX` on a tape that is still growing, so `u32` would overflow within months --
        // it has not overflowed yet, which is exactly why the field type must not wait for it.
        let measured = parse(price_json()).ticks;

        assert_eq!(measured, 4_145_394_353);
        let headroom = u64::from(u32::MAX) - measured;
        assert!(
            headroom * 20 < u64::from(u32::MAX),
            "{headroom} of headroom is more than 5% of u32::MAX -- re-check the claim"
        );
    }

    #[test]
    fn catalog_timestamps_parse_as_utc() {
        let entry = parse(price_json());

        let first = entry.first_tick_time().expect("first_tick should parse");
        assert_eq!(first.to_rfc3339(), "2017-08-17T04:00:28.322+00:00");
        assert!(entry.last_tick_time().is_ok());
    }

    #[test]
    fn a_malformed_timestamp_is_a_typed_error_naming_its_field() {
        let mut entry = parse(price_json());
        entry.first_tick = "17/08/2017".to_owned();

        let error = entry
            .first_tick_time()
            .expect_err("a bad timestamp should not parse");

        assert!(matches!(error, LseError::Deserialize { .. }));
        // The field is named so a caller can tell which of the two failed.
        assert!(error.to_string().contains("first_tick"), "{error}");
        // The other field is unaffected: one bad value does not poison the entry.
        assert!(entry.last_tick_time().is_ok());
    }

    #[test]
    fn a_seconds_only_timestamp_still_parses() {
        // `%.f` makes the fractional part optional, so a response that drops microseconds works.
        let mut entry = parse(price_json());
        entry.first_tick = "2017-08-17 04:00:28".to_owned();

        assert!(entry.first_tick_time().is_ok());
    }

    #[test]
    fn the_six_descriptive_fields_may_be_absent_entirely() {
        // They are already empty on thousands of entries, so absence means the same thing.
        let json = r#"{
            "dataset": "fx", "symbol": "AAA/BBB", "name": "Example",
            "ticks": 1, "first_tick": "2020-01-01 00:00:00.000000",
            "last_tick": "2020-01-02 00:00:00.000000", "years": 1,
            "last_value": 1, "change_pct": 0, "change_1y": 0, "live": 1
        }"#;

        let entry = parse(json);

        assert_eq!(entry.class(), LseDatasetClass::Price);
        assert!(entry.unit.is_empty());
        assert!(entry.country.is_empty());
    }

    #[test]
    fn an_unknown_field_does_not_break_decoding() {
        let json = price_json().replace(r#""live": 1"#, r#""live": 1, "added_later": "whatever""#);

        assert_eq!(parse(&json).class(), LseDatasetClass::Price);
    }

    #[test]
    fn integer_and_float_spellings_of_the_same_number_both_decode() {
        // Measured: `years`, `last_value`, `change_pct` and `change_1y` each arrive as a JSON int
        // on some entries and a float on others.
        let entry = parse(reference_json());

        assert!((entry.years - 36.0).abs() < f64::EPSILON);
        assert!((entry.change_1y - 2.0).abs() < f64::EPSILON);
    }

    #[test]
    fn a_dirty_frequency_spelling_is_preserved_rather_than_normalised() {
        // `bi-annually` and `quarter` are real measured spellings that a closed enum would have
        // dropped. They round-trip because the field is the provider's own string.
        for spelling in ["bi-annually", "quarter", "biweekly", "something-new"] {
            let json = reference_json().replace(
                r#""frequency": "monthly""#,
                &format!(r#""frequency": "{spelling}""#),
            );
            let entry = parse(&json);

            assert_eq!(entry.frequency, spelling);
            assert_eq!(entry.class(), LseDatasetClass::Reference);
        }
    }

    #[test]
    fn an_options_entry_is_a_price_dataset_that_still_resolves_to_no_variant() {
        // `options` is the 11th price dataset in the catalog and has no `LseDataset` variant, so
        // `None` here cannot be read as "this is reference data". Measured: 3,186 such entries.
        let json = price_json().replace(r#""dataset": "crypto""#, r#""dataset": "options""#);

        let entry = parse(&json);

        assert_eq!(entry.class(), LseDatasetClass::Price);
        assert_eq!(entry.price_dataset(), None);
    }

    #[test]
    fn an_entry_populating_only_category_is_treated_as_reference() {
        let json = price_json().replace(r#""category": """#, r#""category": "Credit index""#);

        assert_eq!(parse(&json).class(), LseDatasetClass::Reference);
    }
}
