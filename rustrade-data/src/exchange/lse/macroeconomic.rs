//! Macroeconomic series from LSE Data 3.0 (`/v3/macro`): economic observations, government bond
//! yields and the economic release calendar.
//!
//! Every series has a code and one of three kinds, and its rows are typed by kind:
//!
//! - [`Observation`](LseMacroKind::Observation), such as `US.inflation_rate`: rows of
//!   [`LseMacroObservation`] (`day, value`), from
//!   [`fetch_macro_observations`](LseClient::fetch_macro_observations).
//! - [`Yield`](LseMacroKind::Yield), such as `US10Y`: rows of [`LseMacroYield`] (`day, close`),
//!   from [`fetch_macro_yields`](LseClient::fetch_macro_yields).
//! - [`Release`](LseMacroKind::Release), such as `US.CPI`: rows of [`LseMacroRelease`]
//!   (`released_at` and the figures as text), from
//!   [`fetch_macro_releases`](LseClient::fetch_macro_releases).
//!
//! Find codes with [`fetch_macro_catalogue`](LseClient::fetch_macro_catalogue), and what each
//! kind covers with [`fetch_macro_summary`](LseClient::fetch_macro_summary). Codes are
//! case-sensitive (`US10Y`, not `us10y`) and may contain spaces (`US.10-Year Note Auction`); pass
//! them as the catalogue spells them.
//!
//! The rows are not market events: the provider gives no time at which an observation or a yield
//! became known, and inventing one would let a backtest see a figure before it was published.
//!
//! A key that belongs to an account is needed; a key without one is refused
//! `403 account-required`.
//!
//! # ⚠️ Licensing
//! Data retrieved here is **not redistributable**, and use by or for an organisation needs the
//! provider's licence. See the [module documentation](super#licensing) and
//! <https://londonstrategicedge.com/licence>.

use crate::exchange::lse::client::LseClient;
use crate::exchange::lse::error::LseError;
use crate::exchange::lse::historical::parse_provider_time;
use chrono::{DateTime, NaiveDate, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize, de::DeserializeOwned};
use smol_str::SmolStr;
use std::collections::BTreeMap;
use std::str::FromStr;

/// The kind of a macroeconomic series.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum LseMacroKind {
    /// Economic observations, such as an inflation rate or an unemployment rate.
    Observation,
    /// Government bond yields, one close per day.
    Yield,
    /// The economic release calendar: each release with its actual, previous, consensus and
    /// forecast figures, including scheduled releases not yet out.
    Release,
}

impl LseMacroKind {
    /// The provider's spelling.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Observation => "observation",
            Self::Yield => "yield",
            Self::Release => "release",
        }
    }
}

/// What one kind covers, from [`fetch_macro_summary`](LseClient::fetch_macro_summary).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct LseMacroKindSummary {
    /// The kind.
    pub kind: LseMacroKind,
    /// How many series it has.
    pub series: u64,
    /// How many countries they cover.
    pub countries: u64,
    /// The earliest day any of them covers.
    pub first: Option<NaiveDate>,
    /// The latest day any of them covers; a release series reaches into the future.
    pub last: Option<NaiveDate>,
}

/// What one country has, from [`fetch_macro_summary`](LseClient::fetch_macro_summary).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct LseMacroCountry {
    /// The country, as the provider codes it (two letters for most).
    pub country: SmolStr,
    /// How many series it has.
    pub series: u64,
    /// How many of them are of each kind, by the provider's spelling of the kind.
    #[serde(default)]
    pub kinds: BTreeMap<SmolStr, u64>,
}

/// What the macro catalogue holds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct LseMacroSummary {
    /// Each kind's coverage.
    pub kinds: Vec<LseMacroKindSummary>,
    /// Each country's series.
    pub countries: Vec<LseMacroCountry>,
}

/// A figure in the catalogue: a number for observations and yields, the provider's text for a
/// release.
///
/// Serialized as the provider writes it, a JSON number or a string, so it reads back as the same
/// variant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(untagged)]
pub enum LseMacroValue {
    /// A number.
    // As a JSON number: a figure read from one is its float's shortest text, which the float
    // gives back exactly.
    Number(#[serde(serialize_with = "rust_decimal::serde::float::serialize")] Decimal),
    /// Text, as a release's figures are (`"5.3%"`, `"337.51Index"`); see [`LseReleaseValue`].
    Text(SmolStr),
}

impl<'de> Deserialize<'de> for LseMacroValue {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        // Read by JSON type rather than by trying a decimal first, so the text `"5"` stays text.
        match serde_json::Value::deserialize(deserializer)? {
            serde_json::Value::Number(number) => decimal_from_json(&number)
                .map(Self::Number)
                .map_err(serde::de::Error::custom),
            serde_json::Value::String(text) => Ok(Self::Text(SmolStr::new(text))),
            other => Err(serde::de::Error::custom(format!(
                "a macro figure must be a number or text, not {other}"
            ))),
        }
    }
}

/// A JSON number as a [`Decimal`], through its shortest text: exact for an integer and for the
/// decimal the provider wrote.
fn decimal_from_json(number: &serde_json::Number) -> Result<Decimal, String> {
    let text = number.to_string();
    Decimal::from_str(&text)
        .or_else(|_| Decimal::from_scientific(&text))
        .map_err(|error| format!("{text} is not representable as a decimal: {error}"))
}

/// A decimal field: the provider's JSON number, or the decimal string [`Decimal`] serializes as,
/// so a row this crate serialized reads back.
fn deserialize_decimal<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Decimal, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Figure {
        Number(serde_json::Number),
        Text(SmolStr),
    }

    match Figure::deserialize(deserializer)? {
        Figure::Number(number) => decimal_from_json(&number),
        Figure::Text(text) => {
            Decimal::from_str(&text).map_err(|error| format!("{text} is not a decimal: {error}"))
        }
    }
    .map_err(serde::de::Error::custom)
}

/// An empty string as `None`.
fn deserialize_blank_as_none<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<SmolStr>, D::Error> {
    let text = Option::<SmolStr>::deserialize(deserializer)?;
    Ok(text.filter(|text| !text.trim().is_empty()))
}

/// One series in the macro catalogue.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct LseMacroSeries {
    /// The code its rows are read by, case-sensitive.
    pub code: SmolStr,
    /// Its kind, which decides how its rows are read.
    pub kind: LseMacroKind,
    /// The country.
    pub country: SmolStr,
    /// The provider's category.
    #[serde(default)]
    pub category: Option<SmolStr>,
    /// A readable name.
    pub name: String,
    /// The unit of its figures, as the provider writes it.
    #[serde(default, deserialize_with = "deserialize_blank_as_none")]
    pub unit: Option<SmolStr>,
    /// How often it updates (`monthly`, ...), or `None` when the provider records none.
    #[serde(default, deserialize_with = "deserialize_blank_as_none")]
    pub frequency: Option<SmolStr>,
    /// Where the frequency comes from: `recorded`, `inferred` or `unavailable`.
    #[serde(default)]
    pub frequency_source: Option<SmolStr>,
    /// The provider's note on the frequency; an inferred one is not a published schedule.
    #[serde(default)]
    pub frequency_note: Option<String>,
    /// Its first day.
    #[serde(default)]
    pub first: Option<NaiveDate>,
    /// Its last day; a release series reaches into the future with scheduled releases.
    #[serde(default)]
    pub last: Option<NaiveDate>,
    /// How many figures it has.
    #[serde(default)]
    pub values: Option<u64>,
    /// How many records the provider stores for it.
    #[serde(default)]
    pub stored_records: Option<u64>,
    /// For a release series, how many releases are out.
    #[serde(default)]
    pub released_records: Option<u64>,
    /// For a release series, how many are scheduled and not yet out.
    #[serde(default)]
    pub scheduled_records: Option<u64>,
    /// Its latest figure.
    #[serde(default)]
    pub latest_value: Option<LseMacroValue>,
    /// When its latest figure is from, as the provider's UTC text.
    #[serde(default)]
    pub latest_at: Option<SmolStr>,
    /// Where its latest figure comes from (`released_actual`, ...).
    #[serde(default)]
    pub latest_source: Option<SmolStr>,
}

/// One page of the macro catalogue.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct LseMacroCataloguePage {
    /// How many series matched, across every page.
    pub total: u64,
    /// This page's series.
    pub series: Vec<LseMacroSeries>,
}

/// How [`fetch_macro_catalogue`](LseClient::fetch_macro_catalogue) narrows the catalogue.
///
/// `Default` is no filter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct LseMacroFilter<'a> {
    /// One country, as the provider codes it (`US`, `GB`, ...).
    pub country: Option<&'a str>,
    /// One exact code.
    pub code: Option<&'a str>,
    /// A search over codes and names, at most 128 characters.
    pub search: Option<&'a str>,
}

impl<'a> LseMacroFilter<'a> {
    /// Series of one country.
    #[must_use]
    pub fn country(mut self, country: &'a str) -> Self {
        self.country = Some(country);
        self
    }

    /// The series with one exact code.
    #[must_use]
    pub fn code(mut self, code: &'a str) -> Self {
        self.code = Some(code);
        self
    }

    /// Series whose code or name matches `text`.
    #[must_use]
    pub fn search(mut self, text: &'a str) -> Self {
        self.search = Some(text);
        self
    }
}

/// One day of an economic observation series.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct LseMacroObservation {
    /// The period the figure describes (a month's first day for a monthly series), **not** the day
    /// it was published.
    pub day: NaiveDate,
    /// The figure.
    #[serde(deserialize_with = "deserialize_decimal")]
    pub value: Decimal,
}

/// One day of a government bond yield series.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct LseMacroYield {
    /// The day.
    pub day: NaiveDate,
    /// The yield at that day's close, in percent.
    #[serde(deserialize_with = "deserialize_decimal")]
    pub close: Decimal,
}

/// A figure of an economic release, as the provider's text.
///
/// The provider writes a release's figures with their unit attached (`"5.3%"`, `"337.51Index"`,
/// `"-0.2%"`), and the units vary from series to series. The text is kept whole, and
/// [`parse`](Self::parse) splits it without interpreting the unit.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LseReleaseValue(pub SmolStr);

impl LseReleaseValue {
    /// The provider's text.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Split the text into its leading number and the rest, or `None` when it does not start with
    /// one, or when the rest goes on like a number (`"1.2.3"`, `"5-3"`), so the split would be a
    /// guess.
    ///
    /// The number is read as written and the rest is returned as written: `"5.3%"` is `5.3` and
    /// `"%"`, `"337.51Index"` is `337.51` and `"Index"`, `"250K"` is `250` and `"K"`. Nothing is
    /// scaled: what a suffix means (a percentage, thousands, an index level) is the series'
    /// business, and differs between series.
    pub fn parse(&self) -> Option<(Decimal, &str)> {
        let text = self.0.trim();
        let unsigned = text.strip_prefix(['+', '-']).unwrap_or(text);
        let digits = unsigned
            .find(|character: char| !(character.is_ascii_digit() || character == '.'))
            .unwrap_or(unsigned.len());
        let (number, rest) = text.split_at(text.len() - unsigned.len() + digits);
        // A sign after the number would continue it, and a second point fails the decimal parse.
        if !number.bytes().any(|byte| byte.is_ascii_digit()) || rest.starts_with(['+', '-']) {
            return None;
        }
        Decimal::from_str(number)
            .ok()
            .map(|number| (number, rest.trim()))
    }
}

impl std::fmt::Display for LseReleaseValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// A UTC timestamp written as the provider's text.
fn deserialize_provider_time<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<DateTime<Utc>, D::Error> {
    let text = String::deserialize(deserializer)?;
    parse_provider_time(&text).map_err(serde::de::Error::custom)
}

/// One economic release.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct LseMacroRelease {
    /// When it was, or is scheduled to be, released.
    #[serde(deserialize_with = "deserialize_provider_time")]
    pub released_at: DateTime<Utc>,
    /// The released figure; `None` until it is out.
    pub actual: Option<LseReleaseValue>,
    /// The previous figure, as given at this release. It may since have been revised, so a copy
    /// stored now is not what was known then.
    pub previous: Option<LseReleaseValue>,
    /// The consensus expectation, where the provider has one.
    pub consensus: Option<LseReleaseValue>,
    /// The provider's forecast, where it has one.
    pub forecast: Option<LseReleaseValue>,
}

/// The `/v3/macro/series` answer; only its rows are kept.
#[derive(Debug, Deserialize)]
struct SeriesAnswer<Row> {
    rows: Vec<Row>,
}

impl LseClient {
    /// Fetch what the macro catalogue holds: each kind's coverage and each country's series.
    ///
    /// # Errors
    /// See [`LseError`].
    pub async fn fetch_macro_summary(&self) -> Result<LseMacroSummary, LseError> {
        self.get_json("macro", &[]).await
    }

    /// Fetch one page of one kind's series, to find codes.
    ///
    /// * `limit` - at most 1,000; `offset` - series to skip. The catalogue is ordered by code, so
    ///   paging by offset is stable.
    ///
    /// # Errors
    /// See [`LseError`]: a malformed filter is refused `400` with the provider's code
    /// (`country-bad`, `search-bad`, ...).
    pub async fn fetch_macro_catalogue(
        &self,
        kind: LseMacroKind,
        filter: LseMacroFilter<'_>,
        limit: u32,
        offset: u64,
    ) -> Result<LseMacroCataloguePage, LseError> {
        let mut query = vec![
            ("kind", kind.as_str().to_owned()),
            ("limit", limit.to_string()),
            ("offset", offset.to_string()),
        ];
        if let Some(country) = filter.country {
            query.push(("country", country.to_owned()));
        }
        if let Some(code) = filter.code {
            query.push(("code", code.to_owned()));
        }
        if let Some(search) = filter.search {
            query.push(("q", search.to_owned()));
        }
        self.get_json("macro", &query).await
    }

    /// Fetch an economic observation series' days in `[from, to]` (both inclusive), oldest first.
    ///
    /// `None` for a bound reads from the series' first day or to its last.
    ///
    /// # Errors
    /// - A code not of this kind, or not in the catalogue, is [`LseError::Api`]
    ///   `404 series-unknown`.
    /// - More than 100,000 rows in the window is `413 series-window-too-wide`: split the window.
    /// - An inverted window is [`LseError::InvalidInput`], before anything is sent.
    pub async fn fetch_macro_observations(
        &self,
        code: &str,
        from: Option<NaiveDate>,
        to: Option<NaiveDate>,
    ) -> Result<Vec<LseMacroObservation>, LseError> {
        self.fetch_macro_rows(LseMacroKind::Observation, code, from, to)
            .await
    }

    /// Fetch a government bond yield series' days in `[from, to]` (both inclusive), oldest first.
    ///
    /// `None` for a bound reads from the series' first day or to its last.
    ///
    /// # Errors
    /// - A code not of this kind, or not in the catalogue, is [`LseError::Api`]
    ///   `404 series-unknown`.
    /// - More than 100,000 rows in the window is `413 series-window-too-wide`: split the window.
    /// - An inverted window is [`LseError::InvalidInput`], before anything is sent.
    pub async fn fetch_macro_yields(
        &self,
        code: &str,
        from: Option<NaiveDate>,
        to: Option<NaiveDate>,
    ) -> Result<Vec<LseMacroYield>, LseError> {
        self.fetch_macro_rows(LseMacroKind::Yield, code, from, to)
            .await
    }

    /// Fetch a release series' releases dated in `[from, to]` (both inclusive), oldest first,
    /// scheduled releases included.
    ///
    /// `None` for a bound reads from the series' first day or to its last.
    ///
    /// # Errors
    /// - A code not of this kind, or not in the catalogue, is [`LseError::Api`]
    ///   `404 series-unknown`.
    /// - More than 100,000 rows in the window is `413 series-window-too-wide`: split the window.
    /// - An inverted window is [`LseError::InvalidInput`], before anything is sent.
    pub async fn fetch_macro_releases(
        &self,
        code: &str,
        from: Option<NaiveDate>,
        to: Option<NaiveDate>,
    ) -> Result<Vec<LseMacroRelease>, LseError> {
        self.fetch_macro_rows(LseMacroKind::Release, code, from, to)
            .await
    }

    /// The shared read behind the three typed series methods, whose rustdoc lists its errors.
    async fn fetch_macro_rows<Row: DeserializeOwned>(
        &self,
        kind: LseMacroKind,
        code: &str,
        from: Option<NaiveDate>,
        to: Option<NaiveDate>,
    ) -> Result<Vec<Row>, LseError> {
        if let (Some(from), Some(to)) = (from, to)
            && from > to
        {
            return Err(LseError::InvalidInput {
                message: format!("macro window ends ({to}) before it starts ({from})"),
            });
        }

        let mut query = vec![
            ("kind", kind.as_str().to_owned()),
            ("code", code.to_owned()),
        ];
        if let Some(from) = from {
            query.push(("from", from.to_string()));
        }
        if let Some(to) = to {
            query.push(("to", to.to_string()));
        }
        let answer: SeriesAnswer<Row> = self.get_json("macro/series", &query).await?;
        Ok(answer.rows)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;

    // Shapes measured on `/v3` 2026-11-28; every value is invented.

    fn decimal(text: &str) -> Decimal {
        Decimal::from_str(text).unwrap()
    }

    #[test]
    fn the_summary_decodes() {
        let json = r#"{"group": null,
            "kinds": [{"kind": "yield", "series": 3, "countries": 2, "first": "1962-01-02",
                       "last": "2026-11-25"}],
            "countries": [{"country": "US", "series": 5,
                           "kinds": {"observation": 4, "yield": 1}}]}"#;
        let summary: LseMacroSummary = serde_json::from_str(json).unwrap();

        assert_eq!(summary.kinds[0].kind, LseMacroKind::Yield);
        assert_eq!(summary.countries[0].kinds["yield"], 1);
    }

    #[test]
    fn a_catalogue_page_reads_a_release_series_with_text_figures_and_no_frequency() {
        let json = r#"{"count": 1, "country": "US", "group": null, "kind": "release",
            "limit": 1, "offset": 0, "total": 9, "series": [{"category": "US",
            "code": "US.Test Release", "country": "US", "first": "2025-12-18",
            "frequency": "", "frequency_note": "Frequency is not recorded.",
            "frequency_source": "unavailable", "kind": "release", "last": "2026-12-10",
            "last_value": null, "latest_at": "2026-11-10 13:30:00",
            "latest_source": "released_actual", "latest_value": "1.5%", "name": "Test",
            "released_records": 11, "scheduled_records": 1, "stored_records": 12,
            "unit": "", "values": 12}]}"#;
        let page: LseMacroCataloguePage = serde_json::from_str(json).unwrap();
        let series = &page.series[0];

        assert_eq!(page.total, 9);
        assert_eq!(series.kind, LseMacroKind::Release);
        assert_eq!(series.frequency, None);
        assert_eq!(series.unit, None);
        assert_eq!(
            series.latest_value,
            Some(LseMacroValue::Text("1.5%".into()))
        );
        assert_eq!(series.scheduled_records, Some(1));
    }

    #[test]
    fn a_catalogue_figure_is_a_number_by_its_json_type_only() {
        let number: LseMacroValue = serde_json::from_str("2.6").unwrap();
        let integer: LseMacroValue = serde_json::from_str("3").unwrap();
        let text: LseMacroValue = serde_json::from_str(r#""5""#).unwrap();

        assert_eq!(number, LseMacroValue::Number(decimal("2.6")));
        assert_eq!(integer, LseMacroValue::Number(decimal("3")));
        assert_eq!(text, LseMacroValue::Text("5".into()));
        assert!(serde_json::from_str::<LseMacroValue>("true").is_err());
    }

    #[test]
    fn observation_rows_read_integers_and_decimals_exactly() {
        let json = r#"{"code": "US.test", "columns": ["day", "value"], "from": "2026-01-01",
            "kind": "observation", "to": "2026-03-01",
            "rows": [{"day": "2026-01-01", "value": 3}, {"day": "2026-02-01", "value": 2.65}]}"#;
        let answer: SeriesAnswer<LseMacroObservation> = serde_json::from_str(json).unwrap();

        assert_eq!(answer.rows[0].value, decimal("3"));
        assert_eq!(answer.rows[1].value, decimal("2.65"));
        assert_eq!(
            answer.rows[1].day,
            NaiveDate::from_ymd_opt(2026, 2, 1).unwrap()
        );
    }

    #[test]
    fn yield_rows_read_their_close() {
        let json = r#"{"rows": [{"day": "2026-11-24", "close": 4.123}]}"#;
        let answer: SeriesAnswer<LseMacroYield> = serde_json::from_str(json).unwrap();

        assert_eq!(answer.rows[0].close, decimal("4.123"));
    }

    #[test]
    fn a_release_row_reads_its_time_and_a_scheduled_one_has_no_figures() {
        let json = r#"{"rows": [
            {"released_at": "2026-10-07 17:00:00", "actual": "5.3%", "previous": "4.834%",
             "consensus": null, "forecast": null},
            {"released_at": "2026-12-08 18:00:00", "actual": null, "previous": null,
             "consensus": null, "forecast": null}]}"#;
        let answer: SeriesAnswer<LseMacroRelease> = serde_json::from_str(json).unwrap();

        assert_eq!(
            answer.rows[0].released_at,
            parse_provider_time("2026-10-07 17:00:00").unwrap()
        );
        assert_eq!(answer.rows[0].actual.as_ref().unwrap().as_str(), "5.3%");
        assert_eq!(answer.rows[1].actual, None);
    }

    #[test]
    fn a_release_figure_splits_into_its_number_and_unit_unscaled() {
        let value = |text: &str| LseReleaseValue(SmolStr::new(text));

        assert_eq!(value("5.3%").parse(), Some((decimal("5.3"), "%")));
        assert_eq!(
            value("337.51Index").parse(),
            Some((decimal("337.51"), "Index"))
        );
        assert_eq!(value("250K").parse(), Some((decimal("250"), "K")));
        assert_eq!(value("-0.2%").parse(), Some((decimal("-0.2"), "%")));
        assert_eq!(value(" 12 ").parse(), Some((decimal("12"), "")));
        assert_eq!(value("+1.5").parse(), Some((decimal("1.5"), "")));
        assert_eq!(value(".5%").parse(), Some((decimal("0.5"), "%")));
        // Text that goes on like a number is not split into a number and a unit.
        assert_eq!(value("1.2.3").parse(), None);
        assert_eq!(value("5-3").parse(), None);
        assert_eq!(value("5+").parse(), None);
        assert_eq!(value(".%").parse(), None);
        assert_eq!(value("n/a").parse(), None);
        assert_eq!(value("-").parse(), None);
        assert_eq!(value("").parse(), None);
    }

    #[test]
    fn rows_and_figures_read_back_what_they_serialize() {
        let observation: LseMacroObservation =
            serde_json::from_str(r#"{"day": "2026-11-01", "value": 2.6}"#).unwrap();
        let yields: LseMacroYield =
            serde_json::from_str(r#"{"day": "2026-11-24", "close": 4.5}"#).unwrap();

        let again: LseMacroObservation =
            serde_json::from_str(&serde_json::to_string(&observation).unwrap()).unwrap();
        assert_eq!(again, observation);
        let again: LseMacroYield =
            serde_json::from_str(&serde_json::to_string(&yields).unwrap()).unwrap();
        assert_eq!(again, yields);

        for figure in [
            LseMacroValue::Number(decimal("2.6")),
            LseMacroValue::Text(SmolStr::new("5")),
        ] {
            let json = serde_json::to_string(&figure).unwrap();
            assert_eq!(
                serde_json::from_str::<LseMacroValue>(&json).unwrap(),
                figure,
                "{json}"
            );
        }
    }

    #[test]
    fn a_release_figure_displays_as_the_providers_text() {
        assert_eq!(LseReleaseValue(SmolStr::new("5.3%")).to_string(), "5.3%");
    }
}
