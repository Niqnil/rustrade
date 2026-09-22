//! Sovereign bond yields from the London Strategic Edge data API (`GET /bond-yields`).
//!
//! Daily open/high/low/close yields for 34 countries across the tenors each publishes — 716,820
//! observations at the last measurement. Served by the [data API](super::data_api) host, not by the
//! [vault](super::vault): a bond-yield symbol has **no candle data** there (`GET
//! /vault/candles?symbol=UK5Y` answers `404`), so this is a separate fetch path rather than a
//! resolution of the candle one.
//!
//! ```no_run
//! # use chrono::NaiveDate;
//! # use rustrade_data::exchange::lse::bond_yield::LseBondYieldQuery;
//! # use rustrade_data::exchange::lse::data_api::LseDataApiClient;
//! # use rustrade_data::exchange::lse::error::LseError;
//! # async fn example() -> Result<(), LseError> {
//! let client = LseDataApiClient::from_env()?;
//!
//! // The stats handle is fetched once and reused: it is what makes a query checkable.
//! let stats = client.fetch_bond_yield_stats().await?;
//!
//! let query = LseBondYieldQuery::new(
//!     "GB", // normalised to the provider's `UK` before the request is sent
//!     NaiveDate::from_ymd_opt(2024, 1, 1).expect("valid date"),
//!     NaiveDate::from_ymd_opt(2024, 12, 31).expect("valid date"),
//! )
//! .with_maturity("5Y");
//!
//! for row in client.fetch_bond_yields(&stats, &query).await? {
//!     println!("{} {} {}", row.date, row.symbol, row.close);
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # ⚠️ The endpoint validates nothing — a wrong query is a `200` with zero rows
//!
//! This is the single most important property of the surface, and it is why
//! [`fetch_bond_yields`](LseDataApiClient::fetch_bond_yields) takes a
//! [`LseBondYieldStats`] as a **required** argument rather than offering validation as an option.
//! Measured against the live host:
//!
//! | request | answer |
//! |---|---|
//! | `country=UK` | `200`, rows |
//! | `country=GB` (ISO-3166 spelling) | `200`, `count: 0` |
//! | `country=ZZ` (nonsense) | `200`, `count: 0` |
//! | `maturity=99Y` (nonsense) | `200`, `count: 0` |
//! | `US 10Y TIPS` over 2023–2024 | `200`, `count: 0` — **and correct**, coverage starts 2025-07-07 |
//!
//! All four zero-row answers are byte-identical. Nothing in the response distinguishes a typo from
//! a quiet market, so the distinction has to be made before the request is sent, against the only
//! source of truth the provider offers: `/bond-yields/stats`.
//!
//! # ⚠️ Membership is not enough — the range must be checked too
//!
//! The last row of that table is the reason. `US 10Y TIPS` and `TR 2Y` both **exist** in stats and
//! both return nothing for 2023–2024, because neither was published before 2025-07-07. A validator
//! that only checked that the `(country, maturity)` pair existed would pass that query and hand
//! back the same indistinguishable empty result it was built to prevent. So
//! [`LseBondYieldQuery::validate`] checks the requested window against the **pair's own**
//! `first_date`/`last_date`.
//!
//! # ⚠️ A tenor is a label, not a duration
//!
//! `maturity` is the provider's own string — `5Y`, `10Y TIPS` — and it carries instrument *type* as
//! well as term. Each US TIPS tenor reports the **same `maturity_days` as its nominal twin**
//! (`5Y` and `5Y TIPS` both `1825`, `10Y` and `10Y TIPS` both `3650`, `30Y` and `30Y TIPS` both
//! `10950`), so keying a series on `(country, maturity_days)` silently merges a real yield with an
//! inflation-linked one. Key on [`maturity`](LseBondYieldRow::maturity); treat
//! [`maturity_days`](LseBondYieldRow::maturity_days) as a derived hint and nothing more. The same
//! fact rules out modelling a tenor as a [`Duration`](std::time::Duration).
//!
//! # ⚠️ `country_iso2` is not ISO-3166
//!
//! The column is named `country_iso2` and the United Kingdom is keyed **`UK`**, which is not an
//! ISO-3166-1 alpha-2 code; `GB` is absent from all 34 keys. It is the only divergence in the set,
//! and the vault's catalog spells the same country `GB` — so a code crossing between the two hosts
//! must be translated. [`LseBondYieldStats::normalise_country`] does it, and every entry point here
//! applies it.
//!
//! # The OHLC is genuine
//!
//! Roughly one observation per business day invites the assumption that these are close-only
//! figures reshaped into bars, which would make the open/high/low legs carry no information.
//! Measured over calendar 2024 on three independent series, they are not: degenerate
//! (`open == high == low == close`) rows are **0 of 250** on US 10Y, 1 of 256 on UK 5Y and 5 of 280
//! on CH 2Y, with zero nulls in any leg. The handful that are degenerate are ordinary quiet days.
//!
//! # ⚠️ Licensing — this data may not be redistributed
//!
//! Bond-yield rows are provider data. They may be used for your own research, trading and model
//! training, including commercially, but **not** redistributed or re-served to third parties in any
//! form. Do not commit responses as fixtures or example datasets. Terms:
//! <https://londonstrategicedge.com/terms>

use crate::exchange::lse::data_api::LseDataApiClient;
use crate::exchange::lse::error::LseError;
use chrono::NaiveDate;
use rust_decimal::Decimal;
use serde::Deserialize;
use smol_str::SmolStr;
use std::collections::BTreeMap;

/// Path of the bond-yield rows endpoint, relative to the data API base URL.
const BOND_YIELDS_PATH: &str = "bond-yields";

/// Path of the bond-yield stats endpoint, relative to the data API base URL.
const BOND_YIELDS_STATS_PATH: &str = "bond-yields/stats";

/// Spelling of every date on this endpoint — rows and stats alike.
///
/// ⚠️ **Not** the vault's
/// [`PROVIDER_TIMESTAMP_FORMAT`](super::PROVIDER_TIMESTAMP_FORMAT), which is a 26-character
/// microsecond stamp (`2024-01-02 09:09:00.000000`). Two hosts, two spellings; a bond yield is
/// dated to the day and carries no time of day at all, which is why the query below takes a
/// [`NaiveDate`] rather than an instant.
const DATE_FORMAT: &str = "%Y-%m-%d";

/// Value of the `format` parameter that selects JSON.
///
/// ⚠️ **Required, not a default.** This endpoint serves **CSV** when `format` is absent, so
/// omitting it hands a CSV body to a JSON decoder.
const JSON_FORMAT: &str = "json";

/// The provider's spelling of the United Kingdom, which is not its ISO-3166-1 alpha-2 code.
const UK_CODE: &str = "UK";

/// The ISO-3166-1 alpha-2 code for the United Kingdom, which this host does **not** use.
const GB_CODE: &str = "GB";

/// Parse a provider date, naming the field so a caller can tell which one failed.
fn parse_date(value: &str, field: &str) -> Result<NaiveDate, LseError> {
    NaiveDate::parse_from_str(value, DATE_FORMAT).map_err(|error| LseError::Deserialize {
        message: format!("invalid bond-yield {field} {value:?}: {error}"),
    })
}

/// One daily bond-yield observation, as the data API serves it.
///
/// # ⚠️ Every field arrives as a JSON **string**, numerics included
///
/// `{"date": "2023-01-03", "maturity_days": "1825", "open": "3.5840", …}` — there is not a single
/// JSON number in a row. The prices are therefore decoded with
/// [`rust_decimal::serde::str`], **not** with the `rust_decimal::serde::float` used by
/// `AlpacaStockSplit`: that helper expects a JSON number and copying it here fails outright.
///
/// All eleven fields were present and non-null, with a stable type, on every one of the 1,678 rows
/// measured across five series spanning four countries — so none is optional.
///
/// Unknown fields are ignored, so a field the provider adds does not break decoding.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct LseBondYieldRow {
    /// Observation date, spelled `2023-01-03` — ten characters, no time of day.
    ///
    /// Kept as the provider's string and parsed on demand by
    /// [`observation_date`](Self::observation_date), following the same rule as
    /// [`LseCatalogEntry`](super::reference::LseCatalogEntry)'s timestamps: one malformed row then
    /// fails at the row rather than taking the whole fetch down with it.
    pub date: String,
    /// Provider symbol for the series, formed by concatenating country and tenor **without
    /// spaces**: `UK5Y`, and `US10YTIPS` for the `10Y TIPS` tenor.
    ///
    /// Because the spaces are dropped, this is not a reliable key back to
    /// [`maturity`](Self::maturity) — read the two separately.
    pub symbol: SmolStr,
    /// Country code as this host spells it.
    ///
    /// ⚠️ **Despite the name, this is not ISO-3166-1 alpha-2**: the United Kingdom is `UK`, not
    /// `GB`. See the [module documentation](self).
    pub country_iso2: SmolStr,
    /// Country name (`"United Kingdom"`).
    pub country_name: String,
    /// Tenor as the provider labels it (`"5Y"`, `"10Y TIPS"`).
    ///
    /// ⚠️ **This, not [`maturity_days`](Self::maturity_days), identifies the series.** See the
    /// [module documentation](self) for the TIPS collision that makes the distinction load-bearing.
    pub maturity: SmolStr,
    /// Term in days, as a string (`"1825"`).
    ///
    /// ⚠️ **A derived hint, never a key.** `5Y` and `5Y TIPS` both report `1825`. Parse it with
    /// [`maturity_days_value`](Self::maturity_days_value) if you want the number; kept as the
    /// provider's string so a malformed value on one row cannot fail a whole fetch over a field
    /// that carries no identity.
    pub maturity_days: SmolStr,
    /// Currency the yield's underlying is denominated in (`"GBP"`).
    ///
    /// Descriptive only — a yield is a percentage, and this does not denominate it.
    pub currency: SmolStr,
    /// Opening yield, in percent.
    #[serde(with = "rust_decimal::serde::str")]
    pub open: Decimal,
    /// Highest yield of the session, in percent.
    #[serde(with = "rust_decimal::serde::str")]
    pub high: Decimal,
    /// Lowest yield of the session, in percent.
    #[serde(with = "rust_decimal::serde::str")]
    pub low: Decimal,
    /// Closing yield, in percent.
    #[serde(with = "rust_decimal::serde::str")]
    pub close: Decimal,
}

impl LseBondYieldRow {
    /// Parses [`date`](Self::date) as a calendar date.
    ///
    /// # Errors
    /// Returns [`LseError::Deserialize`] if the value is not in the provider's `YYYY-MM-DD`
    /// spelling.
    pub fn observation_date(&self) -> Result<NaiveDate, LseError> {
        parse_date(&self.date, "date")
    }

    /// Parses [`maturity_days`](Self::maturity_days) as a day count.
    ///
    /// ⚠️ A hint only — see the field's documentation and the [module documentation](self) before
    /// using it for anything but display.
    ///
    /// # Errors
    /// Returns [`LseError::Deserialize`] if the value is not a non-negative integer.
    pub fn maturity_days_value(&self) -> Result<u32, LseError> {
        self.maturity_days
            .parse()
            .map_err(|error| LseError::Deserialize {
                message: format!(
                    "invalid bond-yield maturity_days {:?}: {error}",
                    self.maturity_days
                ),
            })
    }
}

/// The provider's index of what it publishes on `/bond-yields`.
///
/// Fetched by [`fetch_bond_yield_stats`](LseDataApiClient::fetch_bond_yield_stats) and passed to
/// [`fetch_bond_yields`](LseDataApiClient::fetch_bond_yields), which is what makes a query
/// checkable against an endpoint that checks nothing itself.
///
/// # Fetch it once
/// It is a description of the provider's coverage, not of any one query, so one handle serves every
/// fetch. Nothing here refreshes it: a long-lived process that wants to see newly published series
/// must fetch it again, and the cost of *not* doing so is a query rejected for a range the provider
/// has since filled — visible and typed, never a silent wrong answer.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct LseBondYieldStats {
    /// Number of countries published. Measured at 34.
    pub total_countries: u32,
    /// Total observations across every country and tenor. Measured at 716,820.
    pub total_observations: u64,
    /// Column names of a row, in the order the CSV form emits them.
    ///
    /// Matches [`LseBondYieldRow`]'s fields exactly at the time of writing, and is carried through
    /// so a caller can detect a provider-side schema change without decoding rows.
    pub columns: Vec<SmolStr>,
    /// Per-country coverage, keyed by the provider's own country code.
    ///
    /// ⚠️ Keyed **`UK`**, not `GB`. Look countries up through [`country`](Self::country) rather
    /// than indexing this directly, so the normalisation is applied.
    pub countries: BTreeMap<SmolStr, LseBondYieldCountry>,
}

impl LseBondYieldStats {
    /// Translates a country code into the provider's spelling.
    ///
    /// Trims, upper-cases, and maps the ISO-3166-1 alpha-2 `GB` to the provider's `UK`.
    ///
    /// # Why exactly one alias
    /// The United Kingdom is the **only** divergence among the 34 codes — every other one matches
    /// ISO-3166-1 alpha-2 and matches the vault catalog's spelling. A general alias table would
    /// invent mappings the provider has not been measured to want; a single documented exception
    /// matches a single measured exception.
    #[must_use]
    pub fn normalise_country(code: &str) -> SmolStr {
        let upper = code.trim().to_ascii_uppercase();

        if upper == GB_CODE {
            SmolStr::new_static(UK_CODE)
        } else {
            SmolStr::from(upper)
        }
    }

    /// Looks up a country, applying [`normalise_country`](Self::normalise_country) first.
    ///
    /// So `"gb"`, `"GB"` and `"UK"` all find the United Kingdom.
    #[must_use]
    pub fn country(&self, code: &str) -> Option<&LseBondYieldCountry> {
        self.countries.get(&Self::normalise_country(code))
    }
}

/// One country's bond-yield coverage.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct LseBondYieldCountry {
    /// The provider's country code, repeating the map key.
    pub code: SmolStr,
    /// Country name (`"United Kingdom"`).
    pub name: String,
    /// Currency of the country's sovereign debt (`"GBP"`).
    pub currency: SmolStr,
    /// Earliest observation across **any** tenor, spelled `YYYY-MM-DD`. Parse with
    /// [`first_date_value`](Self::first_date_value).
    pub first_date: String,
    /// Latest observation across **any** tenor, spelled `YYYY-MM-DD`. Parse with
    /// [`last_date_value`](Self::last_date_value).
    pub last_date: String,
    /// Total observations across every tenor. A JSON integer here, where a row's own numerics are
    /// strings.
    pub observations: u64,
    /// The tenors this country publishes.
    pub maturities: Vec<LseBondYieldMaturity>,
}

impl LseBondYieldCountry {
    /// Looks up one of this country's tenors by its provider label.
    ///
    /// Matched case-insensitively and ignoring surrounding whitespace, but **not** ignoring inner
    /// spaces: `10Y TIPS` and `10YTIPS` are different strings and only the former is a tenor. The
    /// row's `symbol` field is the spaceless form, which is why it is not a key.
    #[must_use]
    pub fn maturity(&self, tenor: &str) -> Option<&LseBondYieldMaturity> {
        let wanted = tenor.trim();

        self.maturities
            .iter()
            .find(|maturity| maturity.tenor.eq_ignore_ascii_case(wanted))
    }

    /// Parses [`first_date`](Self::first_date).
    ///
    /// # Errors
    /// Returns [`LseError::Deserialize`] if the value is not in the provider's `YYYY-MM-DD`
    /// spelling.
    pub fn first_date_value(&self) -> Result<NaiveDate, LseError> {
        parse_date(&self.first_date, "first_date")
    }

    /// Parses [`last_date`](Self::last_date).
    ///
    /// # Errors
    /// Returns [`LseError::Deserialize`] if the value is not in the provider's `YYYY-MM-DD`
    /// spelling.
    pub fn last_date_value(&self) -> Result<NaiveDate, LseError> {
        parse_date(&self.last_date, "last_date")
    }
}

/// One tenor's coverage within a country.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct LseBondYieldMaturity {
    /// The provider's tenor label (`"5Y"`, `"10Y TIPS"`).
    ///
    /// ⚠️ This identifies the series; [`days`](Self::days) does not.
    pub tenor: SmolStr,
    /// Term in days, as a JSON **integer** here — where a row's `maturity_days` is a string. Same
    /// host, same quantity, two encodings.
    ///
    /// ⚠️ **Not unique within a country.** Each US TIPS tenor reports the same value as its nominal
    /// twin.
    pub days: u32,
    /// Earliest observation for this tenor, spelled `YYYY-MM-DD`. Parse with
    /// [`first_date_value`](Self::first_date_value).
    ///
    /// ⚠️ Can be far later than the country's own `first_date` — `US 10Y TIPS` starts 2025-07-07
    /// against the United States' 1990-01-08. Checking the country's span instead of this one is
    /// exactly the mistake that produces an unexplained empty result.
    pub first_date: String,
    /// Latest observation for this tenor, spelled `YYYY-MM-DD`. Parse with
    /// [`last_date_value`](Self::last_date_value).
    pub last_date: String,
    /// Observations published for this tenor.
    pub observations: u64,
}

impl LseBondYieldMaturity {
    /// Parses [`first_date`](Self::first_date).
    ///
    /// # Errors
    /// Returns [`LseError::Deserialize`] if the value is not in the provider's `YYYY-MM-DD`
    /// spelling.
    pub fn first_date_value(&self) -> Result<NaiveDate, LseError> {
        parse_date(&self.first_date, "first_date")
    }

    /// Parses [`last_date`](Self::last_date).
    ///
    /// # Errors
    /// Returns [`LseError::Deserialize`] if the value is not in the provider's `YYYY-MM-DD`
    /// spelling.
    pub fn last_date_value(&self) -> Result<NaiveDate, LseError> {
        parse_date(&self.last_date, "last_date")
    }
}

/// A bond-yield request.
///
/// `#[non_exhaustive]`: construct with [`new`](Self::new) and
/// [`with_maturity`](Self::with_maturity) so a parameter added alongside a future provider filter
/// does not break callers.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct LseBondYieldQuery {
    /// Country code, in any spelling [`LseBondYieldStats::normalise_country`] accepts.
    pub country: SmolStr,
    /// Tenor label, or `None` for **every** tenor the country publishes.
    ///
    /// Omitting it is a genuine bulk request: all of the United States over its full history is
    /// 76,406 rows across 15 tenors.
    pub maturity: Option<SmolStr>,
    /// First date of the requested window, inclusive.
    pub start: NaiveDate,
    /// Last date of the requested window, inclusive.
    pub end: NaiveDate,
}

impl LseBondYieldQuery {
    /// A query for every tenor a country publishes, over `[start, end]` inclusive.
    ///
    /// Narrow it to one tenor with [`with_maturity`](Self::with_maturity).
    #[must_use]
    pub fn new(country: impl Into<SmolStr>, start: NaiveDate, end: NaiveDate) -> Self {
        Self {
            country: country.into(),
            maturity: None,
            start,
            end,
        }
    }

    /// Restrict the query to one tenor, named as the provider labels it (`"5Y"`, `"10Y TIPS"`).
    #[must_use]
    pub fn with_maturity(mut self, maturity: impl Into<SmolStr>) -> Self {
        self.maturity = Some(maturity.into());
        self
    }

    /// Check this query against the provider's published coverage.
    ///
    /// Called unconditionally by
    /// [`fetch_bond_yields`](LseDataApiClient::fetch_bond_yields), so it cannot be skipped; it is
    /// public so a caller can screen a batch of queries without spending a request on each.
    ///
    /// Checks, in order: that the window is not inverted, that the country is published, that the
    /// tenor is published for it, and that the window **overlaps** that series' own coverage. A
    /// partial overlap passes — it returns the rows that exist, which is what was asked for; only a
    /// disjoint window is rejected.
    ///
    /// # Errors
    /// - [`LseError::InvalidInput`] if `end` precedes `start`.
    /// - [`LseError::UnknownBondYieldCountry`] if the country is not published.
    /// - [`LseError::UnknownBondYieldMaturity`] if the tenor is not published for that country.
    /// - [`LseError::BondYieldRangeOutsideCoverage`] if the window is disjoint from the series'.
    /// - [`LseError::Deserialize`] if a date in `stats` is not in the provider's spelling.
    pub fn validate(&self, stats: &LseBondYieldStats) -> Result<(), LseError> {
        self.resolve(stats).map(|_| ())
    }

    /// Validate, and return the normalised country code the request must actually be sent with.
    ///
    /// The normalisation is the point: sending the caller's `GB` unchanged is precisely the silent
    /// zero-row case this module exists to close.
    fn resolve(&self, stats: &LseBondYieldStats) -> Result<SmolStr, LseError> {
        if self.end < self.start {
            return Err(LseError::InvalidInput {
                message: format!(
                    "bond-yield range end {} precedes start {}",
                    self.end, self.start
                ),
            });
        }

        let normalised = LseBondYieldStats::normalise_country(&self.country);
        let country =
            stats
                .countries
                .get(&normalised)
                .ok_or_else(|| LseError::UnknownBondYieldCountry {
                    requested: self.country.to_string(),
                    normalised: normalised.to_string(),
                })?;

        // Coverage is per tenor where one is named, and per country otherwise. Reading the
        // country's span for a single tenor would accept `US 10Y TIPS` over 2023 -- the United
        // States reaches back to 1990, that tenor only to 2025-07-07 -- which is the exact query
        // that motivated range checking.
        let (first, last) = match &self.maturity {
            Some(tenor) => {
                let maturity =
                    country
                        .maturity(tenor)
                        .ok_or_else(|| LseError::UnknownBondYieldMaturity {
                            country: normalised.to_string(),
                            requested: tenor.to_string(),
                            available: country
                                .maturities
                                .iter()
                                .map(|maturity| maturity.tenor.to_string())
                                .collect(),
                        })?;

                (maturity.first_date_value()?, maturity.last_date_value()?)
            }
            None => (country.first_date_value()?, country.last_date_value()?),
        };

        if self.end < first || self.start > last {
            return Err(LseError::BondYieldRangeOutsideCoverage {
                country: normalised.to_string(),
                maturity: self.maturity.as_ref().map(ToString::to_string),
                start: self.start,
                end: self.end,
                first_date: first,
                last_date: last,
            });
        }

        Ok(normalised)
    }
}

/// The `{count, data}` envelope `/bond-yields` answers with.
///
/// `count` is carried so it can be checked against the rows actually delivered; see
/// [`fetch_bond_yields`](LseDataApiClient::fetch_bond_yields).
#[derive(Debug, Deserialize)]
struct LseBondYieldEnvelope {
    count: usize,
    data: Vec<LseBondYieldRow>,
}

impl LseDataApiClient {
    /// Fetch the provider's index of published bond-yield coverage.
    ///
    /// Fetch this **once** and reuse the handle across queries — see [`LseBondYieldStats`]. It is
    /// the required first argument to [`fetch_bond_yields`](Self::fetch_bond_yields), which is what
    /// makes a query checkable against an endpoint that validates nothing itself.
    ///
    /// # Errors
    /// See [`LseError`].
    pub async fn fetch_bond_yield_stats(&self) -> Result<LseBondYieldStats, LseError> {
        self.get_json(
            BOND_YIELDS_STATS_PATH,
            &[("format", JSON_FORMAT.to_string())],
        )
        .await
    }

    /// Fetch daily bond-yield observations for `query`.
    ///
    /// `stats` is **required**, not optional, and `query` is validated against it before anything is
    /// sent. That is a deliberate API choice: the endpoint answers `200` with zero rows for an
    /// unknown country, an unknown tenor *and* an empty window alike, so a fetch that could skip
    /// validation would be a fetch that could silently return nothing. See the
    /// [module documentation](self) for the measurements.
    ///
    /// Rows arrive ascending by date, oldest first, and the country code sent is the **normalised**
    /// one — a caller passing `GB` reaches the provider's `UK`.
    ///
    /// # The whole answer arrives in one response
    /// This endpoint is **not paged**: its envelope carries no cursor, and a request for the full
    /// published history of every United States tenor returned all 76,406 rows in a single 4-second
    /// response, matching `/bond-yields/stats` exactly. So this returns a `Vec` rather than a
    /// stream, and no pagination is invented for a surface that has none.
    ///
    /// The envelope's own `count` is checked against the rows delivered and a mismatch is an error
    /// rather than a short result — that is the signal a silent page cap would produce if the
    /// provider ever introduced one.
    ///
    /// # ⚠️ An unnarrowed query is a large request
    /// Omitting [`maturity`](LseBondYieldQuery::maturity) fetches every tenor, which for a
    /// deep country over its full history is tens of thousands of rows against the client's 30s
    /// total request timeout. Narrow the tenor or the window for anything latency-sensitive.
    ///
    /// # Errors
    /// Anything [`validate`](LseBondYieldQuery::validate) raises, plus [`LseError`]'s transport and
    /// decode variants. A response whose `count` disagrees with the rows delivered is reported as
    /// [`LseError::Api`] carrying the `200` the provider actually sent.
    pub async fn fetch_bond_yields(
        &self,
        stats: &LseBondYieldStats,
        query: &LseBondYieldQuery,
    ) -> Result<Vec<LseBondYieldRow>, LseError> {
        let country = query.resolve(stats)?;

        let mut params = vec![
            ("country", country.to_string()),
            ("start_date", query.start.format(DATE_FORMAT).to_string()),
            ("end_date", query.end.format(DATE_FORMAT).to_string()),
            // Absent, this endpoint serves CSV.
            ("format", JSON_FORMAT.to_string()),
        ];

        // Omitted entirely rather than sent empty: an empty `maturity` is an unknown one, and this
        // endpoint answers an unknown filter with zero rows rather than an error.
        if let Some(maturity) = &query.maturity {
            params.push(("maturity", maturity.to_string()));
        }

        let envelope: LseBondYieldEnvelope = self.get_json(BOND_YIELDS_PATH, &params).await?;

        if envelope.count != envelope.data.len() {
            return Err(LseError::Api {
                status: 200,
                message: format!(
                    "bond-yield response declared {} rows but carried {}: the response appears to \
                     have been truncated",
                    envelope.count,
                    envelope.data.len()
                ),
            });
        }

        Ok(envelope.data)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    /// A stats handle shaped exactly like the provider's, with **invented** dates and counts.
    ///
    /// The provider prohibits redistributing its data, so nothing here is a recorded response. What
    /// is reproduced is the *structure* that the validation rules turn on:
    ///
    /// - the United Kingdom keyed `UK`, with no `GB` entry;
    /// - a tenor whose coverage starts far later than its country's (`US`/`10Y TIPS`), which is the
    ///   shape that makes membership-only validation wrong;
    /// - two tenors sharing a `days` value (`10Y` and `10Y TIPS`), which is the shape that makes
    ///   `maturity_days` unusable as a key.
    fn stats() -> LseBondYieldStats {
        serde_json::from_str(
            r#"{
                "total_countries": 2,
                "total_observations": 30,
                "columns": ["date", "symbol", "country_iso2", "country_name", "maturity",
                            "maturity_days", "currency", "open", "high", "low", "close"],
                "countries": {
                    "UK": {
                        "code": "UK", "name": "United Kingdom", "currency": "GBP",
                        "first_date": "2020-01-01", "last_date": "2024-12-31", "observations": 10,
                        "maturities": [
                            {"tenor": "5Y", "days": 1825, "first_date": "2020-01-01",
                             "last_date": "2024-12-31", "observations": 10}
                        ]
                    },
                    "US": {
                        "code": "US", "name": "United States", "currency": "USD",
                        "first_date": "2000-01-01", "last_date": "2024-12-31", "observations": 20,
                        "maturities": [
                            {"tenor": "10Y", "days": 3650, "first_date": "2000-01-01",
                             "last_date": "2024-12-31", "observations": 15},
                            {"tenor": "10Y TIPS", "days": 3650, "first_date": "2024-06-01",
                             "last_date": "2024-12-31", "observations": 5}
                        ]
                    }
                }
            }"#,
        )
        .unwrap()
    }

    fn date(year: i32, month: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(year, month, day).unwrap()
    }

    /// Every field on the wire is a JSON string, numerics included — the property that rules out
    /// the `rust_decimal::serde::float` helper the Alpaca corporate-action types use.
    #[test]
    fn a_row_decodes_with_every_field_arriving_as_a_string() {
        let row: LseBondYieldRow = serde_json::from_str(
            r#"{"date":"2024-03-14","symbol":"UK5Y","country_iso2":"UK",
                "country_name":"United Kingdom","maturity":"5Y","maturity_days":"1825",
                "currency":"GBP","open":"1.2500","high":"1.3750","low":"1.2000","close":"1.3125"}"#,
        )
        .unwrap();

        assert_eq!(row.open, dec!(1.2500));
        assert_eq!(row.high, dec!(1.3750));
        assert_eq!(row.low, dec!(1.2000));
        assert_eq!(row.close, dec!(1.3125));
        assert_eq!(row.observation_date().unwrap(), date(2024, 3, 14));
        assert_eq!(row.maturity_days_value().unwrap(), 1825);
    }

    /// A field the provider adds must not break decoding.
    #[test]
    fn a_row_ignores_an_unknown_field() {
        let row: LseBondYieldRow = serde_json::from_str(
            r#"{"date":"2024-03-14","symbol":"UK5Y","country_iso2":"UK",
                "country_name":"United Kingdom","maturity":"5Y","maturity_days":"1825",
                "currency":"GBP","open":"1.0","high":"1.0","low":"1.0","close":"1.0",
                "some_new_field":"whatever"}"#,
        )
        .unwrap();

        assert_eq!(row.symbol, "UK5Y");
    }

    /// The date is kept as the provider's string precisely so a malformed one fails at the row
    /// rather than taking a whole fetch down, so the parse must be fallible and must name the field.
    #[test]
    fn a_malformed_date_is_a_typed_error_naming_the_field() {
        let row = LseBondYieldRow {
            date: "14/03/2024".to_string(),
            symbol: "UK5Y".into(),
            country_iso2: "UK".into(),
            country_name: "United Kingdom".to_string(),
            maturity: "5Y".into(),
            maturity_days: "not-a-number".into(),
            currency: "GBP".into(),
            open: dec!(1),
            high: dec!(1),
            low: dec!(1),
            close: dec!(1),
        };

        let error = row.observation_date().unwrap_err();
        assert!(matches!(error, LseError::Deserialize { .. }));
        assert!(error.to_string().contains("date"), "{error}");

        let error = row.maturity_days_value().unwrap_err();
        assert!(matches!(error, LseError::Deserialize { .. }));
        assert!(error.to_string().contains("maturity_days"), "{error}");
    }

    /// The one divergence in the provider's country vocabulary, in both directions and in any case.
    #[test]
    fn gb_normalises_to_the_providers_uk_and_everything_else_is_untouched() {
        assert_eq!(LseBondYieldStats::normalise_country("GB"), "UK");
        assert_eq!(LseBondYieldStats::normalise_country("gb"), "UK");
        assert_eq!(LseBondYieldStats::normalise_country("  Gb "), "UK");
        assert_eq!(LseBondYieldStats::normalise_country("UK"), "UK");
        assert_eq!(LseBondYieldStats::normalise_country("us"), "US");
        assert_eq!(LseBondYieldStats::normalise_country("DE"), "DE");
    }

    /// The lookup applies the normalisation, so the ISO spelling a caller is likeliest to reach for
    /// finds the country instead of silently missing it.
    #[test]
    fn a_country_lookup_finds_the_uk_under_its_iso_spelling() {
        let stats = stats();

        assert_eq!(stats.country("GB").unwrap().name, "United Kingdom");
        assert_eq!(stats.country("gb").unwrap().name, "United Kingdom");
        assert_eq!(stats.country("UK").unwrap().name, "United Kingdom");
        assert!(stats.country("ZZ").is_none());
        // The vault's spelling is absent from the provider's own keys; only the lookup bridges them.
        assert!(!stats.countries.contains_key("GB"));
    }

    /// Tenors are matched case-insensitively, but the inner space is significant: the row `symbol`
    /// field drops it (`US10YTIPS`), so that form is not a tenor and must not resolve to one.
    #[test]
    fn a_tenor_matches_ignoring_case_but_not_ignoring_its_inner_space() {
        let stats = stats();
        let us = stats.country("US").unwrap();

        assert_eq!(us.maturity("10Y TIPS").unwrap().tenor, "10Y TIPS");
        assert_eq!(us.maturity("10y tips").unwrap().tenor, "10Y TIPS");
        assert_eq!(us.maturity(" 10Y TIPS ").unwrap().tenor, "10Y TIPS");
        assert!(us.maturity("10YTIPS").is_none());
        assert!(us.maturity("99Y").is_none());
    }

    /// The measured collision that rules out keying on the day count.
    #[test]
    fn two_tenors_of_one_country_share_a_day_count() {
        let stats = stats();
        let us = stats.country("US").unwrap();

        assert_eq!(us.maturity("10Y").unwrap().days, 3650);
        assert_eq!(us.maturity("10Y TIPS").unwrap().days, 3650);
    }

    #[test]
    fn a_valid_query_passes() {
        let stats = stats();
        let query =
            LseBondYieldQuery::new("UK", date(2021, 1, 1), date(2022, 1, 1)).with_maturity("5Y");

        assert!(query.validate(&stats).is_ok());
    }

    #[test]
    fn an_inverted_range_is_rejected() {
        let stats = stats();
        let query = LseBondYieldQuery::new("UK", date(2022, 1, 1), date(2021, 1, 1));

        assert!(matches!(
            query.validate(&stats).unwrap_err(),
            LseError::InvalidInput { .. }
        ));
    }

    /// `country=ZZ` answers `200 count=0` at the provider, so the rejection has to happen here.
    #[test]
    fn an_unknown_country_is_rejected_before_the_request_is_sent() {
        let stats = stats();
        let query = LseBondYieldQuery::new("ZZ", date(2021, 1, 1), date(2022, 1, 1));

        let error = query.validate(&stats).unwrap_err();
        assert!(matches!(error, LseError::UnknownBondYieldCountry { .. }));
        // Both spellings are reported so a caller can see which one was looked up.
        let rendered = error.to_string();
        assert!(rendered.contains("ZZ"), "{rendered}");
    }

    /// The error names the normalised form, so a caller passing `GB` for a country the provider
    /// genuinely lacks is not left wondering which string was searched for.
    #[test]
    fn an_unknown_country_error_reports_both_the_requested_and_the_normalised_spelling() {
        let stats = stats();
        let query = LseBondYieldQuery::new("gb-typo", date(2021, 1, 1), date(2022, 1, 1));

        match query.validate(&stats).unwrap_err() {
            LseError::UnknownBondYieldCountry {
                requested,
                normalised,
            } => {
                assert_eq!(requested, "gb-typo");
                assert_eq!(normalised, "GB-TYPO");
            }
            other => panic!("expected UnknownBondYieldCountry, got {other}"),
        }
    }

    /// `maturity=99Y` also answers `200 count=0`, and the error carries the list a caller needs
    /// because a tenor is a label there is no way to compute.
    #[test]
    fn an_unknown_maturity_is_rejected_and_lists_the_published_tenors() {
        let stats = stats();
        let query =
            LseBondYieldQuery::new("US", date(2021, 1, 1), date(2022, 1, 1)).with_maturity("99Y");

        match query.validate(&stats).unwrap_err() {
            LseError::UnknownBondYieldMaturity {
                country,
                requested,
                available,
            } => {
                assert_eq!(country, "US");
                assert_eq!(requested, "99Y");
                assert_eq!(available, vec!["10Y".to_string(), "10Y TIPS".to_string()]);
            }
            other => panic!("expected UnknownBondYieldMaturity, got {other}"),
        }
    }

    /// 🔴 The decisive case. `US 10Y TIPS` **exists**, and the United States spans 2000–2024, so a
    /// membership-only validator passes this query — and the provider answers `200 count=0` because
    /// that tenor does not start until 2024-06-01. Coverage must be read per tenor, not per country.
    #[test]
    fn a_range_before_a_tenors_own_coverage_is_rejected_though_the_country_spans_it() {
        let stats = stats();
        let us = stats.country("US").unwrap();

        // The premise: the country covers the window, the tenor does not.
        assert!(us.first_date_value().unwrap() < date(2021, 1, 1));

        let query = LseBondYieldQuery::new("US", date(2021, 1, 1), date(2021, 12, 31))
            .with_maturity("10Y TIPS");

        match query.validate(&stats).unwrap_err() {
            LseError::BondYieldRangeOutsideCoverage {
                country,
                maturity,
                first_date,
                ..
            } => {
                assert_eq!(country, "US");
                assert_eq!(maturity.as_deref(), Some("10Y TIPS"));
                assert_eq!(first_date, date(2024, 6, 1));
            }
            other => panic!("expected BondYieldRangeOutsideCoverage, got {other}"),
        }

        // The same window against the nominal tenor is fine, which is what makes the rejection
        // above about the tenor rather than about the window.
        assert!(
            LseBondYieldQuery::new("US", date(2021, 1, 1), date(2021, 12, 31))
                .with_maturity("10Y")
                .validate(&stats)
                .is_ok()
        );
    }

    #[test]
    fn a_range_after_the_published_coverage_is_rejected() {
        let stats = stats();
        let query = LseBondYieldQuery::new("UK", date(2030, 1, 1), date(2031, 1, 1));

        assert!(matches!(
            query.validate(&stats).unwrap_err(),
            LseError::BondYieldRangeOutsideCoverage { .. }
        ));
    }

    /// A window that only partly overlaps returns the rows that exist, which is what was asked for.
    /// Rejecting it would make the validator refuse legitimate open-ended requests.
    #[test]
    fn a_partially_overlapping_range_passes() {
        let stats = stats();

        // Starts before coverage, ends inside it.
        assert!(
            LseBondYieldQuery::new("UK", date(1990, 1, 1), date(2021, 1, 1))
                .validate(&stats)
                .is_ok()
        );
        // Starts inside coverage, ends after it.
        assert!(
            LseBondYieldQuery::new("UK", date(2024, 1, 1), date(2099, 1, 1))
                .validate(&stats)
                .is_ok()
        );
        // A single day exactly on the boundary.
        assert!(
            LseBondYieldQuery::new("UK", date(2020, 1, 1), date(2020, 1, 1))
                .validate(&stats)
                .is_ok()
        );
    }

    /// Without a tenor the bound is the country's own span, which is wider than any single tenor's.
    #[test]
    fn a_query_without_a_maturity_is_bounded_by_the_countrys_own_coverage() {
        let stats = stats();

        // Inside the country's span but before the `10Y TIPS` tenor's — allowed, because the query
        // asks for every tenor and the nominal one does cover it.
        assert!(
            LseBondYieldQuery::new("US", date(2021, 1, 1), date(2021, 12, 31))
                .validate(&stats)
                .is_ok()
        );

        match LseBondYieldQuery::new("US", date(1950, 1, 1), date(1960, 1, 1))
            .validate(&stats)
            .unwrap_err()
        {
            LseError::BondYieldRangeOutsideCoverage { maturity, .. } => assert!(maturity.is_none()),
            other => panic!("expected BondYieldRangeOutsideCoverage, got {other}"),
        }
    }

    /// The caller's spelling must not reach the wire: `GB` has to become `UK` or the request comes
    /// back empty with a `200`.
    #[test]
    fn validation_yields_the_normalised_country_code_the_request_is_sent_with() {
        let stats = stats();
        let query = LseBondYieldQuery::new("GB", date(2021, 1, 1), date(2022, 1, 1));

        assert_eq!(query.resolve(&stats).unwrap(), "UK");
    }

    #[test]
    fn stats_decode_with_their_integer_typed_counts() {
        let stats = stats();

        assert_eq!(stats.total_countries, 2);
        assert_eq!(stats.total_observations, 30);
        assert_eq!(stats.columns.len(), 11);
        assert_eq!(stats.columns.first().unwrap(), "date");
        assert_eq!(stats.country("US").unwrap().observations, 20);
        assert_eq!(stats.country("US").unwrap().maturities.len(), 2);
    }

    /// The stats dates are the provider's strings, so a malformed one must surface as a typed
    /// decode error from validation rather than being silently treated as no constraint.
    #[test]
    fn a_malformed_stats_date_fails_validation_rather_than_being_ignored() {
        let mut stats = stats();
        stats
            .countries
            .get_mut("UK")
            .unwrap()
            .maturities
            .get_mut(0)
            .unwrap()
            .first_date = "not-a-date".to_string();

        let query =
            LseBondYieldQuery::new("UK", date(2021, 1, 1), date(2022, 1, 1)).with_maturity("5Y");

        assert!(matches!(
            query.validate(&stats).unwrap_err(),
            LseError::Deserialize { .. }
        ));
    }
}
