//! Economic calendar events from the London Strategic Edge data API
//! (`GET /economic-calendar`).
//!
//! Scheduled macroeconomic releases — CPI prints, rate decisions, payrolls — across 108 country
//! codes, each carrying the consensus estimate, the previous figure and the actual outcome where
//! the provider has them. Served by the [data API](super::data_api) host, alongside
//! [`bond_yield`](super::bond_yield).
//!
//! ```no_run
//! # use chrono::NaiveDate;
//! # use rustrade_data::exchange::lse::calendar::LseCalendarQuery;
//! # use rustrade_data::exchange::lse::data_api::LseDataApiClient;
//! # use rustrade_data::exchange::lse::error::LseError;
//! # async fn example() -> Result<(), LseError> {
//! let client = LseDataApiClient::from_env()?;
//!
//! // The stats handle is fetched once and reused: it is what makes a query checkable.
//! let stats = client.fetch_economic_calendar_stats().await?;
//!
//! let query = LseCalendarQuery::new(
//!     NaiveDate::from_ymd_opt(2026, 1, 1).expect("valid date"),
//!     NaiveDate::from_ymd_opt(2026, 3, 24).expect("valid date"),
//! )
//! .with_country("GB") // normalised to the provider's `UK` before the request is sent
//! .with_impact("High");
//!
//! for event in client.fetch_economic_calendar(&stats, &query).await? {
//!     println!("{} {} {}", event.event_date, event.country, event.event);
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # 🔴 The feed stopped on 2026-03-24 — this is a historical archive, not a calendar
//!
//! **The forward-looking use case an economic calendar exists for is not served at all.** The
//! provider's last event is dated **2026-03-24**, and nothing has been published since. This is
//! not staleness that a later fetch resolves — it was measured as frozen **to the unit**, two
//! months of wall-clock apart:
//!
//! | measured | 2026-07-24 | 2026-09-22 |
//! |---|---|---|
//! | `total_events` | 124,896 | 124,896 |
//! | `latest` | 2026-03-24 | 2026-03-24 |
//!
//! Confirmed from the other side as well: a forward window of `2026-03-25`..=`2030-12-31` with no
//! country filter answers `200` with `count: 0`, as does any window after `latest`, while
//! `2026-03-24` itself returns 17 events. Zero events exist after that date across all 108
//! countries. Nothing on the provider's issue tracker reports it, so no fix should be assumed.
//!
//! What remains is a genuine and complete historical archive: **124,896 events from 2014-12-31 to
//! 2026-03-24**, with an `estimate` populated on 74,843 of them. That is useful for backtesting
//! and for event-study work, and it is what this module is for. It is not useful for anything that
//! needs to know what releases are coming, and no amount of client-side care changes that.
//!
//! [`LseCalendarStats::latest`] is the live figure rather than a constant here, so a caller can
//! detect a revival: if it ever moves past 2026-03-24, the feed resumed.
//!
//! # ⚠️ The endpoint validates nothing — a wrong query is a `200` with zero rows
//!
//! As with [`bond_yield`](super::bond_yield), and for the same reason
//! [`fetch_economic_calendar`](LseDataApiClient::fetch_economic_calendar) takes an
//! [`LseCalendarStats`] as a **required** argument. Measured against the live host:
//!
//! | request | answer |
//! |---|---|
//! | `country=UK` | `200`, rows |
//! | `country=GB` (ISO-3166 spelling) | `200`, `count: 0` |
//! | `country=ZZ` (nonsense) | `200`, `count: 0` |
//! | `impact=Critical` (nonsense) | `200`, `count: 0` |
//! | a reversed range | `200`, `count: 0` |
//! | any window after 2026-03-24 | `200`, `count: 0` |
//!
//! All of those zero-row answers are identical. The one query shape that does *not* answer `200`
//! is a malformed date — `start_date=not-a-date` returns **`500 Internal Server Error`**, not a
//! `400`. [`LseCalendarQuery`] holds typed [`NaiveDate`]s, which makes that unreachable.
//!
//! # 🔴 But an empty result inside the published range is NOT an error
//!
//! This is where the calendar differs from `/bond-yields`, and the difference is load-bearing.
//! `/bond-yields/stats` publishes per-country **and** per-tenor `first_date`/`last_date`, so a
//! window that would return nothing can be rejected before it is sent.
//! `/economic-calendar/stats` publishes **no per-country coverage at all** — only a flat global
//! `earliest`/`latest` — and coverage is wildly uneven: 88 of the 108 countries hold fewer than
//! 100 events, the median is 13, and `UK` holds 56, all between 2026-01-16 and 2026-03-24.
//!
//! So `UK` over 2020–2025 returns zero events. That is correct, it is unhelpful, and **this
//! library cannot pre-explain it**, because the provider does not publish the coverage that would
//! be needed to. An empty [`Vec`] from a query inside the global range is a legitimate answer and
//! is deliberately **not** raised as an error. Inventing a fourth error variant here would mean
//! inventing knowledge the provider does not give.
//!
//! # ⚠️ Two silent parameter traps
//!
//! 1. **A comma-separated country list is a real OR, and is the only form that works.**
//!    `country=US,UK` returns 36,477 events — exactly US alone (36,421) plus UK alone (56).
//!    Supported, and undocumented by the provider.
//! 2. **🔴 A repeated parameter silently takes the LAST value.** `country=US&country=UK` returns
//!    56 events: the UK ones. The US rows vanish with no error and no warning, leaving a wrong but
//!    entirely plausible result. A naive `.query(&[("country", "US"), ("country", "UK")])` hits
//!    this exactly.
//!
//! [`LseCalendarQuery`] joins with commas and never repeats the key. Nothing here lets a caller
//! express the broken form.
//!
//! # ⚠️ `country` is not ISO-3166, and not every code is a country
//!
//! The same `UK`-not-`GB` divergence as [`bond_yield`](super::bond_yield): `UK` is published and
//! `GB` is absent, so the one documented alias carries over and
//! [`LseCalendarStats::normalise_country`] applies it. But **the two vocabularies are not the
//! same set** — the calendar's 108 codes include `EA` (Euro Area) and `EU`, which are not
//! countries at all. Do not assume a code valid on one endpoint is valid on the other.
//!
//! Both hosts are case- and whitespace-insensitive server-side, so `uk`, `UK` and `'  Uk  '` all
//! resolve on the wire. Normalisation is still required, for a different reason than it appears:
//! the **client-side membership check** runs against an upper-case vocabulary, and only the client
//! can turn `GB` into `UK` at all.
//!
//! # ⚠️ `impact` of `"None"` is a literal string, not a JSON null
//!
//! The vocabulary is exactly `High`, `Low`, `Medium`, `None`, and 592 events carry `"None"` as a
//! four-character string. Mapping it to an absent value would be wrong, so
//! [`LseCalendarImpact`] models it as the variant [`None`](LseCalendarImpact::None) and the field
//! is not an [`Option`].
//!
//! # ⚠️ Licensing — this data may not be redistributed
//!
//! Calendar events are provider data. They may be used for your own research, trading and model
//! training, including commercially, but **not** redistributed or re-served to third parties in
//! any form. Do not commit responses as fixtures or example datasets. Terms:
//! <https://londonstrategicedge.com/terms>

use crate::exchange::lse::data_api::LseDataApiClient;
use crate::exchange::lse::error::LseError;
use chrono::{DateTime, NaiveDate, Utc};
use rust_decimal::Decimal;
use serde::Deserialize;
use smol_str::SmolStr;

/// Path of the economic-calendar events endpoint, relative to the data API base URL.
const ECONOMIC_CALENDAR_PATH: &str = "economic-calendar";

/// Path of the economic-calendar stats endpoint, relative to the data API base URL.
const ECONOMIC_CALENDAR_STATS_PATH: &str = "economic-calendar/stats";

/// Spelling of the `start_date`/`end_date` query parameters, and of the stats bounds.
///
/// ⚠️ **Not** the spelling of a row's [`event_date`](LseCalendarEvent::event_date), which carries a
/// time of day and an offset. The query is bounded by whole days; an event is stamped to the
/// second.
const DATE_FORMAT: &str = "%Y-%m-%d";

/// Value of the `format` parameter that selects JSON.
///
/// ⚠️ **Required, not a default.** The provider's own OpenAPI document declares `csv` as this
/// parameter's default, and the wire agrees: omitting it — or passing anything other than the
/// exact string `json`, which is *not* validated — hands a CSV body to a JSON decoder.
const JSON_FORMAT: &str = "json";

/// Separator for a multi-country filter.
///
/// ⚠️ Load-bearing. See the [module documentation](self): repeating the `country` key instead
/// silently discards every value but the last.
const COUNTRY_SEPARATOR: &str = ",";

/// The provider's spelling of the United Kingdom, which is not its ISO-3166-1 alpha-2 code.
const UK_CODE: &str = "UK";

/// The ISO-3166-1 alpha-2 code for the United Kingdom, which this host does **not** use.
const GB_CODE: &str = "GB";

/// Parse a provider date, naming the field so a caller can tell which one failed.
fn parse_date(value: &str, field: &str) -> Result<NaiveDate, LseError> {
    NaiveDate::parse_from_str(value, DATE_FORMAT).map_err(|error| LseError::Deserialize {
        message: format!("invalid economic-calendar {field} {value:?}: {error}"),
    })
}

/// How much market impact the provider attributes to an event.
///
/// # ⚠️ [`None`](Self::None) is the provider's own literal string
///
/// The wire vocabulary is exactly `["High", "Low", "Medium", "None"]`, and `"None"` is a
/// four-character JSON string carried by 592 of the 124,896 measured events — it is **not** a JSON
/// `null`, and no event omits the field. That is why [`LseCalendarEvent::impact`] is this type
/// rather than an `Option<_>`: an `Option` would make `Some(Impact::None)` and `None`
/// indistinguishable in intent while meaning quite different things.
///
/// The set is closed at four. An unrecognised value is a decode error rather than a silently
/// absorbed variant, so a provider addition surfaces instead of being rounded to the nearest
/// known rating.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize)]
pub enum LseCalendarImpact {
    /// The provider's `"High"`.
    High,
    /// The provider's `"Medium"`.
    Medium,
    /// The provider's `"Low"`.
    Low,
    /// The provider's literal `"None"` — an event it rates as market-irrelevant.
    ///
    /// ⚠️ Not an absent value. See the type's documentation.
    None,
}

impl LseCalendarImpact {
    /// The provider's spelling of this rating, suitable for the `impact` query parameter.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::High => "High",
            Self::Medium => "Medium",
            Self::Low => "Low",
            Self::None => "None",
        }
    }
}

impl std::fmt::Display for LseCalendarImpact {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One scheduled economic release, as the data API serves it.
///
/// # ⚠️ Every numeric arrives as a JSON **string**, and absent is always `null`
///
/// `{"event_date": "2026-03-24 14:00:00+00:00", "previous": "2.9", "estimate": null, …}` — there
/// is not a single JSON number in an event. The numerics are therefore decoded with
/// [`rust_decimal::serde::str_option`], **not** the `rust_decimal::serde::float` used by
/// `AlpacaStockSplit`, which expects a JSON number and fails outright here.
///
/// Absence is spelled `null` and **only** `null`: measured across all 124,896 events, the
/// empty-string count is **zero for every one of the eleven fields**. Nulls themselves are common
/// — `estimate` on 50,053 events, `unit` on 37,349, `change` on 25,481, `currency` on 253 — so the
/// optionality is real even though the empty string never appears.
///
/// Every one of roughly 462,000 non-null numeric values parses. The only shapes that occur are
/// `9.9`, `-9.9`, `9` and `-9`: no thousands separators, no trailing `%`, no scientific notation.
/// **Negatives are abundant and load-bearing** (49,080 on `change`, 48,718 on `change_percentage`),
/// so an unsigned type would be wrong.
///
/// All eleven fields are present on every event — there is exactly one key-set across the whole
/// corpus — so none is `#[serde(default)]`. A field the provider drops is a decode failure rather
/// than a silent `None`, which is the intended trade.
///
/// Unknown fields are ignored, so a field the provider adds does not break decoding.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct LseCalendarEvent {
    /// When the release is scheduled, spelled `2026-03-24 14:00:00+00:00`.
    ///
    /// # ⚠️ A timestamp, not a date
    ///
    /// Unlike [`LseBondYieldRow::date`](super::bond_yield::LseBondYieldRow::date), which is a bare
    /// ten-character day, this carries a real time of day: 100% of the 124,896 measured events
    /// match `YYYY-MM-DD HH:MM:SS+00:00`, the offset is `+00:00` on every one, and **348 distinct
    /// times of day** occur. Truncating it to a calendar date would discard the intraday ordering
    /// that makes an event study possible, so [`event_time`](Self::event_time) yields a
    /// [`DateTime<Utc>`].
    ///
    /// Kept as the provider's string and parsed on demand, following the same rule as
    /// [`LseCatalogEntry`](super::reference::LseCatalogEntry) and `LseBondYieldRow`: one malformed
    /// event then fails at the event rather than taking the whole fetch down with it.
    pub event_date: String,
    /// Country code as this host spells it.
    ///
    /// ⚠️ Not ISO-3166: the United Kingdom is `UK`, not `GB`, and `EA`/`EU` are not countries at
    /// all. See the [module documentation](self).
    pub country: SmolStr,
    /// The release's name, as the provider labels it (`"CPI (YoY)"`, `"Interest Rate Decision"`).
    ///
    /// Free text, not a closed vocabulary. `(event_date, country, event)` is a natural key — all
    /// 124,896 measured triples are distinct — but it is the *provider's* shape, not a reason to
    /// key a local store on it.
    pub event: String,
    /// Currency the figures are denominated in (`"GBP"`), where one applies.
    ///
    /// `None` on 253 events; 83 distinct values otherwise.
    pub currency: Option<SmolStr>,
    /// The previously reported figure, where the provider has it.
    #[serde(with = "rust_decimal::serde::str_option")]
    pub previous: Option<Decimal>,
    /// The consensus forecast ahead of the release.
    ///
    /// Populated on 74,843 of the 124,896 measured events — the single most useful field here for
    /// surprise-based work, and absent on two fifths of the corpus.
    #[serde(with = "rust_decimal::serde::str_option")]
    pub estimate: Option<Decimal>,
    /// The figure actually released.
    #[serde(with = "rust_decimal::serde::str_option")]
    pub actual: Option<Decimal>,
    /// Absolute change from [`previous`](Self::previous) to [`actual`](Self::actual).
    ///
    /// Provider-computed, not derived here. Frequently negative.
    #[serde(with = "rust_decimal::serde::str_option")]
    pub change: Option<Decimal>,
    /// Relative change from [`previous`](Self::previous) to [`actual`](Self::actual), in percent.
    ///
    /// Provider-computed, and carried as a bare number: the `%` is in
    /// [`unit`](Self::unit) or nowhere, never in this value.
    #[serde(with = "rust_decimal::serde::str_option")]
    pub change_percentage: Option<Decimal>,
    /// The provider's market-impact rating.
    ///
    /// ⚠️ Never absent, and [`LseCalendarImpact::None`] is a real rating rather than a missing
    /// one. See [`LseCalendarImpact`].
    pub impact: LseCalendarImpact,
    /// Magnitude or unit suffix for the figures (`"%"`, `"K"`, `"M"`, `"B"`, `"T"`).
    ///
    /// A closed set of five in the measured corpus — `%` on 65,018 events, `B` on 10,972, `K` on
    /// 5,990, `M` on 5,108, `T` on 459 — plus 37,349 nulls.
    ///
    /// ⚠️ **A different vocabulary from the catalog's `unit`**, which names a quote currency or an
    /// instrument denomination. The two must not share a type, and a value here must not be looked
    /// up against that one. Left as the provider's string rather than modelled as an enum: it is
    /// not validated anywhere and carries no behaviour, so a sixth suffix should arrive as data
    /// rather than as a decode failure.
    pub unit: Option<SmolStr>,
}

impl LseCalendarEvent {
    /// Parses [`event_date`](Self::event_date) as an instant.
    ///
    /// The provider's space-separated spelling (`2026-03-24 14:00:00+00:00`) is accepted by
    /// [`DateTime::parse_from_rfc3339`] directly, as the RFC 3339 §5.6 lenience it is — the same
    /// path [`LseTick`](super::tick::LseTick) takes for the vault's timestamps. No second grammar
    /// is used, so the two cannot drift apart in what they admit.
    ///
    /// # Errors
    /// Returns [`LseError::Deserialize`] if the value is not a valid RFC 3339 instant.
    pub fn event_time(&self) -> Result<DateTime<Utc>, LseError> {
        DateTime::parse_from_rfc3339(&self.event_date)
            .map(|parsed| parsed.with_timezone(&Utc))
            .map_err(|error| LseError::Deserialize {
                message: format!(
                    "invalid economic-calendar event_date {:?}: {error}",
                    self.event_date
                ),
            })
    }

    /// The calendar day of [`event_date`](Self::event_date), in UTC.
    ///
    /// A convenience for grouping; prefer [`event_time`](Self::event_time) for ordering, since 348
    /// distinct times of day occur and a day alone does not order events within one.
    ///
    /// # Errors
    /// As [`event_time`](Self::event_time).
    pub fn event_day(&self) -> Result<NaiveDate, LseError> {
        Ok(self.event_time()?.date_naive())
    }
}

/// The provider's index of what it publishes on `/economic-calendar`.
///
/// Fetched by [`fetch_economic_calendar_stats`](LseDataApiClient::fetch_economic_calendar_stats)
/// and passed to [`fetch_economic_calendar`](LseDataApiClient::fetch_economic_calendar), which is
/// what makes a query checkable against an endpoint that validates nothing itself.
///
/// # 🔴 Much weaker than `/bond-yields/stats`, and that shapes the whole error design
///
/// This is a **flat** summary: a list of country codes, a list of impact ratings, and one global
/// `earliest`/`latest` pair. There is **no per-country coverage** — unlike the bond-yield stats,
/// which publish `first_date`/`last_date` per country *and* per tenor.
///
/// The consequence is that only three checks are possible before a request is sent: that a country
/// is published, that an impact rating is published, and that the window overlaps the **global**
/// range. A query inside that global range can still legitimately return nothing, and the library
/// deliberately does not treat that as an error. See the [module documentation](self).
///
/// # Fetch it once
/// It is a description of the provider's coverage, not of any one query, so one handle serves every
/// fetch. Nothing here refreshes it.
///
/// # It is also the revival detector
/// [`latest`](Self::latest) is the live figure. The feed has been frozen at 2026-03-24 across two
/// measurements two months apart; if this ever reports a later date, the feed resumed.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct LseCalendarStats {
    /// Every country code the provider publishes events for. Measured at 108.
    ///
    /// ⚠️ Contains `UK` and not `GB`, and contains `EA` and `EU`, which are not countries. Test
    /// membership through [`publishes_country`](Self::publishes_country) rather than searching
    /// this directly, so the normalisation is applied.
    pub countries: Vec<SmolStr>,
    /// Every impact rating in use. Measured as exactly `["High", "Low", "Medium", "None"]`.
    ///
    /// ⚠️ `"None"` is a literal string here too — see [`LseCalendarImpact`].
    pub impacts: Vec<SmolStr>,
    /// Date of the earliest published event, spelled `YYYY-MM-DD`. Measured at `2014-12-31`.
    ///
    /// Parse with [`earliest_date`](Self::earliest_date).
    pub earliest: String,
    /// Date of the latest published event, spelled `YYYY-MM-DD`. Measured at `2026-03-24`.
    ///
    /// 🔴 The feed has not moved past this date since it was first measured. Parse with
    /// [`latest_date`](Self::latest_date); compare it against 2026-03-24 to detect a revival.
    pub latest: String,
    /// Total events across every country. Measured at 124,896, unchanged over two months.
    pub total_events: u64,
}

impl LseCalendarStats {
    /// Translates a country code into the provider's spelling.
    ///
    /// Trims, upper-cases, and maps the ISO-3166-1 alpha-2 `GB` to the provider's `UK`.
    ///
    /// # Why normalise at all, when the server does not care
    /// The host is measured to be case- and whitespace-insensitive: `country=uk` and
    /// `country='  Uk  '` both resolve on the wire. The trim and upper-case are not what gets a
    /// code accepted — they are what lets the **client-side** membership check run against an
    /// upper-case vocabulary. The `GB`→`UK` mapping is different in kind: the server would answer
    /// `GB` with a silent `200 count=0`, and only the client can bridge it.
    ///
    /// # Why exactly one alias
    /// The United Kingdom is the only divergence from ISO-3166-1 alpha-2 among the published
    /// codes, matching the [`bond_yield`](super::bond_yield) endpoint. A general alias table would
    /// invent mappings the provider has not been measured to want.
    #[must_use]
    pub fn normalise_country(code: &str) -> SmolStr {
        let upper = code.trim().to_ascii_uppercase();

        if upper == GB_CODE {
            SmolStr::new_static(UK_CODE)
        } else {
            SmolStr::from(upper)
        }
    }

    /// Whether the provider publishes events for a country, in any spelling
    /// [`normalise_country`](Self::normalise_country) accepts.
    ///
    /// So `"gb"`, `"GB"` and `"UK"` all find the United Kingdom.
    #[must_use]
    pub fn publishes_country(&self, code: &str) -> bool {
        let normalised = Self::normalise_country(code);

        self.countries.iter().any(|known| known == &normalised)
    }

    /// Resolves an impact rating to the provider's own spelling, ignoring case.
    ///
    /// Returns `None` if the provider does not publish that rating. The provider's spelling is
    /// returned rather than the caller's so the request carries a value from the published
    /// vocabulary verbatim.
    #[must_use]
    pub fn resolve_impact(&self, impact: &str) -> Option<&SmolStr> {
        let wanted = impact.trim();

        self.impacts
            .iter()
            .find(|known| known.eq_ignore_ascii_case(wanted))
    }

    /// Parses [`earliest`](Self::earliest) as a calendar date.
    ///
    /// # Errors
    /// Returns [`LseError::Deserialize`] if the value is not in the provider's `YYYY-MM-DD`
    /// spelling.
    pub fn earliest_date(&self) -> Result<NaiveDate, LseError> {
        parse_date(&self.earliest, "earliest")
    }

    /// Parses [`latest`](Self::latest) as a calendar date.
    ///
    /// # Errors
    /// Returns [`LseError::Deserialize`] if the value is not in the provider's `YYYY-MM-DD`
    /// spelling.
    pub fn latest_date(&self) -> Result<NaiveDate, LseError> {
        parse_date(&self.latest, "latest")
    }
}

/// An economic-calendar request.
///
/// `#[non_exhaustive]`: construct with [`new`](Self::new) and the `with_*` builders so a parameter
/// added alongside a future provider filter does not break callers.
///
/// # ⚠️ Countries are joined with commas, never repeated
/// The provider accepts `country=US,UK` as a genuine OR, and silently keeps only the **last** of
/// `country=US&country=UK`. This type only ever emits the first form. See the
/// [module documentation](self).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct LseCalendarQuery {
    /// Country codes, in any spelling [`LseCalendarStats::normalise_country`] accepts.
    ///
    /// Empty means **every** country, which is a genuine bulk request: the whole corpus is 124,896
    /// events in a single 29 MB response.
    pub countries: Vec<SmolStr>,
    /// Impact rating to filter on, or `None` for every rating.
    pub impact: Option<SmolStr>,
    /// First date of the requested window, inclusive.
    pub start: NaiveDate,
    /// Last date of the requested window, inclusive.
    pub end: NaiveDate,
}

impl LseCalendarQuery {
    /// A query for every country and every impact rating, over `[start, end]` inclusive.
    ///
    /// Narrow it with [`with_country`](Self::with_country),
    /// [`with_countries`](Self::with_countries) and [`with_impact`](Self::with_impact).
    #[must_use]
    pub fn new(start: NaiveDate, end: NaiveDate) -> Self {
        Self {
            countries: Vec::new(),
            impact: None,
            start,
            end,
        }
    }

    /// Add one country to the filter.
    ///
    /// Additive: calling it twice asks for both, sent as one comma-joined parameter.
    #[must_use]
    pub fn with_country(mut self, country: impl Into<SmolStr>) -> Self {
        self.countries.push(country.into());
        self
    }

    /// Add several countries to the filter.
    ///
    /// Additive, like [`with_country`](Self::with_country).
    #[must_use]
    pub fn with_countries<I, C>(mut self, countries: I) -> Self
    where
        I: IntoIterator<Item = C>,
        C: Into<SmolStr>,
    {
        self.countries.extend(countries.into_iter().map(Into::into));
        self
    }

    /// Restrict the query to one impact rating, named as the provider labels it (`"High"`).
    ///
    /// Matched against the published vocabulary ignoring case, and sent in the provider's own
    /// spelling. Replaces any rating set earlier.
    #[must_use]
    pub fn with_impact(mut self, impact: impl Into<SmolStr>) -> Self {
        self.impact = Some(impact.into());
        self
    }

    /// Check this query against the provider's published coverage.
    ///
    /// Called unconditionally by
    /// [`fetch_economic_calendar`](LseDataApiClient::fetch_economic_calendar), so it cannot be
    /// skipped; it is public so a caller can screen a batch of queries without spending a request
    /// on each.
    ///
    /// Checks, in order: that the window is not inverted, that every country named is published,
    /// that the impact rating is published, and that the window **overlaps** the provider's global
    /// range. A partial overlap passes — it returns the events that exist, which is what was asked
    /// for; only a disjoint window is rejected.
    ///
    /// # ⚠️ What it deliberately does not check
    /// Whether *this* country has events in *this* window. The provider publishes no per-country
    /// coverage, so that is unknowable before the request; an empty result inside the global range
    /// is a legitimate answer rather than an error. See the [module documentation](self).
    ///
    /// # Errors
    /// - [`LseError::InvalidInput`] if `end` precedes `start`.
    /// - [`LseError::UnknownCalendarCountry`] if a country is not published.
    /// - [`LseError::UnknownCalendarImpact`] if the impact rating is not published.
    /// - [`LseError::CalendarRangeOutsideCoverage`] if the window is disjoint from the global one.
    /// - [`LseError::Deserialize`] if a date in `stats` is not in the provider's spelling.
    pub fn validate(&self, stats: &LseCalendarStats) -> Result<(), LseError> {
        self.resolve(stats).map(|_| ())
    }

    /// Validate, and return the query parameters the request must actually be sent with.
    ///
    /// Returning the parameters rather than rebuilding them at the call site is what guarantees the
    /// normalised country codes and the provider's own impact spelling are the ones that reach the
    /// wire — sending the caller's `GB` unchanged is precisely the silent zero-row case this module
    /// exists to close.
    fn resolve(&self, stats: &LseCalendarStats) -> Result<Vec<(&'static str, String)>, LseError> {
        if self.end < self.start {
            return Err(LseError::InvalidInput {
                message: format!(
                    "economic-calendar range end {} precedes start {}",
                    self.end, self.start
                ),
            });
        }

        let mut normalised = Vec::with_capacity(self.countries.len());
        for country in &self.countries {
            let code = LseCalendarStats::normalise_country(country);

            if !stats.countries.iter().any(|known| known == &code) {
                return Err(LseError::UnknownCalendarCountry {
                    requested: country.to_string(),
                    normalised: code.to_string(),
                });
            }

            normalised.push(code);
        }

        let impact = match &self.impact {
            Some(requested) => Some(
                stats
                    .resolve_impact(requested)
                    .ok_or_else(|| LseError::UnknownCalendarImpact {
                        requested: requested.to_string(),
                        available: stats.impacts.iter().map(ToString::to_string).collect(),
                    })?
                    .to_string(),
            ),
            None => None,
        };

        // The only range check the provider makes possible. Per-country coverage is not published,
        // so a window inside this one may still hold nothing -- which is not an error here.
        let earliest = stats.earliest_date()?;
        let latest = stats.latest_date()?;

        if self.end < earliest || self.start > latest {
            return Err(LseError::CalendarRangeOutsideCoverage {
                start: self.start,
                end: self.end,
                earliest,
                latest,
            });
        }

        let mut params = vec![
            ("start_date", self.start.format(DATE_FORMAT).to_string()),
            ("end_date", self.end.format(DATE_FORMAT).to_string()),
            // Absent, this endpoint serves CSV.
            ("format", JSON_FORMAT.to_string()),
        ];

        // Joined, never repeated: the provider keeps only the last of a repeated key, discarding
        // every earlier country without an error.
        if !normalised.is_empty() {
            params.push((
                "country",
                normalised
                    .iter()
                    .map(SmolStr::as_str)
                    .collect::<Vec<_>>()
                    .join(COUNTRY_SEPARATOR),
            ));
        }

        if let Some(impact) = impact {
            params.push(("impact", impact));
        }

        Ok(params)
    }
}

/// The `{count, data}` envelope `/economic-calendar` answers with.
///
/// `count` is carried so it can be checked against the events actually delivered; see
/// [`fetch_economic_calendar`](LseDataApiClient::fetch_economic_calendar).
#[derive(Debug, Deserialize)]
struct LseCalendarEnvelope {
    count: usize,
    data: Vec<LseCalendarEvent>,
}

impl LseDataApiClient {
    /// Fetch the provider's index of published economic-calendar coverage.
    ///
    /// Fetch this **once** and reuse the handle across queries — see [`LseCalendarStats`]. It is
    /// the required first argument to
    /// [`fetch_economic_calendar`](Self::fetch_economic_calendar), which is what makes a query
    /// checkable against an endpoint that validates nothing itself.
    ///
    /// It is also how a caller detects the frozen feed reviving:
    /// [`latest`](LseCalendarStats::latest) has not moved past 2026-03-24 across measurements two
    /// months apart.
    ///
    /// # Errors
    /// See [`LseError`].
    pub async fn fetch_economic_calendar_stats(&self) -> Result<LseCalendarStats, LseError> {
        self.get_json(
            ECONOMIC_CALENDAR_STATS_PATH,
            &[("format", JSON_FORMAT.to_string())],
        )
        .await
    }

    /// Fetch economic-calendar events for `query`.
    ///
    /// `stats` is **required**, not optional, and `query` is validated against it before anything
    /// is sent. That is a deliberate API choice: the endpoint answers `200` with zero rows for an
    /// unknown country, an unknown impact rating, a reversed range *and* an empty window alike, so
    /// a fetch that could skip validation would be a fetch that could silently return nothing. See
    /// the [module documentation](self) for the measurements.
    ///
    /// The country codes sent are the **normalised** ones — a caller passing `GB` reaches the
    /// provider's `UK` — joined into a single comma-separated parameter.
    ///
    /// # 🔴 Nothing is published after 2026-03-24
    /// The feed is frozen. A window after that date is rejected by validation; a window spanning it
    /// returns only what exists up to it. This endpoint serves a historical archive, not a
    /// forward-looking calendar. See the [module documentation](self).
    ///
    /// # 🔴 An empty result is not necessarily an error
    /// The provider publishes no per-country coverage, and coverage is very uneven — 88 of 108
    /// countries hold fewer than 100 events, and `UK` holds 56, all in early 2026. A query for a
    /// published country inside the global range can legitimately return an empty [`Vec`], and
    /// that is **not** reported as an error, because the library has no way to distinguish it from
    /// a quiet period. Only the three checks validation can actually make are made.
    ///
    /// # The whole answer arrives in one response
    /// This endpoint is **not paged**: its envelope carries no cursor, the provider's OpenAPI
    /// document declares no `limit`, `offset`, `cursor` or `page` parameter, and a request for the
    /// entire corpus with no country filter returned all 124,896 events in a single 29 MB
    /// response, matching `total_events` exactly. So this returns a `Vec` rather than a stream, and
    /// no pagination is invented for a surface that has none.
    ///
    /// The envelope's own `count` is checked against the events delivered and a mismatch is an
    /// error rather than a short result — that is the signal a silent page cap would produce if the
    /// provider ever introduced one.
    ///
    /// # ⚠️ An unnarrowed query is a large request
    /// That whole-corpus fetch took **6.3 seconds**, against this client's 30-second total request
    /// timeout. It is the measured worst case rather than a typical one, but the margin is not
    /// enormous: narrow the countries or the window for anything latency-sensitive.
    ///
    /// # Errors
    /// Anything [`validate`](LseCalendarQuery::validate) raises, plus [`LseError`]'s transport and
    /// decode variants. A response whose `count` disagrees with the events delivered is reported as
    /// [`LseError::Api`] carrying the `200` the provider actually sent.
    pub async fn fetch_economic_calendar(
        &self,
        stats: &LseCalendarStats,
        query: &LseCalendarQuery,
    ) -> Result<Vec<LseCalendarEvent>, LseError> {
        let params = query.resolve(stats)?;

        let envelope: LseCalendarEnvelope = self.get_json(ECONOMIC_CALENDAR_PATH, &params).await?;

        if envelope.count != envelope.data.len() {
            return Err(LseError::Api {
                status: 200,
                message: format!(
                    "economic-calendar response declared {} events but carried {}: the response \
                     appears to have been truncated",
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

    /// A stats handle shaped like the live one: `UK` present, `GB` absent, `EA` and `EU` among the
    /// codes, and the four-rating impact vocabulary including the literal `"None"`.
    fn stats() -> LseCalendarStats {
        LseCalendarStats {
            countries: ["EA", "EU", "JP", "UK", "US"]
                .into_iter()
                .map(SmolStr::new)
                .collect(),
            impacts: ["High", "Low", "Medium", "None"]
                .into_iter()
                .map(SmolStr::new)
                .collect(),
            earliest: "2014-12-31".to_string(),
            latest: "2026-03-24".to_string(),
            total_events: 124_896,
        }
    }

    fn date(year: i32, month: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(year, month, day).expect("valid date")
    }

    fn params(query: &LseCalendarQuery) -> Vec<(&'static str, String)> {
        query.resolve(&stats()).expect("query should validate")
    }

    fn param<'a>(params: &'a [(&'static str, String)], key: &str) -> Option<&'a str> {
        params
            .iter()
            .find(|(name, _)| *name == key)
            .map(|(_, value)| value.as_str())
    }

    /// Every numeric arrives as a JSON string and absence is spelled `null`. The decoder uses
    /// `rust_decimal::serde::str_option`; a JSON number would fail it outright.
    #[test]
    fn an_event_decodes_with_string_encoded_numerics_and_null_absences() {
        let json = r#"{
            "event_date": "2026-03-24 14:00:00+00:00",
            "country": "US",
            "event": "CPI (YoY)",
            "currency": "USD",
            "previous": "2.9",
            "estimate": null,
            "actual": "-3.1",
            "change": "-0.2",
            "change_percentage": "-6.9",
            "impact": "High",
            "unit": "%"
        }"#;

        let event: LseCalendarEvent = serde_json::from_str(json).unwrap();

        assert_eq!(event.country, "US");
        assert_eq!(event.event, "CPI (YoY)");
        assert_eq!(event.currency.as_deref(), Some("USD"));
        assert_eq!(event.previous, Some(dec!(2.9)));
        assert_eq!(event.estimate, None);
        // Negatives are abundant on this feed and must survive the decode.
        assert_eq!(event.actual, Some(dec!(-3.1)));
        assert_eq!(event.change, Some(dec!(-0.2)));
        assert_eq!(event.change_percentage, Some(dec!(-6.9)));
        assert_eq!(event.impact, LseCalendarImpact::High);
        assert_eq!(event.unit.as_deref(), Some("%"));
    }

    /// 🔴 `"None"` is a literal four-character rating carried by 592 events, not a JSON null. A
    /// decoder that folded it into an absent value would lose a real distinction.
    #[test]
    fn an_impact_of_none_is_a_rating_and_not_an_absent_value() {
        let json = r#"{
            "event_date": "2026-03-24 14:00:00+00:00",
            "country": "JP", "event": "Bank Holiday", "currency": null,
            "previous": null, "estimate": null, "actual": null,
            "change": null, "change_percentage": null,
            "impact": "None", "unit": null
        }"#;

        let event: LseCalendarEvent = serde_json::from_str(json).unwrap();

        assert_eq!(event.impact, LseCalendarImpact::None);
        assert_eq!(event.impact.as_str(), "None");
        assert_eq!(event.impact.to_string(), "None");
        // Nullable fields really are null on this feed -- never an empty string.
        assert_eq!(event.currency, None);
        assert_eq!(event.unit, None);
    }

    /// The set is closed at four. An addition must surface rather than be absorbed.
    #[test]
    fn an_unknown_impact_rating_fails_the_decode_rather_than_being_absorbed() {
        let json = r#"{
            "event_date": "2026-03-24 14:00:00+00:00",
            "country": "US", "event": "x", "currency": null,
            "previous": null, "estimate": null, "actual": null,
            "change": null, "change_percentage": null,
            "impact": "Critical", "unit": null
        }"#;

        assert!(serde_json::from_str::<LseCalendarEvent>(json).is_err());
    }

    #[test]
    fn an_event_ignores_an_unknown_field() {
        let json = r#"{
            "event_date": "2026-03-24 14:00:00+00:00",
            "country": "US", "event": "x", "currency": null,
            "previous": null, "estimate": null, "actual": null,
            "change": null, "change_percentage": null,
            "impact": "Low", "unit": null,
            "a_field_the_provider_added_later": 1
        }"#;

        assert!(serde_json::from_str::<LseCalendarEvent>(json).is_ok());
    }

    /// 🔴 The provider's space-separated spelling is a real timestamp with 348 distinct times of
    /// day, and `DateTime::parse_from_rfc3339` accepts the space as RFC 3339 §5.6 lenience. This is
    /// the claim that licenses `DateTime<Utc>` rather than the bond endpoint's `NaiveDate`.
    #[test]
    fn an_event_date_parses_as_an_instant_keeping_its_time_of_day() {
        let event = LseCalendarEvent {
            event_date: "2026-03-24 14:30:00+00:00".to_string(),
            country: SmolStr::new("US"),
            event: "x".to_string(),
            currency: None,
            previous: None,
            estimate: None,
            actual: None,
            change: None,
            change_percentage: None,
            impact: LseCalendarImpact::Low,
            unit: None,
        };

        let time = event.event_time().unwrap();

        assert_eq!(time.to_rfc3339(), "2026-03-24T14:30:00+00:00");
        assert_eq!(event.event_day().unwrap(), date(2026, 3, 24));
    }

    #[test]
    fn a_malformed_event_date_is_a_typed_error_naming_the_field() {
        let event = LseCalendarEvent {
            event_date: "not-a-date".to_string(),
            country: SmolStr::new("US"),
            event: "x".to_string(),
            currency: None,
            previous: None,
            estimate: None,
            actual: None,
            change: None,
            change_percentage: None,
            impact: LseCalendarImpact::Low,
            unit: None,
        };

        let error = event.event_time().unwrap_err();

        assert!(matches!(error, LseError::Deserialize { .. }));
        assert!(error.to_string().contains("event_date"), "{error}");
    }

    #[test]
    fn gb_normalises_to_the_providers_uk_and_everything_else_is_untouched() {
        assert_eq!(LseCalendarStats::normalise_country("GB"), "UK");
        assert_eq!(LseCalendarStats::normalise_country("gb"), "UK");
        assert_eq!(LseCalendarStats::normalise_country("  Gb  "), "UK");
        assert_eq!(LseCalendarStats::normalise_country("us"), "US");
        // `EA` and `EU` are in this endpoint's vocabulary and are not countries; nothing maps them.
        assert_eq!(LseCalendarStats::normalise_country("ea"), "EA");
        assert_eq!(LseCalendarStats::normalise_country("eu"), "EU");
    }

    #[test]
    fn a_country_lookup_finds_the_uk_under_its_iso_spelling() {
        let stats = stats();

        assert!(stats.publishes_country("GB"));
        assert!(stats.publishes_country("gb"));
        assert!(stats.publishes_country("UK"));
        assert!(!stats.publishes_country("ZZ"));
    }

    #[test]
    fn an_impact_resolves_ignoring_case_and_yields_the_providers_spelling() {
        let stats = stats();

        assert_eq!(stats.resolve_impact("high").unwrap(), "High");
        assert_eq!(stats.resolve_impact("  HIGH  ").unwrap(), "High");
        assert_eq!(stats.resolve_impact("None").unwrap(), "None");
        assert!(stats.resolve_impact("Critical").is_none());
    }

    #[test]
    fn a_valid_query_passes() {
        let query = LseCalendarQuery::new(date(2024, 1, 1), date(2024, 12, 31)).with_country("US");

        assert!(query.validate(&stats()).is_ok());
    }

    #[test]
    fn an_inverted_range_is_rejected() {
        let query = LseCalendarQuery::new(date(2024, 12, 31), date(2024, 1, 1));

        let error = query.validate(&stats()).unwrap_err();

        assert!(matches!(error, LseError::InvalidInput { .. }));
    }

    #[test]
    fn an_unknown_country_is_rejected_and_reports_both_spellings() {
        let query = LseCalendarQuery::new(date(2024, 1, 1), date(2024, 12, 31)).with_country("zz");

        let error = query.validate(&stats()).unwrap_err();

        let LseError::UnknownCalendarCountry {
            requested,
            normalised,
        } = &error
        else {
            panic!("expected UnknownCalendarCountry, got {error:?}");
        };
        assert_eq!(requested, "zz");
        assert_eq!(normalised, "ZZ");
    }

    /// One bad code among several must not be let through by a check that only looked at the first.
    #[test]
    fn an_unknown_country_anywhere_in_the_list_is_rejected() {
        let query = LseCalendarQuery::new(date(2024, 1, 1), date(2024, 12, 31))
            .with_countries(["US", "UK", "ZZ"]);

        assert!(matches!(
            query.validate(&stats()).unwrap_err(),
            LseError::UnknownCalendarCountry { .. }
        ));
    }

    #[test]
    fn an_unknown_impact_is_rejected_and_lists_the_published_vocabulary() {
        let query =
            LseCalendarQuery::new(date(2024, 1, 1), date(2024, 12, 31)).with_impact("Critical");

        let error = query.validate(&stats()).unwrap_err();

        let LseError::UnknownCalendarImpact {
            requested,
            available,
        } = &error
        else {
            panic!("expected UnknownCalendarImpact, got {error:?}");
        };
        assert_eq!(requested, "Critical");
        assert_eq!(available, &["High", "Low", "Medium", "None"]);
    }

    /// 🔴 The frozen feed: every forward-looking window is disjoint from the published range.
    #[test]
    fn a_window_after_the_frozen_latest_is_rejected() {
        let query = LseCalendarQuery::new(date(2026, 4, 1), date(2026, 12, 31));

        let error = query.validate(&stats()).unwrap_err();

        let LseError::CalendarRangeOutsideCoverage {
            start,
            end,
            earliest,
            latest,
        } = &error
        else {
            panic!("expected CalendarRangeOutsideCoverage, got {error:?}");
        };
        assert_eq!(*start, date(2026, 4, 1));
        assert_eq!(*end, date(2026, 12, 31));
        assert_eq!(*earliest, date(2014, 12, 31));
        assert_eq!(*latest, date(2026, 3, 24));
    }

    #[test]
    fn a_window_before_the_earliest_event_is_rejected() {
        let query = LseCalendarQuery::new(date(2010, 1, 1), date(2014, 12, 30));

        assert!(matches!(
            query.validate(&stats()).unwrap_err(),
            LseError::CalendarRangeOutsideCoverage { .. }
        ));
    }

    /// A window straddling the frozen end returns what exists up to it, so it must pass.
    #[test]
    fn a_partially_overlapping_range_passes() {
        let query = LseCalendarQuery::new(date(2026, 3, 1), date(2026, 12, 31));

        assert!(query.validate(&stats()).is_ok());
    }

    /// 🔴 The validation the library deliberately does NOT do. `UK` holds 56 events, all in early
    /// 2026, so this window returns nothing — and the provider publishes no per-country coverage,
    /// so it cannot be distinguished from a quiet period. It must validate, not error.
    #[test]
    fn a_published_country_with_no_events_in_the_window_still_validates() {
        let query = LseCalendarQuery::new(date(2020, 1, 1), date(2020, 12, 31)).with_country("UK");

        assert!(
            query.validate(&stats()).is_ok(),
            "an empty-but-legitimate window must not be an error: the provider publishes no \
             per-country coverage to justify one"
        );
    }

    /// 🔴 The trap. A repeated `country` key silently keeps only the last value, so the countries
    /// must reach the wire as ONE comma-joined parameter.
    #[test]
    fn several_countries_are_sent_as_one_comma_joined_parameter() {
        let query = LseCalendarQuery::new(date(2024, 1, 1), date(2024, 12, 31))
            .with_countries(["US", "UK"]);
        let params = params(&query);

        assert_eq!(
            params.iter().filter(|(name, _)| *name == "country").count(),
            1,
            "the country key must never be repeated: the provider keeps only the last value"
        );
        assert_eq!(param(&params, "country"), Some("US,UK"));
    }

    /// The normalisation has to reach the wire, not merely the membership check.
    #[test]
    fn the_request_carries_the_normalised_country_codes() {
        let query = LseCalendarQuery::new(date(2024, 1, 1), date(2024, 12, 31))
            .with_countries(["gb", "us"]);

        assert_eq!(param(&params(&query), "country"), Some("UK,US"));
    }

    #[test]
    fn the_request_carries_the_providers_own_impact_spelling() {
        let query =
            LseCalendarQuery::new(date(2024, 1, 1), date(2024, 12, 31)).with_impact("medium");

        assert_eq!(param(&params(&query), "impact"), Some("Medium"));
    }

    /// Absent filters are omitted rather than sent empty: this endpoint answers an unknown filter
    /// with zero rows rather than an error, so an empty value would be a silent empty result.
    #[test]
    fn an_unfiltered_query_omits_country_and_impact_entirely() {
        let query = LseCalendarQuery::new(date(2024, 1, 1), date(2024, 12, 31));
        let params = params(&query);

        assert_eq!(param(&params, "country"), None);
        assert_eq!(param(&params, "impact"), None);
        assert_eq!(param(&params, "start_date"), Some("2024-01-01"));
        assert_eq!(param(&params, "end_date"), Some("2024-12-31"));
    }

    /// ⚠️ Omitting `format` serves CSV, which would hand a CSV body to a JSON decoder.
    #[test]
    fn every_request_asks_for_json_because_the_endpoint_defaults_to_csv() {
        let query = LseCalendarQuery::new(date(2024, 1, 1), date(2024, 12, 31));

        assert_eq!(param(&params(&query), "format"), Some("json"));
    }

    #[test]
    fn stats_decode_with_their_integer_typed_total() {
        let json = r#"{
            "countries": ["EA", "UK", "US"],
            "impacts": ["High", "Low", "Medium", "None"],
            "earliest": "2014-12-31",
            "latest": "2026-03-24",
            "total_events": 124896
        }"#;

        let stats: LseCalendarStats = serde_json::from_str(json).unwrap();

        assert_eq!(stats.total_events, 124_896);
        assert_eq!(stats.earliest_date().unwrap(), date(2014, 12, 31));
        assert_eq!(stats.latest_date().unwrap(), date(2026, 3, 24));
    }

    #[test]
    fn a_malformed_stats_date_fails_validation_rather_than_being_ignored() {
        let mut stats = stats();
        stats.latest = "24/03/2026".to_string();

        let query = LseCalendarQuery::new(date(2024, 1, 1), date(2024, 12, 31));
        let error = query.validate(&stats).unwrap_err();

        assert!(matches!(error, LseError::Deserialize { .. }));
        assert!(error.to_string().contains("latest"), "{error}");
    }
}
