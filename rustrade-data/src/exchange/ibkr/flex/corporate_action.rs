//! Faithful, raw IBKR Flex corporate-action records and the XML parser.
//!
//! This module turns the `<CorporateAction>` rows of an IBKR **Flex Web Service** statement
//! (the account *Activity* report's Corporate Actions section) into a list of
//! [`IbkrFlexCorporateAction`] records, **without** interpreting them. It is a *reconciliation*
//! surface, not a split-ratio source:
//!
//! - **No ratio is derived.** Flex reports a corporate-action *type* (e.g. forward/reverse split)
//!   plus an account-scoped share **delta**, but carries no standardised split-ratio field. Deriving
//!   a ratio would mean parsing the unstable free-text `actionDescription`, or dividing the
//!   post-event by the pre-event holding (account state this library deliberately does not own).
//!   Both are silent-failure risks (a wrong-but-plausible ratio would mis-scale a position), so the
//!   library surfaces the raw record and leaves ratio derivation/verification to the caller — e.g.
//!   cross-referencing a market-reference split source.
//! - **These records cannot drive a live split.** A Flex statement is *post-hoc*: its `reportDate`
//!   is the day the broker booked the action (typically T+1 or later), **not** the market execution
//!   date a backtest/live engine needs. Use these records for reconciliation and audit, not for
//!   injecting split events at the right point in a timeline.
//! - **Records are account-scoped.** `quantity_delta` is the change to *this account's* position
//!   from the action, not a market-wide quantity. Two accounts see different deltas for the same
//!   corporate action.
//!
//! The parser returns **every** reorg row faithfully (forward/reverse splits, spin-offs, dividends,
//! mergers, …); selecting which rows matter (e.g. only `FS`/`RS`) is the caller's job.

use chrono::NaiveDate;
use rust_decimal::Decimal;
use serde::Deserialize;
use smol_str::SmolStr;
use tracing::warn;

use super::{IbkrFlexError, finish_parse_error, nonempty};

/// A single corporate-action row from an IBKR Flex statement, surfaced verbatim.
///
/// Every field mirrors a Flex `<CorporateAction>` attribute with no interpretation applied beyond
/// type coercion (string → `Decimal`/`NaiveDate`) and treating absent/empty attributes as `None`.
///
/// This is a **reconciliation** record, not a split-ratio source, and three limitations follow from
/// that:
///
/// - **No ratio is derived.** Flex carries no standardised split-ratio field, only an action type
///   plus an account-scoped share delta. Deriving a ratio would mean parsing the unstable free-text
///   `action_description` or dividing post- by pre-event holdings (account state this library does
///   not own) — both silent-failure risks, so ratio derivation is left to the caller.
/// - **These records cannot drive a live split.** A Flex statement is *post-hoc*: `report_date` is
///   when the broker booked the action (typically T+1 or later), not the market execution date a
///   backtest or live engine needs.
/// - **Records are account-scoped.** `quantity_delta` is the change to *this account's* position,
///   not a market-wide quantity; two accounts see different deltas for the same action.
///
/// `#[non_exhaustive]`: IBKR may add attributes to the Flex schema; new fields are surfaced
/// additively without a breaking change. Construct instances via [`parse_corporate_actions`], not a
/// struct literal.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct IbkrFlexCorporateAction {
    /// IBKR account the action was booked against (`accountId`). Account-scoped — see the module docs.
    pub account_id: Option<SmolStr>,
    /// Instrument ticker symbol (`symbol`).
    pub symbol: Option<SmolStr>,
    /// IBKR contract id (`conid`), surfaced as a string.
    pub conid: Option<SmolStr>,
    /// ISIN identifier (`isin`), when present.
    pub isin: Option<SmolStr>,
    /// CUSIP identifier (`cusip`), when present.
    pub cusip: Option<SmolStr>,
    /// IBKR asset category (`assetCategory`, e.g. `"STK"`, `"BOND"`).
    pub asset_category: Option<SmolStr>,
    /// The reorganisation type (`type`, aliased from `actionType`). See [`IbkrReorgType`].
    pub action_type: IbkrReorgType,
    /// Signed change to *this account's* share count from the action (`quantity`).
    ///
    /// This is an **account-scoped delta**, not a market-wide quantity, and is **not** a split
    /// ratio: deriving a ratio from `new_qty / old_qty` would require the pre-event holding, which
    /// this library does not track. An absent or empty `quantity` is surfaced as `0`; a **malformed**
    /// (present-but-unparseable) `quantity` is also coerced to `0` but emits a `warn!` first, since
    /// `0` here is a valid-looking "no share change" sentinel that would otherwise silently mask a
    /// real reorg quantity (unlike the other fields, which faithfully surface a malformed value as
    /// `None`).
    pub quantity_delta: Decimal,
    /// Free-text action description (`actionDescription`), when present.
    ///
    /// **Unstable / human-facing.** The wording is not a stable contract and must **not** be parsed
    /// to extract a split ratio or other structured data — IBKR can change it at any time. Some rows
    /// omit it entirely.
    pub action_description: Option<SmolStr>,
    /// The date IBKR booked the action (`reportDate`), best-effort parsed from `YYYY-MM-DD`.
    ///
    /// This is **not** the market execution date of the corporate action — a Flex statement is
    /// post-hoc, so `report_date` is typically a trading day (or more) *after* the event took effect
    /// in the market. Do not treat it as an effective date for engine injection.
    pub report_date: Option<NaiveDate>,
    /// Raw `dateTime` attribute, surfaced as-is.
    ///
    /// The Flex date/time format is query-configuration-dependent — it can be `"2025-01-15;000000"`
    /// (date;time) or a bare `"2025-01-15"` depending on the saved query's date/time format settings
    /// — so it is intentionally **not** parsed here. Callers that need a typed value should parse it
    /// against the format their own Flex query is configured to emit.
    pub date_time: Option<SmolStr>,
    /// Booked value of the action (`value`), when present.
    pub value: Option<Decimal>,
    /// Cash proceeds of the action (`proceeds`), when present.
    pub proceeds: Option<Decimal>,
    /// Realised FIFO P&L attributed to the action (`fifoPnlRealized`), when present.
    pub fifo_pnl_realized: Option<Decimal>,
    /// Raw `principalAdjustFactor` attribute, when present — **surfaced, never interpreted**.
    ///
    /// IBKR documents this as the calculated principal-adjustment factor for **Treasury
    /// Inflation-Protected Securities (TIPS)**, *not* a split ratio. Some synthetic third-party
    /// fixtures show it populated on split rows in a way that resembles `split_to / split_from`, but
    /// that has **not** been confirmed against live broker output and may be an artefact of those
    /// fixtures. It is surfaced here only so the raw record drops no schema field; it must **not** be
    /// used as a primary source for a split ratio. A caller holding both this record and a
    /// market-reference ratio is the right place to optionally cross-check it.
    pub principal_adjust_factor: Option<Decimal>,
    /// IBKR action identifier (`actionID`), when present.
    pub action_id: Option<SmolStr>,
    /// IBKR transaction identifier (`transactionID`), when present.
    pub transaction_id: Option<SmolStr>,
}

/// IBKR Flex reorganisation type (the `<CorporateAction>` `type` attribute).
///
/// Only the split-related codes the reconciliation use-case cares about are modelled as named
/// variants; every other code (dividends, spin-offs, mergers, tender offers, bond events, …) is
/// preserved verbatim in [`Other`](IbkrReorgType::Other) so no information is lost.
///
/// `#[non_exhaustive]`: named variants may be added as more codes gain first-class handling, so
/// downstream `match`es must include a wildcard arm.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum IbkrReorgType {
    /// Forward split (`FS`).
    ForwardSplit,
    /// Reverse split (`RS`).
    ReverseSplit,
    /// Forward split issue (`FI`).
    ForwardSplitIssue,
    /// Contract split (`CS`).
    ContractSplit,
    /// Spin-off (`SO`).
    SpinOff,
    /// Contract spin-off (`CO`).
    ContractSpinOff,
    /// Any other (or absent) reorg code, preserved verbatim (e.g. `"DI"`, `"TC"`, `"CD"`).
    Other(SmolStr),
}

impl From<&str> for IbkrReorgType {
    /// Map an IBKR reorg code string to a variant. Unknown or empty codes become
    /// [`Other`](IbkrReorgType::Other) (an empty code yields `Other("")`).
    ///
    /// This is the canonical code → variant mapping; it is total (every input maps to a variant)
    /// so consumers parsing their own Flex records can reconstruct the type from a stored code.
    fn from(code: &str) -> Self {
        match code {
            "FS" => Self::ForwardSplit,
            "RS" => Self::ReverseSplit,
            "FI" => Self::ForwardSplitIssue,
            "CS" => Self::ContractSplit,
            "SO" => Self::SpinOff,
            "CO" => Self::ContractSpinOff,
            other => Self::Other(SmolStr::from(other)),
        }
    }
}

/// Parse the `<CorporateAction>` rows out of an IBKR Flex statement XML document.
///
/// Walks every `<FlexStatement>` in the `<FlexQueryResponse>` and returns each `<CorporateAction>`
/// as a faithful [`IbkrFlexCorporateAction`], in document order. **All** reorg types are returned —
/// filtering to splits (or any other subset) is the caller's responsibility.
///
/// Non-corporate-action sections of the statement (trades, positions, cash transactions, …) are
/// ignored. A statement with no Corporate Actions section yields an empty vector.
///
/// # Errors
///
/// Returns [`IbkrFlexError::Parse`] if the document is not well-formed XML or does not match the
/// expected Flex statement shape.
pub fn parse_corporate_actions(xml: &str) -> Result<Vec<IbkrFlexCorporateAction>, IbkrFlexError> {
    parse_corporate_actions_scrubbed(xml, None)
}

/// [`parse_corporate_actions`], additionally redacting `token` from any error message.
///
/// Exists because the two callers differ in what they are able to protect. Called directly, this
/// parser has no credential in scope and can only *bound* its messages. Called from
/// [`IbkrFlexClient::fetch_corporate_actions`](super::IbkrFlexClient::fetch_corporate_actions) a
/// token does exist, and the message must be redacted as well as bounded.
///
/// The token is threaded in *here*, rather than the caller scrubbing the returned message, because
/// the two operations do not commute. Redaction matches the full token, so it has to run while the
/// message is still unbounded: bounding first can leave a straddling credential present only as a
/// prefix fragment, which a subsequent full-token match would miss. See
/// [`sanitize_error_body`](super::sanitize_error_body), which enforces the same ordering internally.
pub(super) fn parse_corporate_actions_scrubbed(
    xml: &str,
    token: Option<&str>,
) -> Result<Vec<IbkrFlexCorporateAction>, IbkrFlexError> {
    // Assert the document is actually a `<FlexQueryResponse>` statement BEFORE deserializing.
    // quick-xml's serde path does not verify the root element, and the raw structs use
    // `#[serde(default)]`, so an IBKR error/status envelope (`<FlexStatementResponse>`, e.g. an
    // "Invalid token" `ErrorCode 1003`) would otherwise deserialize into an all-defaults
    // (empty) `RawFlexQueryResponse` and return `Ok(vec![])` — indistinguishable from a genuine
    // statement with no Corporate Actions section, and contradicting this function's `# Errors`
    // contract. Reuse the same root-element helper the blessed `GetStatement` path uses
    // (`super::root_element_name`, which `super::classify_get_statement` also wraps).
    match super::root_element_name(xml).as_deref() {
        Some("FlexQueryResponse") => {}
        Some(other) => {
            // `other` is a tag name lifted verbatim out of the supplied document, so an adversarial
            // or corrupt statement could otherwise inflate the error to the size of the input.
            // `finish_parse_error` applies the same cap the fetch-response parse path applies —
            // after redaction, never before it.
            return Err(finish_parse_error(
                format!("expected Flex statement root <FlexQueryResponse>, found <{other}>"),
                token,
            ));
        }
        None => {
            // Routed through the same finaliser as the branches above even though this message is a
            // fixed literal with nothing to redact or bound. Uniformity is the point: "every `Parse`
            // is built by `finish_parse_error`" is a property a reader can check locally, whereas
            // "every `Parse` that *could* carry a credential is" requires re-auditing each branch
            // whenever a message gains an interpolated value.
            return Err(finish_parse_error(
                "empty or unreadable Flex statement document".to_owned(),
                token,
            ));
        }
    }

    // Bounded for the same reason: a `DeError` embeds the deserialiser's rendering of the offending
    // input (quick-xml's `UnexpectedStart` carries the raw tag bytes it choked on), so the message
    // length tracks the document, not the failure.
    let response: RawFlexQueryResponse = quick_xml::de::from_str(xml).map_err(|e| {
        finish_parse_error(format!("failed to parse Flex statement XML: {e}"), token)
    })?;

    Ok(response
        .flex_statements
        .statements
        .into_iter()
        .flat_map(|statement| statement.corporate_actions.actions)
        .map(RawCorporateAction::into_corporate_action)
        .collect())
}

// ============================================================================
// Raw deserialisation layer
// ============================================================================
//
// quick-xml's serde support maps XML *attributes* to fields renamed with a leading `@`. Every
// attribute is captured as `Option<String>` first, then coerced in `into_corporate_action`, so that
// empty (`foo=""`) and absent attributes both collapse to `None` and a malformed numeric/date
// attribute degrades to `None`/`0` rather than failing the whole parse.

#[derive(Debug, Deserialize)]
struct RawFlexQueryResponse {
    #[serde(rename = "FlexStatements", default)]
    flex_statements: RawFlexStatements,
}

#[derive(Debug, Default, Deserialize)]
struct RawFlexStatements {
    #[serde(rename = "FlexStatement", default)]
    statements: Vec<RawFlexStatement>,
}

#[derive(Debug, Deserialize)]
struct RawFlexStatement {
    #[serde(rename = "CorporateActions", default)]
    corporate_actions: RawCorporateActions,
}

#[derive(Debug, Default, Deserialize)]
struct RawCorporateActions {
    #[serde(rename = "CorporateAction", default)]
    actions: Vec<RawCorporateAction>,
}

#[derive(Debug, Deserialize)]
struct RawCorporateAction {
    #[serde(rename = "@accountId", default)]
    account_id: Option<String>,
    #[serde(rename = "@symbol", default)]
    symbol: Option<String>,
    #[serde(rename = "@conid", default)]
    conid: Option<String>,
    #[serde(rename = "@isin", default)]
    isin: Option<String>,
    #[serde(rename = "@cusip", default)]
    cusip: Option<String>,
    #[serde(rename = "@assetCategory", default)]
    asset_category: Option<String>,
    // The reorg type lives in `type`; some statements also (or instead) carry `actionType`. Capture
    // both separately and prefer `type`, falling back to `actionType` — serde `alias` would reject a
    // row that carries *both* attributes as a duplicate field.
    #[serde(rename = "@type", default)]
    type_attr: Option<String>,
    #[serde(rename = "@actionType", default)]
    action_type_attr: Option<String>,
    #[serde(rename = "@quantity", default)]
    quantity: Option<String>,
    #[serde(rename = "@actionDescription", default)]
    action_description: Option<String>,
    #[serde(rename = "@reportDate", default)]
    report_date: Option<String>,
    #[serde(rename = "@dateTime", default)]
    date_time: Option<String>,
    #[serde(rename = "@value", default)]
    value: Option<String>,
    #[serde(rename = "@proceeds", default)]
    proceeds: Option<String>,
    #[serde(rename = "@fifoPnlRealized", default)]
    fifo_pnl_realized: Option<String>,
    #[serde(rename = "@principalAdjustFactor", default)]
    principal_adjust_factor: Option<String>,
    #[serde(rename = "@actionID", default)]
    action_id: Option<String>,
    #[serde(rename = "@transactionID", default)]
    transaction_id: Option<String>,
}

impl RawCorporateAction {
    fn into_corporate_action(self) -> IbkrFlexCorporateAction {
        let code = nonempty(self.type_attr)
            .or_else(|| nonempty(self.action_type_attr))
            .unwrap_or_default();

        // Computed BEFORE the struct literal so the warn can still reference `symbol` (which the
        // literal moves). See `quantity_delta_or_warn` for why this field warns and the others do not.
        let quantity_delta = quantity_delta_or_warn(self.quantity, self.symbol.as_deref());

        IbkrFlexCorporateAction {
            account_id: opt_smol(self.account_id),
            symbol: opt_smol(self.symbol),
            conid: opt_smol(self.conid),
            isin: opt_smol(self.isin),
            cusip: opt_smol(self.cusip),
            asset_category: opt_smol(self.asset_category),
            action_type: IbkrReorgType::from(code.as_str()),
            quantity_delta,
            action_description: opt_smol(self.action_description),
            report_date: opt_date(self.report_date),
            date_time: opt_smol(self.date_time),
            value: opt_decimal(self.value),
            proceeds: opt_decimal(self.proceeds),
            fifo_pnl_realized: opt_decimal(self.fifo_pnl_realized),
            principal_adjust_factor: opt_decimal(self.principal_adjust_factor),
            action_id: opt_smol(self.action_id),
            transaction_id: opt_smol(self.transaction_id),
        }
    }
}

fn opt_smol(value: Option<String>) -> Option<SmolStr> {
    nonempty(value).map(SmolStr::from)
}

/// Parse a non-empty attribute as a [`Decimal`], yielding `None` if absent, empty, or malformed.
fn opt_decimal(value: Option<String>) -> Option<Decimal> {
    nonempty(value).and_then(|v| v.parse().ok())
}

/// Parse the `quantity` attribute into `quantity_delta`, coercing a malformed value to
/// `Decimal::ZERO` but **warning first**.
///
/// Every other field in this module is deliberately faithful — an absent, empty, or malformed
/// attribute surfaces as `None`, so a downstream reader can tell "not reported" from "reported as
/// X". `quantity_delta` is the one non-optional coercion: a malformed value collapses to `0`, which
/// is a *valid-looking* "no share change" sentinel indistinguishable from a genuine zero. That
/// silent collapse could mask a real reorg quantity, so a present-but-unparseable value is surfaced
/// as an observable `warn!` (absent/empty stays a quiet `0` — a genuine "not reported", not a parse
/// failure).
fn quantity_delta_or_warn(value: Option<String>, symbol: Option<&str>) -> Decimal {
    match nonempty(value) {
        Some(raw) => raw.parse().unwrap_or_else(|_| {
            warn!(
                quantity = %raw,
                symbol = ?symbol,
                "IBKR flex: malformed corporate-action quantity coerced to 0 (a valid-looking \
                 'no share change' sentinel) — a real reorg quantity may be silently lost."
            );
            Decimal::ZERO
        }),
        None => Decimal::ZERO,
    }
}

/// Best-effort parse a `YYYY-MM-DD` attribute, yielding `None` if absent, empty, or malformed.
fn opt_date(value: Option<String>) -> Option<NaiveDate> {
    nonempty(value).and_then(|v| NaiveDate::parse_from_str(&v, "%Y-%m-%d").ok())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // Tests should panic on unexpected values.
mod tests {
    use super::*;
    // Only the assertions below reference the cap directly — `finish_parse_error` applies it.
    use super::super::MAX_ERROR_BODY_BYTES;

    #[test]
    fn parse_errors_are_bounded() {
        // These messages embed the offending document, so their length must track the cap rather
        // than the input. Both input-reflecting branches are covered: the root-element mismatch and
        // the deserialiser failure. The short "empty or unreadable" branch is a fixed literal.
        let huge_root = format!("<{}/>", "a".repeat(8192));
        match parse_corporate_actions(&huge_root) {
            Err(IbkrFlexError::Parse(message)) => {
                assert!(
                    message.starts_with("expected Flex statement root"),
                    "must reach the root-element branch, got: {message}"
                );
                assert!(
                    message.len() <= MAX_ERROR_BODY_BYTES,
                    "bounded to the cap, got {} bytes",
                    message.len()
                );
            }
            other => panic!("expected Parse, got {other:?}"),
        }

        // A correct root that then fails to deserialize, driving the `DeError` branch. The inner
        // tag is left unclosed so quick-xml reports a mismatched end tag, embedding the long name.
        // (A malformed *attribute* would not do: `quantity` is `Option<String>` and an unparseable
        // value is coerced to `0` with a warn, so it returns `Ok` and would test nothing.)
        let huge_body = format!(
            "<FlexQueryResponse><{}></FlexQueryResponse>",
            "a".repeat(8192)
        );
        match parse_corporate_actions(&huge_body) {
            Err(IbkrFlexError::Parse(message)) => {
                assert!(
                    message.starts_with("failed to parse Flex statement XML"),
                    "must reach the deserialiser branch, got: {message}"
                );
                assert!(
                    message.len() <= MAX_ERROR_BODY_BYTES,
                    "bounded to the cap, got {} bytes",
                    message.len()
                );
            }
            other => panic!("expected Parse, got {other:?}"),
        }
    }

    const ACTIVITY_FIXTURE: &str =
        include_str!("../../../../tests/fixtures/ibkr_flex/activity_corporate_actions.xml");
    const COMPLEX_FIXTURE: &str =
        include_str!("../../../../tests/fixtures/ibkr_flex/activity_complex_corporate_actions.xml");

    fn find<'a>(
        actions: &'a [IbkrFlexCorporateAction],
        symbol: &str,
    ) -> &'a IbkrFlexCorporateAction {
        actions
            .iter()
            .find(|a| a.symbol.as_deref() == Some(symbol))
            .unwrap_or_else(|| panic!("no corporate action for symbol {symbol}"))
    }

    #[test]
    fn parses_basic_activity_fixture() {
        let actions = parse_corporate_actions(ACTIVITY_FIXTURE).unwrap();
        // 8 <CorporateAction> rows: DI, FS, SO, TC, RI, SD, RS, DW.
        assert_eq!(actions.len(), 8);

        // Forward split (TSLA, 2:1): the named variant + account-scoped +100 share delta.
        let tsla = find(&actions, "TSLA");
        assert_eq!(tsla.action_type, IbkrReorgType::ForwardSplit);
        assert_eq!(tsla.quantity_delta, Decimal::from(100));
        assert_eq!(tsla.account_id.as_deref(), Some("U1234567"));
        assert_eq!(tsla.cusip.as_deref(), Some("88160R101"));
        assert_eq!(tsla.isin.as_deref(), Some("US88160R1014"));
        assert_eq!(tsla.asset_category.as_deref(), Some("STK"));
        assert_eq!(tsla.report_date, NaiveDate::from_ymd_opt(2025, 1, 15));
        // `dateTime` kept raw in the `date;time` format this query emitted.
        assert_eq!(tsla.date_time.as_deref(), Some("2025-01-15;000000"));

        // Reverse split (SPLIT, 1:10): negative (account-scoped) delta.
        let split = find(&actions, "SPLIT");
        assert_eq!(split.action_type, IbkrReorgType::ReverseSplit);
        assert_eq!(split.quantity_delta, Decimal::from(-900));

        // A non-split code is preserved verbatim, not dropped.
        let dividend = find(&actions, "AAPL");
        assert_eq!(
            dividend.action_type,
            IbkrReorgType::Other(SmolStr::from("DI"))
        );
        assert_eq!(dividend.value, Some(Decimal::from(100)));
        assert_eq!(dividend.proceeds, Some(Decimal::from(100)));

        // A merger row carries realised P&L and a negative delta.
        let merger = find(&actions, "ACQUIRED");
        assert_eq!(
            merger.action_type,
            IbkrReorgType::Other(SmolStr::from("TC"))
        );
        assert_eq!(merger.quantity_delta, Decimal::from(-100));
        assert_eq!(merger.fifo_pnl_realized, Some(Decimal::from(1500)));
    }

    #[test]
    fn principal_adjust_factor_is_surfaced_raw_not_derived() {
        // The field is surfaced verbatim (a faithful record drops no attribute) but is NOT a split
        // ratio — see the field rustdoc. These assertions pin that we read it, nothing more.
        let actions = parse_corporate_actions(ACTIVITY_FIXTURE).unwrap();
        assert_eq!(
            find(&actions, "TSLA").principal_adjust_factor,
            // A TIPS-style inflation factor (deliberately NOT a round split-ratio-looking value),
            // reinforcing that this field is surfaced raw and is not the split ratio.
            Some(Decimal::new(10023, 4)) // 1.0023
        );
        assert_eq!(
            find(&actions, "SPLIT").principal_adjust_factor,
            Some(Decimal::new(9977, 4)) // 0.9977 (again a TIPS factor, not a ratio)
        );
        // Empty `principalAdjustFactor=""` collapses to None.
        assert_eq!(find(&actions, "AAPL").principal_adjust_factor, None);
    }

    #[test]
    fn parses_complex_fixture_with_type_and_actiontype_aliasing() {
        // Every row in this fixture carries BOTH `type` and `actionType` (equal values) plus a bare
        // `dateTime` (no time component) — exercising the alias coalesce and the second date format.
        let actions = parse_corporate_actions(COMPLEX_FIXTURE).unwrap();
        assert_eq!(actions.len(), 10);

        let choice_dividend = &actions[0];
        assert_eq!(choice_dividend.symbol.as_deref(), Some("XYZ"));
        assert_eq!(
            choice_dividend.action_type,
            IbkrReorgType::Other(SmolStr::from("CD"))
        );
        assert_eq!(
            choice_dividend.action_description.as_deref(),
            Some("Choice Dividend")
        );
        // Bare-date `dateTime` form is kept raw, not coerced.
        assert_eq!(choice_dividend.date_time.as_deref(), Some("2025-01-15"));

        // A BOND-category row survives intact.
        let bond = find(&actions, "DEF");
        assert_eq!(bond.asset_category.as_deref(), Some("BOND"));
    }

    #[test]
    fn empty_corporate_actions_section_yields_no_rows() {
        let xml = r#"<?xml version="1.0"?>
            <FlexQueryResponse queryName="Activity" type="AF">
              <FlexStatements count="1">
                <FlexStatement accountId="U1" fromDate="2025-01-15" toDate="2025-01-15">
                  <CorporateActions />
                </FlexStatement>
              </FlexStatements>
            </FlexQueryResponse>"#;
        assert!(parse_corporate_actions(xml).unwrap().is_empty());
    }

    #[test]
    fn missing_corporate_actions_section_yields_no_rows() {
        let xml = r#"<?xml version="1.0"?>
            <FlexQueryResponse queryName="Activity" type="AF">
              <FlexStatements count="1">
                <FlexStatement accountId="U1" fromDate="2025-01-15" toDate="2025-01-15">
                  <Trades />
                </FlexStatement>
              </FlexStatements>
            </FlexQueryResponse>"#;
        assert!(parse_corporate_actions(xml).unwrap().is_empty());
    }

    #[test]
    fn collects_rows_across_multiple_statements() {
        let xml = r#"<?xml version="1.0"?>
            <FlexQueryResponse queryName="Activity" type="AF">
              <FlexStatements count="2">
                <FlexStatement accountId="U1">
                  <CorporateActions>
                    <CorporateAction accountId="U1" symbol="AAA" type="FS" quantity="10" />
                  </CorporateActions>
                </FlexStatement>
                <FlexStatement accountId="U2">
                  <CorporateActions>
                    <CorporateAction accountId="U2" symbol="BBB" type="RS" quantity="-5" />
                  </CorporateActions>
                </FlexStatement>
              </FlexStatements>
            </FlexQueryResponse>"#;

        let actions = parse_corporate_actions(xml).unwrap();
        assert_eq!(actions.len(), 2);
        assert_eq!(actions[0].symbol.as_deref(), Some("AAA"));
        assert_eq!(actions[0].action_type, IbkrReorgType::ForwardSplit);
        assert_eq!(actions[1].symbol.as_deref(), Some("BBB"));
        assert_eq!(actions[1].action_type, IbkrReorgType::ReverseSplit);
    }

    #[test]
    fn unknown_type_maps_to_other() {
        let xml = r#"<FlexQueryResponse><FlexStatements><FlexStatement>
            <CorporateActions>
              <CorporateAction symbol="ZZ" type="ZZ" quantity="1" />
            </CorporateActions>
        </FlexStatement></FlexStatements></FlexQueryResponse>"#;
        let actions = parse_corporate_actions(xml).unwrap();
        assert_eq!(
            actions[0].action_type,
            IbkrReorgType::Other(SmolStr::from("ZZ"))
        );
    }

    #[test]
    fn actiontype_used_when_type_absent() {
        let xml = r#"<FlexQueryResponse><FlexStatements><FlexStatement>
            <CorporateActions>
              <CorporateAction symbol="AAA" actionType="FS" quantity="1" />
            </CorporateActions>
        </FlexStatement></FlexStatements></FlexQueryResponse>"#;
        let actions = parse_corporate_actions(xml).unwrap();
        assert_eq!(actions[0].action_type, IbkrReorgType::ForwardSplit);
    }

    #[test]
    fn empty_type_falls_back_to_actiontype() {
        // An empty `type=""` must not shadow a populated `actionType`.
        let xml = r#"<FlexQueryResponse><FlexStatements><FlexStatement>
            <CorporateActions>
              <CorporateAction symbol="AAA" type="" actionType="RS" quantity="1" />
            </CorporateActions>
        </FlexStatement></FlexStatements></FlexQueryResponse>"#;
        let actions = parse_corporate_actions(xml).unwrap();
        assert_eq!(actions[0].action_type, IbkrReorgType::ReverseSplit);
    }

    #[test]
    fn sparse_row_degrades_gracefully() {
        // Only `type` present: quantity defaults to 0, every optional field is None, and a missing
        // `type` (here for a second row) yields `Other("")` rather than failing the parse.
        let xml = r#"<FlexQueryResponse><FlexStatements><FlexStatement>
            <CorporateActions>
              <CorporateAction type="FS" />
              <CorporateAction symbol="X" cusip="" reportDate="not-a-date" value="" quantity="oops" />
            </CorporateActions>
        </FlexStatement></FlexStatements></FlexQueryResponse>"#;
        let actions = parse_corporate_actions(xml).unwrap();
        assert_eq!(actions.len(), 2);

        let first = &actions[0];
        assert_eq!(first.action_type, IbkrReorgType::ForwardSplit);
        assert_eq!(first.quantity_delta, Decimal::ZERO);
        assert!(first.symbol.is_none());
        assert!(first.value.is_none());

        let second = &actions[1];
        assert_eq!(second.action_type, IbkrReorgType::Other(SmolStr::from("")));
        assert!(second.cusip.is_none(), "empty cusip must be None");
        assert!(
            second.report_date.is_none(),
            "unparseable date must be None"
        );
        assert!(second.value.is_none(), "empty value must be None");
        assert_eq!(
            second.quantity_delta,
            Decimal::ZERO,
            "unparseable quantity defaults to 0"
        );
    }

    #[test]
    fn reorg_code_mapping_is_total() {
        assert_eq!(IbkrReorgType::from("FS"), IbkrReorgType::ForwardSplit);
        assert_eq!(IbkrReorgType::from("RS"), IbkrReorgType::ReverseSplit);
        assert_eq!(IbkrReorgType::from("FI"), IbkrReorgType::ForwardSplitIssue);
        assert_eq!(IbkrReorgType::from("CS"), IbkrReorgType::ContractSplit);
        assert_eq!(IbkrReorgType::from("SO"), IbkrReorgType::SpinOff);
        assert_eq!(IbkrReorgType::from("CO"), IbkrReorgType::ContractSpinOff);
        assert_eq!(
            IbkrReorgType::from("anything"),
            IbkrReorgType::Other(SmolStr::from("anything"))
        );
    }

    #[test]
    fn malformed_xml_is_an_error() {
        assert!(matches!(
            parse_corporate_actions("<FlexQueryResponse><not-closed>"),
            Err(IbkrFlexError::Parse(_))
        ));
    }

    /// An IBKR error/status envelope is a `<FlexStatementResponse>`, NOT a `<FlexQueryResponse>`
    /// statement. Because `quick-xml` does not verify the root element and the raw structs use
    /// `#[serde(default)]`, this previously deserialized into an all-defaults `RawFlexQueryResponse`
    /// and returned `Ok(vec![])` — indistinguishable from a genuine statement with an empty Corporate
    /// Actions section. It must now be a `Parse` error (honoring the `# Errors` contract), so a
    /// caller re-parsing a persisted envelope standalone cannot mistake "invalid token" for "no
    /// corporate actions".
    #[test]
    fn error_status_envelope_is_an_error_not_empty_vec() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
            <FlexStatementResponse timestamp="18 April, 2026 08:00 AM EDT">
                <Status>Fail</Status>
                <ErrorCode>1003</ErrorCode>
                <ErrorMessage>Statement could not be generated at this time. Invalid token.</ErrorMessage>
            </FlexStatementResponse>"#;
        assert!(
            matches!(parse_corporate_actions(xml), Err(IbkrFlexError::Parse(_))),
            "a <FlexStatementResponse> error envelope must be a Parse error, not Ok(vec![])"
        );
    }

    /// An empty / unreadable document is a `Parse` error, not a silent empty `Vec`.
    #[test]
    fn empty_document_is_an_error() {
        assert!(matches!(
            parse_corporate_actions(""),
            Err(IbkrFlexError::Parse(_))
        ));
    }
}
