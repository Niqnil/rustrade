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

use super::{IbkrFlexError, nonempty};

/// A single corporate-action row from an IBKR Flex statement, surfaced verbatim.
///
/// Every field mirrors a Flex `<CorporateAction>` attribute with no interpretation applied beyond
/// type coercion (string → `Decimal`/`NaiveDate`) and treating absent/empty attributes as `None`.
/// See the [module docs](self) for why this is a reconciliation record and not a split-ratio source.
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
    /// this library does not track. An absent or empty `quantity` is surfaced as `0`.
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
    let response: RawFlexQueryResponse = quick_xml::de::from_str(xml)
        .map_err(|e| IbkrFlexError::Parse(format!("failed to parse Flex statement XML: {e}")))?;

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

        IbkrFlexCorporateAction {
            account_id: opt_smol(self.account_id),
            symbol: opt_smol(self.symbol),
            conid: opt_smol(self.conid),
            isin: opt_smol(self.isin),
            cusip: opt_smol(self.cusip),
            asset_category: opt_smol(self.asset_category),
            action_type: IbkrReorgType::from(code.as_str()),
            quantity_delta: opt_decimal(self.quantity).unwrap_or(Decimal::ZERO),
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

/// Best-effort parse a `YYYY-MM-DD` attribute, yielding `None` if absent, empty, or malformed.
fn opt_date(value: Option<String>) -> Option<NaiveDate> {
    nonempty(value).and_then(|v| NaiveDate::parse_from_str(&v, "%Y-%m-%d").ok())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // Tests should panic on unexpected values.
mod tests {
    use super::*;

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
            Some(Decimal::from(2))
        );
        assert_eq!(
            find(&actions, "SPLIT").principal_adjust_factor,
            Some(Decimal::new(1, 1)) // 0.1
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
}
