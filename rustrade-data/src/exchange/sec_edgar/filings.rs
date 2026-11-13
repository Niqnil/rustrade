//! A company's filing index from EDGAR's submissions API, and the rules that find its earnings
//! releases in it.

use super::error::SecEdgarError;
use crate::earnings::ReleaseTiming;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use serde::Deserialize;
use std::collections::BTreeMap;

/// The quarterly and annual reports whose period end dates a release (transition-period reports
/// included). Amendments (`/A`) are left out: they repeat a period already filed, later.
const PERIODIC_FORMS: [&str; 4] = ["10-Q", "10-K", "10-QT", "10-KT"];

/// A foreign private issuer's annual and current reports, with or without `/A`.
const FOREIGN_FORMS: [&str; 3] = ["20-F", "40-F", "6-K"];

/// Item 2.02 of Form 8-K, "Results of Operations and Financial Condition".
const RESULTS_ITEM: &str = "2.02";

/// The regular session of the US equity exchanges, New York time.
const SESSION_OPEN: NaiveTime = match NaiveTime::from_hms_opt(9, 30, 0) {
    Some(time) => time,
    None => unreachable!(),
};
const SESSION_CLOSE: NaiveTime = match NaiveTime::from_hms_opt(16, 0, 0) {
    Some(time) => time,
    None => unreachable!(),
};

/// The submissions document of one company (`/submissions/CIK##########.json`), reduced to what
/// this integration reads.
#[derive(Debug, Deserialize)]
pub(crate) struct Submissions {
    pub(crate) filings: SubmissionFilings,
}

#[derive(Debug, Deserialize)]
pub(crate) struct SubmissionFilings {
    /// At least the last year's filings, or the last 1,000 if more.
    pub(crate) recent: Columns,
    /// The pages holding older filings, each the same columns at its top level.
    #[serde(default)]
    pub(crate) files: Vec<PageRef>,
}

/// One page of older filings and the filing dates it covers.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PageRef {
    pub(crate) name: String,
    pub(crate) filing_to: NaiveDate,
}

/// A filing index in EDGAR's columnar form: one array per field, one entry per filing.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Columns {
    accession_number: Vec<String>,
    filing_date: Vec<String>,
    report_date: Vec<String>,
    acceptance_date_time: Vec<String>,
    form: Vec<String>,
    /// Absent from some older pages; every filing then has no items.
    #[serde(default)]
    items: Vec<String>,
}

/// One filing of a company's index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Filing {
    pub(crate) accession_number: String,
    pub(crate) form: String,
    /// The filing date EDGAR assigned. A filing accepted after 17:30 New York time is dated the
    /// next business day.
    pub(crate) filed: NaiveDate,
    /// For a 10-Q or 10-K, the period end. For an 8-K, the date of the earliest event the filing
    /// reports, which is not necessarily its release's date.
    pub(crate) report_date: Option<NaiveDate>,
    /// The acceptance time as the index reports it. Only used to order filings of one day: it can
    /// be hours off (the filing header's time is the reliable one).
    pub(crate) acceptance: String,
    /// The 8-K item numbers, comma-separated (`2.02,9.01`).
    pub(crate) items: String,
}

impl Columns {
    /// The filings, one per index entry.
    pub(crate) fn into_filings(self) -> Result<Vec<Filing>, SecEdgarError> {
        let len = self.accession_number.len();
        let same_len = [
            self.filing_date.len(),
            self.report_date.len(),
            self.acceptance_date_time.len(),
            self.form.len(),
        ]
        .iter()
        .all(|&other| other == len);
        if !same_len || !(self.items.is_empty() || self.items.len() == len) {
            return Err(SecEdgarError::decode(
                "filing index columns have different lengths",
            ));
        }
        let mut items = self.items.into_iter();
        self.accession_number
            .into_iter()
            .zip(self.filing_date)
            .zip(self.report_date)
            .zip(self.acceptance_date_time)
            .zip(self.form)
            .map(
                |((((accession_number, filed), report_date), acceptance), form)| {
                    Ok(Filing {
                        filed: parse_date(&filed)?,
                        report_date: match report_date.trim() {
                            "" => None,
                            text => Some(parse_date(text)?),
                        },
                        items: items.next().unwrap_or_default(),
                        accession_number,
                        acceptance,
                        form,
                    })
                },
            )
            .collect()
    }
}

fn parse_date(text: &str) -> Result<NaiveDate, SecEdgarError> {
    NaiveDate::parse_from_str(text.trim(), "%Y-%m-%d")
        .map_err(|e| SecEdgarError::decode(format!("date {text:?}: {e}")))
}

/// A company's earnings releases found in its filing index.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Releases<'a> {
    /// Each paired with the end of the fiscal period it reports, in period order.
    Paired(Vec<(NaiveDate, &'a Filing)>),
    /// The company files foreign private issuer reports; see
    /// [`SecEdgarError::ForeignPrivateIssuer`].
    ForeignPrivateIssuer,
}

/// The release of each fiscal period: for a quarterly or annual report with period end P, filed
/// on F, the **latest** 8-K furnishing Item 2.02 filed after P and no later than F, nor than the
/// next report's period end.
///
/// - **The latest, not the earliest**, because an issuer can furnish other results under Item
///   2.02 after a period ends and before its release: a quarter's production and delivery
///   figures, or preliminary results. The full release comes last, by the time the report is
///   filed.
/// - **No later than the next period end**, so a report filed late cannot claim the next
///   period's release.
/// - **A period whose report is not filed yet has no release here**: until then, nothing in the
///   index says which Item 2.02 filing is its release.
/// - 8-K/A amendments are left out; an amended release keeps its original date.
pub(crate) fn releases(filings: &[Filing]) -> Releases<'_> {
    let mut periods: BTreeMap<NaiveDate, NaiveDate> = BTreeMap::new();
    for filing in filings {
        if let (true, Some(period_end)) = (
            PERIODIC_FORMS.contains(&filing.form.as_str()),
            filing.report_date,
        ) {
            periods
                .entry(period_end)
                .and_modify(|filed| *filed = (*filed).min(filing.filed))
                .or_insert(filing.filed);
        }
    }
    if periods.is_empty() {
        let foreign = filings
            .iter()
            .any(|f| FOREIGN_FORMS.contains(&f.form.trim_end_matches("/A")));
        return if foreign {
            Releases::ForeignPrivateIssuer
        } else {
            Releases::Paired(Vec::new())
        };
    }

    let results: Vec<&Filing> = filings
        .iter()
        .filter(|f| f.form == "8-K" && f.items.split(',').any(|i| i.trim() == RESULTS_ITEM))
        .collect();
    let periods: Vec<(NaiveDate, NaiveDate)> = periods.into_iter().collect();
    let paired = periods
        .iter()
        .enumerate()
        .filter_map(|(index, &(period_end, report_filed))| {
            let last = periods
                .get(index + 1)
                .map_or(report_filed, |&(next_end, _)| report_filed.min(next_end));
            results
                .iter()
                .copied()
                .filter(|f| f.filed > period_end && f.filed <= last)
                .max_by(|a, b| (a.filed, &a.acceptance).cmp(&(b.filed, &b.acceptance)))
                .map(|release| (period_end, release))
        })
        .collect();
    Releases::Paired(paired)
}

/// The acceptance time in a filing header (`…-index-headers.html`), New York time. The header
/// carries it as `<ACCEPTANCE-DATETIME>YYYYMMDDHHMMSS`, raw or HTML-escaped.
pub(crate) fn acceptance_time(header: &str) -> Result<NaiveDateTime, SecEdgarError> {
    let digits = ["<ACCEPTANCE-DATETIME>", "&lt;ACCEPTANCE-DATETIME&gt;"]
        .iter()
        .filter_map(|tag| header.find(tag).map(|at| &header[at + tag.len()..]))
        .map(|rest| {
            let rest = rest.trim_start();
            let end = rest
                .find(|c: char| !c.is_ascii_digit())
                .unwrap_or(rest.len());
            &rest[..end]
        })
        .next()
        .ok_or_else(|| SecEdgarError::decode("filing header has no ACCEPTANCE-DATETIME"))?;
    NaiveDateTime::parse_from_str(digits, "%Y%m%d%H%M%S")
        .map_err(|e| SecEdgarError::decode(format!("ACCEPTANCE-DATETIME {digits:?}: {e}")))
}

/// Where a New York time of day falls against the regular session, 09:30 to 16:00. Early closes
/// are not known here: a release after an early close but before 16:00 reads as during the
/// session.
pub(crate) fn timing(local: NaiveTime) -> ReleaseTiming {
    if local < SESSION_OPEN {
        ReleaseTiming::BeforeOpen
    } else if local < SESSION_CLOSE {
        ReleaseTiming::DuringSession
    } else {
        ReleaseTiming::AfterClose
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;

    fn date(text: &str) -> NaiveDate {
        NaiveDate::parse_from_str(text, "%Y-%m-%d").unwrap()
    }

    fn filing(accession: &str, form: &str, filed: &str, report: &str, items: &str) -> Filing {
        Filing {
            accession_number: accession.into(),
            form: form.into(),
            filed: date(filed),
            report_date: (!report.is_empty()).then(|| date(report)),
            acceptance: format!("{filed}T20:00:00.000Z"),
            items: items.into(),
        }
    }

    fn paired(filings: &[Filing]) -> Vec<(NaiveDate, &str)> {
        match releases(filings) {
            Releases::Paired(pairs) => pairs
                .into_iter()
                .map(|(end, f)| (end, f.accession_number.as_str()))
                .collect(),
            Releases::ForeignPrivateIssuer => panic!("expected pairs"),
        }
    }

    #[test]
    fn each_report_takes_the_latest_results_8k_since_its_period_end() {
        // A quarter with a production-figures 8-K under Item 2.02 two days after the period end,
        // then the results release, then the 10-Q.
        let filings = [
            filing("q3-10q", "10-Q", "2026-10-22", "2026-09-30", ""),
            filing("q3-results", "8-K", "2026-10-21", "2026-10-21", "2.02,9.01"),
            filing(
                "q3-deliveries",
                "8-K",
                "2026-10-02",
                "2026-10-02",
                "2.02,9.01",
            ),
            filing("q2-10q", "10-Q", "2026-07-23", "2026-06-30", ""),
            filing("q2-results", "8-K", "2026-07-22", "2026-07-22", "2.02,9.01"),
            filing(
                "q2-deliveries",
                "8-K",
                "2026-07-02",
                "2026-07-02",
                "2.02,9.01",
            ),
        ];
        assert_eq!(
            paired(&filings),
            vec![
                (date("2026-06-30"), "q2-results"),
                (date("2026-09-30"), "q3-results"),
            ]
        );
    }

    #[test]
    fn a_release_filed_the_same_day_as_its_report_counts() {
        let filings = [
            filing("10k", "10-K", "2026-11-03", "2026-09-26", ""),
            filing("results", "8-K", "2026-11-03", "2026-11-03", "2.02,9.01"),
        ];
        assert_eq!(paired(&filings), vec![(date("2026-09-26"), "results")]);
    }

    #[test]
    fn a_report_filed_after_the_next_period_end_does_not_take_that_periods_release() {
        // Q1's 10-Q filed late, after Q2 has ended and Q2's release is out.
        let filings = [
            filing("q1-10q-late", "10-Q", "2026-07-20", "2026-03-31", ""),
            filing("q1-results", "8-K", "2026-04-28", "2026-04-28", "2.02"),
            filing("q2-results", "8-K", "2026-07-15", "2026-07-15", "2.02"),
            filing("q2-10q", "10-Q", "2026-07-25", "2026-06-30", ""),
        ];
        assert_eq!(
            paired(&filings),
            vec![
                (date("2026-03-31"), "q1-results"),
                (date("2026-06-30"), "q2-results"),
            ]
        );
    }

    #[test]
    fn an_8k_filed_on_the_period_end_day_is_not_that_periods_release() {
        // Results can only follow a period's end; an Item 2.02 filing dated that very day
        // reports something earlier.
        let filings = [
            filing("10q", "10-Q", "2026-08-06", "2026-06-30", ""),
            filing(
                "on-period-end",
                "8-K",
                "2026-06-30",
                "2026-06-30",
                "2.02,9.01",
            ),
        ];
        assert_eq!(paired(&filings), vec![]);
    }

    #[test]
    fn a_period_without_a_filed_report_has_no_release_yet() {
        let filings = [
            filing("q3-results", "8-K", "2026-10-13", "2026-10-13", "2.02,9.01"),
            filing("q2-10q", "10-Q", "2026-08-06", "2026-06-30", ""),
            filing("q2-results", "8-K", "2026-07-14", "2026-07-14", "2.02,9.01"),
        ];
        assert_eq!(paired(&filings), vec![(date("2026-06-30"), "q2-results")]);
    }

    #[test]
    fn only_original_8ks_with_item_2_02_and_original_reports_count() {
        let filings = [
            filing("10q", "10-Q", "2026-08-06", "2026-06-30", ""),
            filing("10q-amended", "10-Q/A", "2026-09-30", "2026-06-30", ""),
            filing(
                "amended-results",
                "8-K/A",
                "2026-07-20",
                "2026-07-14",
                "2.02",
            ),
            filing("other-item", "8-K", "2026-07-30", "2026-07-30", "5.02,9.01"),
            filing("results", "8-K", "2026-07-14", "2026-07-14", "9.01, 2.02"),
            filing("look-alike", "8-K", "2026-07-31", "2026-07-31", "12.02"),
        ];
        assert_eq!(paired(&filings), vec![(date("2026-06-30"), "results")]);
    }

    #[test]
    fn a_period_filed_twice_is_paired_from_its_first_filing() {
        // A transition report and a 10-K for the same period end: the window closes at the first.
        let filings = [
            filing("10kt", "10-KT", "2026-03-01", "2025-12-31", ""),
            filing("10k", "10-K", "2026-04-01", "2025-12-31", ""),
            filing("release", "8-K", "2026-02-10", "2026-02-10", "2.02"),
            filing("after", "8-K", "2026-03-15", "2026-03-15", "2.02"),
        ];
        assert_eq!(paired(&filings), vec![(date("2025-12-31"), "release")]);
    }

    #[test]
    fn same_day_results_8ks_are_ordered_by_acceptance() {
        let mut early = filing("early", "8-K", "2026-07-14", "2026-07-14", "2.02");
        early.acceptance = "2026-07-14T10:30:00.000Z".into();
        let mut late = filing("late", "8-K", "2026-07-14", "2026-07-14", "2.02");
        late.acceptance = "2026-07-14T20:30:00.000Z".into();
        let filings = [
            late,
            early,
            filing("10q", "10-Q", "2026-08-06", "2026-06-30", ""),
        ];
        assert_eq!(paired(&filings), vec![(date("2026-06-30"), "late")]);
    }

    #[test]
    fn a_foreign_private_issuer_is_recognised_and_an_empty_index_is_not_one() {
        let foreign = [
            filing("20f", "20-F", "2026-04-01", "2025-12-31", ""),
            filing("6k", "6-K", "2026-05-01", "", ""),
        ];
        assert_eq!(releases(&foreign), Releases::ForeignPrivateIssuer);
        let amended_only = [filing("6ka", "6-K/A", "2026-05-01", "", "")];
        assert_eq!(releases(&amended_only), Releases::ForeignPrivateIssuer);
        let none = [filing("s1", "S-1", "2026-05-01", "", "")];
        assert_eq!(releases(&none), Releases::Paired(Vec::new()));
    }

    #[test]
    fn columns_decode_into_filings_with_empty_report_dates_and_missing_items() {
        let json = r#"{
            "accessionNumber": ["0000000001-26-000002", "0000000001-26-000001"],
            "filingDate": ["2026-07-14", "2026-05-01"],
            "reportDate": ["2026-07-14", ""],
            "acceptanceDateTime": ["2026-07-14T10:30:38.000Z", "2026-05-01T12:00:00.000Z"],
            "form": ["8-K", "S-8"],
            "items": ["2.02,9.01", ""],
            "size": [1, 2]
        }"#;
        let filings = serde_json::from_str::<Columns>(json)
            .unwrap()
            .into_filings()
            .unwrap();
        assert_eq!(
            filings,
            vec![
                Filing {
                    accession_number: "0000000001-26-000002".into(),
                    form: "8-K".into(),
                    filed: date("2026-07-14"),
                    report_date: Some(date("2026-07-14")),
                    acceptance: "2026-07-14T10:30:38.000Z".into(),
                    items: "2.02,9.01".into(),
                },
                Filing {
                    accession_number: "0000000001-26-000001".into(),
                    form: "S-8".into(),
                    filed: date("2026-05-01"),
                    report_date: None,
                    acceptance: "2026-05-01T12:00:00.000Z".into(),
                    items: String::new(),
                },
            ]
        );

        let no_items = r#"{"accessionNumber": ["a"], "filingDate": ["2001-01-02"],
            "reportDate": [""], "acceptanceDateTime": [""], "form": ["8-K"]}"#;
        let filings = serde_json::from_str::<Columns>(no_items)
            .unwrap()
            .into_filings()
            .unwrap();
        assert_eq!(filings[0].items, "");
    }

    #[test]
    fn columns_of_different_lengths_or_bad_dates_are_refused() {
        let short = r#"{"accessionNumber": ["a", "b"], "filingDate": ["2026-01-02"],
            "reportDate": ["", ""], "acceptanceDateTime": ["", ""], "form": ["8-K", "8-K"]}"#;
        let error = serde_json::from_str::<Columns>(short)
            .unwrap()
            .into_filings()
            .unwrap_err();
        assert!(matches!(error, SecEdgarError::Decode { .. }), "{error:?}");

        let short_items = r#"{"accessionNumber": ["a", "b"], "filingDate": ["2026-01-02", "2026-01-02"],
            "reportDate": ["", ""], "acceptanceDateTime": ["", ""], "form": ["8-K", "8-K"],
            "items": ["2.02"]}"#;
        let error = serde_json::from_str::<Columns>(short_items)
            .unwrap()
            .into_filings()
            .unwrap_err();
        assert!(matches!(error, SecEdgarError::Decode { .. }), "{error:?}");

        let bad_date = r#"{"accessionNumber": ["a"], "filingDate": ["01/02/2026"],
            "reportDate": [""], "acceptanceDateTime": [""], "form": ["8-K"]}"#;
        let error = serde_json::from_str::<Columns>(bad_date)
            .unwrap()
            .into_filings()
            .unwrap_err();
        assert!(matches!(error, SecEdgarError::Decode { .. }), "{error:?}");
    }

    #[test]
    fn the_acceptance_time_is_read_raw_or_escaped() {
        let raw = "<SEC-HEADER>\n<ACCEPTANCE-DATETIME>20261102163023\nACCESSION NUMBER:";
        let escaped = "<pre>&lt;ACCEPTANCE-DATETIME&gt;20260414063242\n</pre>";
        assert_eq!(
            acceptance_time(raw).unwrap(),
            NaiveDateTime::parse_from_str("2026-11-02 16:30:23", "%Y-%m-%d %H:%M:%S").unwrap()
        );
        assert_eq!(
            acceptance_time(escaped).unwrap(),
            NaiveDateTime::parse_from_str("2026-04-14 06:32:42", "%Y-%m-%d %H:%M:%S").unwrap()
        );
        for bad in [
            "no tag here",
            "<ACCEPTANCE-DATETIME>2026",
            "<ACCEPTANCE-DATETIME>2026131",
        ] {
            assert!(
                matches!(acceptance_time(bad), Err(SecEdgarError::Decode { .. })),
                "{bad:?}"
            );
        }
    }

    #[test]
    fn the_session_bounds_split_the_day() {
        let at = |h, m, s| NaiveTime::from_hms_opt(h, m, s).unwrap();
        assert_eq!(timing(at(0, 0, 0)), ReleaseTiming::BeforeOpen);
        assert_eq!(timing(at(9, 29, 59)), ReleaseTiming::BeforeOpen);
        assert_eq!(timing(at(9, 30, 0)), ReleaseTiming::DuringSession);
        assert_eq!(timing(at(15, 59, 59)), ReleaseTiming::DuringSession);
        assert_eq!(timing(at(16, 0, 0)), ReleaseTiming::AfterClose);
        assert_eq!(timing(at(23, 59, 59)), ReleaseTiming::AfterClose);
    }
}
