//! Alpha Vantage's `SPLITS` endpoint, as a [`StockSplitSource`].

use super::{client::AlphaVantageClient, error::AlphaVantageError, records};
use async_stream::try_stream;
use futures::Stream;
use rust_decimal::Decimal;
use rustrade_instrument::corporate_action::{CorporateAction, CorporateActionKind};
use rustrade_integration::corporate_action::{CorporateActionFilter, StockSplitSource};
use serde::Deserialize;
use smol_str::SmolStr;
use std::str::FromStr;
use tracing::warn;

/// The largest denominator [`ratio`] tries when recovering a split's exact ratio from its rounded
/// factor.
const MAX_DENOMINATOR: i64 = 100;

/// How many decimals Alpha Vantage rounds a split factor to.
const FACTOR_DECIMALS: u32 = 4;

impl StockSplitSource for AlphaVantageClient {
    type Error = AlphaVantageError;

    /// Fetch the stock splits of `filter`'s symbols whose effective date is within its range, one
    /// request per symbol, each symbol's oldest first.
    ///
    /// `effective_date` provenance: this impl maps it onto Alpha Vantage's `effective_date`, the
    /// first day the shares trade on the split basis, satisfying the
    /// [`StockSplitSource::fetch_splits`] market-execution-date contract. Alpha Vantage gives no
    /// other date for a split. Pinned by `the_effective_date_and_factor_map_to_the_action`.
    ///
    /// Alpha Vantage's `split_factor` is new shares per old share, rounded to four decimals.
    /// When a ratio of two whole numbers with a denominator of at most 100 rounds to that factor,
    /// the split is that ratio, so a one-for-three reverse split (`0.3333`) is exactly 1/3;
    /// otherwise it is the factor itself. A factor that is zero or negative is logged and skipped
    /// (see [`CorporateActionKind::stock_split`]).
    ///
    /// The splits are known only as history: Alpha Vantage gives no declaration date, so a split
    /// is not known to be coming before it takes effect.
    ///
    /// # Errors
    /// - [`AlphaVantageError::InvalidInput`], as the only item, if `filter` names no symbol:
    ///   Alpha Vantage reads one symbol at a time. Also for an empty symbol, in its place.
    /// - Any request error, in place of that symbol's splits. The stream ends there.
    ///
    /// An unknown symbol reads as one with no splits: Alpha Vantage answers both alike.
    fn fetch_splits(
        &self,
        filter: &CorporateActionFilter,
    ) -> impl Stream<Item = Result<CorporateAction<SmolStr>, Self::Error>> + Send {
        // Owned copies, so the returned stream borrows only `self`, not `filter`.
        let symbols = filter.symbols.clone();
        let (start, end) = (filter.start, filter.end);
        try_stream! {
            if symbols.is_empty() {
                Err(AlphaVantageError::invalid_input(
                    "Alpha Vantage reads splits one symbol at a time: the filter names no symbol",
                ))?;
            }
            for symbol in &symbols {
                let symbol = super::client::checked_symbol(symbol)?;
                let body = self.get(&[("function", "SPLITS"), ("symbol", symbol)]).await?;
                if let Some(error) = records::notice(&body) {
                    Err(error)?;
                }
                for action in splits(&body, symbol)? {
                    let date = action.effective_date;
                    if start.is_none_or(|start| date.is_some_and(|d| d >= start))
                        && end.is_none_or(|end| date.is_some_and(|d| d <= end))
                    {
                        yield action;
                    }
                }
            }
        }
    }
}

#[derive(Deserialize)]
struct Splits {
    data: Vec<Row>,
}

#[derive(Deserialize)]
struct Row {
    effective_date: String,
    split_factor: String,
}

/// The splits in a `SPLITS` body for `symbol`, oldest first, skipping (and logging) any whose
/// factor is not positive.
fn splits(body: &str, symbol: &str) -> Result<Vec<CorporateAction<SmolStr>>, AlphaVantageError> {
    let response: Splits = serde_json::from_str(body)
        .map_err(|e| AlphaVantageError::decode(format!("splits: {e}")))?;
    let mut actions = Vec::with_capacity(response.data.len());
    for row in &response.data {
        let date = records::date(&row.effective_date, "effective_date")?;
        let factor = Decimal::from_str(row.split_factor.trim()).map_err(|_| {
            AlphaVantageError::decode(format!(
                "split_factor {:?} is not a number",
                row.split_factor
            ))
        })?;
        let (split_to, split_from) = ratio(factor);
        match CorporateActionKind::stock_split(split_to, split_from) {
            Some(kind) => actions.push(CorporateAction::new(
                SmolStr::from(symbol),
                kind,
                Some(date),
            )),
            None => warn!(
                %symbol,
                effective_date = %date,
                %factor,
                "Alpha Vantage stock split has a degenerate factor; skipping",
            ),
        }
    }
    actions.sort_by_key(|action| action.effective_date);
    Ok(actions)
}

/// The split `factor` (new shares per old, rounded to [`FACTOR_DECIMALS`]) as new and old share
/// counts: the whole numbers with the smallest denominator up to [`MAX_DENOMINATOR`] whose ratio
/// rounds to `factor`, else `factor` over one.
///
/// Within that bound the ratio is unique: two ratios with denominators of at most 100 differ by
/// more than 1/10,000, wider than the half a unit of rounding either side of a factor.
///
/// A factor of zero or below needs no check of its own: it comes back over one, as zero or the
/// same negative factor, for [`CorporateActionKind::stock_split`] to refuse.
fn ratio(factor: Decimal) -> (Decimal, Decimal) {
    for denominator in 1..=MAX_DENOMINATOR {
        let denominator = Decimal::from(denominator);
        let numerator = (factor * denominator).round();
        if (numerator / denominator).round_dp(FACTOR_DECIMALS) == factor {
            return (numerator, denominator);
        }
    }
    (factor, Decimal::ONE)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use chrono::NaiveDate;
    use rust_decimal_macros::dec;
    use rustrade_instrument::corporate_action::SplitRatio;

    fn day(y: i32, m: u32, d: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, d).unwrap()
    }

    fn split_ratio(action: &CorporateAction<SmolStr>) -> Decimal {
        match action.kind {
            CorporateActionKind::StockSplit { ratio } => ratio.get(),
            ref other => panic!("not a split: {other:?}"),
        }
    }

    #[test]
    fn the_effective_date_and_factor_map_to_the_action() {
        // Synthetic rows, newest first as Alpha Vantage sends them.
        let body = r#"{"symbol": "XYZ", "data": [
            {"effective_date": "2024-06-10", "split_factor": "10.0000"},
            {"effective_date": "2021-07-20", "split_factor": "4.0000"},
            {"effective_date": "2007-09-11", "split_factor": "1.5000"}
        ]}"#;
        let actions = splits(body, "XYZ").unwrap();
        let rows: Vec<_> = actions
            .iter()
            .map(|a| (a.instrument.as_str(), a.effective_date, split_ratio(a)))
            .collect();
        assert_eq!(
            rows,
            vec![
                ("XYZ", Some(day(2007, 9, 11)), dec!(1.5)),
                ("XYZ", Some(day(2021, 7, 20)), dec!(4)),
                ("XYZ", Some(day(2024, 6, 10)), dec!(10)),
            ]
        );
    }

    #[test]
    fn a_rounded_reverse_factor_becomes_its_exact_ratio() {
        let body = r#"{"data": [
            {"effective_date": "2025-01-02", "split_factor": "0.3333"},
            {"effective_date": "2025-02-03", "split_factor": "0.6667"},
            {"effective_date": "2025-03-03", "split_factor": "0.0667"}
        ]}"#;
        let ratios: Vec<_> = splits(body, "XYZ")
            .unwrap()
            .iter()
            .map(split_ratio)
            .collect();
        assert_eq!(
            ratios,
            [
                SplitRatio::new(dec!(1) / dec!(3)).unwrap().get(),
                SplitRatio::new(dec!(2) / dec!(3)).unwrap().get(),
                SplitRatio::new(dec!(1) / dec!(15)).unwrap().get(),
            ]
        );
    }

    #[test]
    fn ratio_takes_the_smallest_denominator_and_falls_back_to_the_factor() {
        assert_eq!(ratio(dec!(2.0000)), (dec!(2), dec!(1)));
        assert_eq!(ratio(dec!(1.5000)), (dec!(3), dec!(2)));
        assert_eq!(ratio(dec!(1.2500)), (dec!(5), dec!(4)));
        assert_eq!(ratio(dec!(0.3333)), (dec!(1), dec!(3)));
        assert_eq!(ratio(dec!(0.1000)), (dec!(1), dec!(10)));
        assert_eq!(ratio(dec!(0.0100)), (dec!(1), dec!(100)));
        // 1-for-150 rounds to 0.0067, which no denominator up to 100 reaches.
        assert_eq!(ratio(dec!(0.0067)), (dec!(0.0067), dec!(1)));
        assert_eq!(ratio(dec!(0.0005)), (dec!(0.0005), dec!(1)));
        assert_eq!(ratio(Decimal::ZERO), (Decimal::ZERO, Decimal::ONE));
        assert_eq!(ratio(dec!(-2)), (dec!(-2), Decimal::ONE));
    }

    #[test]
    fn a_degenerate_factor_is_skipped_and_a_bad_row_is_a_decode_error() {
        let body = r#"{"data": [
            {"effective_date": "2025-01-02", "split_factor": "0.0000"},
            {"effective_date": "2025-02-03", "split_factor": "-2.0000"},
            {"effective_date": "2025-03-03", "split_factor": "2.0000"}
        ]}"#;
        let actions = splits(body, "XYZ").unwrap();
        assert_eq!(
            actions.iter().map(|a| a.effective_date).collect::<Vec<_>>(),
            [Some(day(2025, 3, 3))]
        );
        assert_eq!(splits(r#"{"data": []}"#, "XYZ").unwrap(), vec![]);
        for bad in [
            "{}",
            "not json",
            r#"{"data": [{"effective_date": "None", "split_factor": "2.0"}]}"#,
            r#"{"data": [{"effective_date": "2025-01-02", "split_factor": "two"}]}"#,
            r#"{"data": [{"split_factor": "2.0"}]}"#,
        ] {
            assert!(
                matches!(splits(bad, "XYZ"), Err(AlphaVantageError::Decode { .. })),
                "{bad}"
            );
        }
    }
}
