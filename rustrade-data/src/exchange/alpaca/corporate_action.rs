//! [`StockSplitSource`] implementation for the shared Alpaca REST client.
//!
//! Adapts the lower-level [`AlpacaRestClient::fetch_splits_raw`](super::reference) raw-split stream into
//! the provider-agnostic [`CorporateAction`] sourcing descriptor, computing the split ratio via the
//! shared [`CorporateActionKind::stock_split`] helper so Alpaca derives ratios identically to every
//! other provider.

use async_stream::try_stream;
use futures::{Stream, StreamExt};
use smol_str::SmolStr;
use tracing::warn;

use rustrade_instrument::corporate_action::{CorporateAction, CorporateActionKind};
use rustrade_integration::corporate_action::{CorporateActionFilter, StockSplitSource};

use super::reference::{AlpacaStockSplit, CorporateActionsQuery};
use super::rest::{AlpacaRestClient, AlpacaRestError};

impl StockSplitSource for AlpacaRestClient {
    type Error = AlpacaRestError;

    /// Fetch stock splits for `filter`'s symbols and effective-date range, mapped to
    /// [`CorporateAction<SmolStr>`] descriptors.
    ///
    /// `effective_date` provenance: this impl maps it onto Alpaca's **`ex_date`** — the session on
    /// which the share count re-bases and the price adjusts, satisfying the
    /// [`StockSplitSource::fetch_splits`] market-execution-date contract. It deliberately does NOT
    /// use `payable_date`: for forward splits Alpaca's `payable_date` can fall a trading day
    /// *before* `ex_date` (sampled: `APH` ex 2024-06-12 / payable 2024-06-11; `CNQ` ex 2024-06-11 /
    /// payable 2024-06-10) while the price re-bases on `ex_date` (observed equal to `process_date`
    /// across the sample), so mapping `payable_date` would apply the split a day early — look-ahead
    /// corruption. This mapping is pinned by `ex_date_provenance_ignores_payable_date` (in
    /// [`reference`](super::reference)) and `map_split_maps_ex_date_to_effective_date` below.
    ///
    /// Alpaca's endpoint accepts a single comma-joined `symbols` parameter, so — unlike Massive's
    /// per-symbol fan-out — a multi-symbol filter issues **one** paginated query. An empty `symbols`
    /// list requests every split in the date range (subject to provider page limits). Splits whose
    /// `new_rate` / `old_rate` yield a degenerate ratio (zero, negative, or division by zero) are
    /// logged and skipped — see [`CorporateActionKind::stock_split`].
    fn fetch_splits(
        &self,
        filter: &CorporateActionFilter,
    ) -> impl Stream<Item = Result<CorporateAction<SmolStr>, Self::Error>> + Send {
        // Build the owned query eagerly so the returned stream borrows only `self`, not `filter`.
        let query = build_query(filter);

        try_stream! {
            // Calls the *inherent* `AlpacaRestClient::fetch_splits_raw` (taking `&CorporateActionsQuery`),
            // the lower-level raw-split stream this impl adapts. Its distinct name keeps the dispatch
            // explicit — no collision with this trait's `fetch_splits`, so no risk of recursing into it.
            let raw = self.fetch_splits_raw(&query);
            futures::pin_mut!(raw);
            while let Some(split) = raw.next().await {
                if let Some(action) = map_split(split?) {
                    yield action;
                }
            }
        }
    }
}

/// Translate a [`CorporateActionFilter`] into a single Alpaca [`CorporateActionsQuery`] (Alpaca
/// batches symbols into one comma-joined request).
fn build_query(filter: &CorporateActionFilter) -> CorporateActionsQuery {
    let mut query = CorporateActionsQuery::new();
    if !filter.symbols.is_empty() {
        query = query.symbols(filter.symbols.iter().map(SmolStr::as_str));
    }
    if let Some(start) = filter.start {
        query = query.start(start);
    }
    if let Some(end) = filter.end {
        query = query.end(end);
    }
    query
}

/// Map a raw [`AlpacaStockSplit`] to a [`CorporateAction`] descriptor via the shared ratio helper.
/// Returns `None` (and logs) for a degenerate ratio so the caller's stream simply omits it.
fn map_split(split: AlpacaStockSplit) -> Option<CorporateAction<SmolStr>> {
    match CorporateActionKind::stock_split(split.new_rate, split.old_rate) {
        // `effective_date` maps to Alpaca's `ex_date` — the split's market execution date, per the
        // `StockSplitSource::fetch_splits` contract (NOT `payable_date`; see the impl rustdoc).
        Some(kind) => Some(CorporateAction::new(
            SmolStr::from(split.symbol),
            kind,
            Some(split.ex_date),
        )),
        None => {
            warn!(
                symbol = %split.symbol,
                new_rate = %split.new_rate,
                old_rate = %split.old_rate,
                "Alpaca stock split has a degenerate ratio; skipping",
            );
            None
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Tests should panic on unexpected values
mod tests {
    use super::*;
    use chrono::NaiveDate;
    use rust_decimal::Decimal;
    use rustrade_instrument::corporate_action::SplitRatio;

    fn split(symbol: &str, new_rate: i64, old_rate: i64, ex_date: NaiveDate) -> AlpacaStockSplit {
        AlpacaStockSplit {
            symbol: symbol.to_string(),
            ex_date,
            new_rate: Decimal::from(new_rate),
            old_rate: Decimal::from(old_rate),
        }
    }

    #[test]
    fn build_query_joins_symbols_and_dates() {
        let filter = CorporateActionFilter {
            symbols: vec![SmolStr::new("NVDA"), SmolStr::new("AAPL")],
            start: NaiveDate::from_ymd_opt(2024, 1, 1),
            end: NaiveDate::from_ymd_opt(2024, 12, 31),
        };

        let query = build_query(&filter);
        assert_eq!(query.symbols, vec!["NVDA".to_string(), "AAPL".to_string()]);
        assert_eq!(query.start, filter.start);
        assert_eq!(query.end, filter.end);
    }

    #[test]
    fn build_query_empty_symbols_is_unrestricted() {
        let filter = CorporateActionFilter::default();
        let query = build_query(&filter);
        assert!(query.symbols.is_empty());
        assert!(query.start.is_none());
        assert!(query.end.is_none());
    }

    #[test]
    fn map_split_forward_ratio() {
        let action = map_split(split(
            "NVDA",
            10,
            1,
            NaiveDate::from_ymd_opt(2024, 6, 10).unwrap(),
        ))
        .unwrap();
        assert_eq!(action.instrument, SmolStr::new("NVDA"));
        assert_eq!(
            action.kind,
            CorporateActionKind::StockSplit {
                ratio: SplitRatio::new(Decimal::from(10)).unwrap()
            }
        );
    }

    #[test]
    fn map_split_reverse_ratio() {
        // 1-for-25 reverse → ratio 0.04.
        let action = map_split(split(
            "ATRA",
            1,
            25,
            NaiveDate::from_ymd_opt(2024, 6, 20).unwrap(),
        ))
        .unwrap();
        assert_eq!(
            action.kind,
            CorporateActionKind::StockSplit {
                ratio: SplitRatio::new(Decimal::new(4, 2)).unwrap()
            }
        );
    }

    #[test]
    fn map_split_maps_ex_date_to_effective_date() {
        // Pins the provenance: `effective_date` is the raw split's `ex_date`. Paired with the
        // `reference::ex_date_provenance_ignores_payable_date` deser test (which proves the parsed
        // `ex_date` is the JSON `ex_date`, not `payable_date`), this covers the full chain.
        let ex_date = NaiveDate::from_ymd_opt(2024, 6, 12).unwrap();
        let action = map_split(split("APH", 2, 1, ex_date)).unwrap();
        assert_eq!(action.effective_date, Some(ex_date));
    }

    #[test]
    fn map_split_drops_degenerate_ratio() {
        // old_rate == 0 → division by zero → skipped.
        assert!(
            map_split(split(
                "BAD",
                1,
                0,
                NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()
            ))
            .is_none()
        );
    }
}
