//! [`StockSplitSource`] implementation for the Massive REST client.
//!
//! Adapts the lower-level [`MassiveRestClient::fetch_splits_raw`](super::rest::MassiveRestClient)
//! raw-split stream into the provider-agnostic
//! [`CorporateAction`] sourcing descriptor, computing the split ratio via the shared
//! [`CorporateActionKind::stock_split`] helper so Massive derives ratios identically to every other
//! provider.

use async_stream::try_stream;
use futures::{Stream, StreamExt};
use smol_str::SmolStr;
use tracing::warn;

use rustrade_instrument::corporate_action::{CorporateAction, CorporateActionKind};
use rustrade_integration::corporate_action::{CorporateActionFilter, StockSplitSource};

use super::error::MassiveError;
use super::reference::{SplitQuery, StockSplit};
use super::rest::MassiveRestClient;

impl StockSplitSource for MassiveRestClient {
    type Error = MassiveError;

    /// Fetch stock splits for `filter`'s symbols and effective-date range, mapped to
    /// [`CorporateAction<SmolStr>`] descriptors.
    ///
    /// `effective_date` provenance: this impl maps it onto Massive's `execution_date` — the split's
    /// market execution date, satisfying the [`StockSplitSource::fetch_splits`] `effective_date`
    /// contract. (`StockSplit` exposes no other date field, so there is no ambiguity to resolve.)
    /// Massive's `/v3/reference/splits` endpoint filters by a **single** ticker, so a multi-symbol filter
    /// fans out into one query per symbol, concatenated into a single stream in `symbols` order; an
    /// **empty** `symbols` list issues one unfiltered query (all splits in the date range, subject
    /// to provider page limits). Splits whose raw `split_to` / `split_from` yield a degenerate ratio
    /// (zero, negative, or division by zero) are logged and skipped — see
    /// [`CorporateActionKind::stock_split`].
    fn fetch_splits(
        &self,
        filter: &CorporateActionFilter,
    ) -> impl Stream<Item = Result<CorporateAction<SmolStr>, Self::Error>> + Send {
        // Build owned queries eagerly so the returned stream borrows only `self`, not `filter`.
        let queries = build_split_queries(filter);

        try_stream! {
            for query in &queries {
                // Calls the *inherent* `MassiveRestClient::fetch_splits_raw` (taking `&SplitQuery`),
                // the lower-level raw-split stream this impl adapts. Its distinct name keeps the
                // dispatch explicit — no collision with this trait's `fetch_splits`, so no recursion.
                let raw = self.fetch_splits_raw(query);
                futures::pin_mut!(raw);
                while let Some(split) = raw.next().await {
                    if let Some(action) = map_split(split?) {
                        yield action;
                    }
                }
            }
        }
    }
}

/// Translate a [`CorporateActionFilter`] into one or more Massive [`SplitQuery`]s (one per symbol,
/// or a single date-only query when no symbols are given).
fn build_split_queries(filter: &CorporateActionFilter) -> Vec<SplitQuery> {
    // These queries only ever set the `execution_date_gte`/`lte` range — never the exact
    // `execution_date` field. `SplitQuery::validate()` (run inside the inner `fetch_splits_raw` stream)
    // rejects only the exact+range combination, so queries built here can never trip it.
    let with_dates = |mut query: SplitQuery| {
        if let Some(start) = filter.start {
            query = query.execution_date_gte(start);
        }
        if let Some(end) = filter.end {
            query = query.execution_date_lte(end);
        }
        query
    };

    if filter.symbols.is_empty() {
        vec![with_dates(SplitQuery::new())]
    } else {
        filter
            .symbols
            .iter()
            .map(|symbol| with_dates(SplitQuery::new().ticker(symbol.as_str())))
            .collect()
    }
}

/// Map a raw [`StockSplit`] to a [`CorporateAction`] descriptor via the shared ratio helper.
/// Returns `None` (and logs) for a degenerate ratio so the caller's stream simply omits it.
fn map_split(split: StockSplit) -> Option<CorporateAction<SmolStr>> {
    match CorporateActionKind::stock_split(split.split_to, split.split_from) {
        // `effective_date` maps to Massive's `execution_date` — the split's market execution date,
        // per the `StockSplitSource::fetch_splits` contract.
        Some(kind) => Some(CorporateAction::new(
            SmolStr::from(split.ticker),
            kind,
            Some(split.execution_date),
        )),
        None => {
            warn!(
                ticker = %split.ticker,
                split_to = %split.split_to,
                split_from = %split.split_from,
                "Massive stock split has a degenerate ratio; skipping",
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

    #[test]
    fn build_split_queries_fans_out_per_symbol() {
        let filter = CorporateActionFilter {
            symbols: vec![SmolStr::new("AAPL"), SmolStr::new("NVDA")],
            start: NaiveDate::from_ymd_opt(2020, 1, 1),
            end: NaiveDate::from_ymd_opt(2024, 12, 31),
        };

        let queries = build_split_queries(&filter);
        assert_eq!(queries.len(), 2);
        assert_eq!(queries[0].ticker.as_deref(), Some("AAPL"));
        assert_eq!(queries[1].ticker.as_deref(), Some("NVDA"));
        // Date bounds are applied to every per-symbol query.
        assert_eq!(queries[0].execution_date_gte, filter.start);
        assert_eq!(queries[0].execution_date_lte, filter.end);
    }

    #[test]
    fn build_split_queries_empty_symbols_is_one_unfiltered_query() {
        let filter = CorporateActionFilter {
            symbols: vec![],
            start: NaiveDate::from_ymd_opt(2023, 1, 1),
            end: None,
        };

        let queries = build_split_queries(&filter);
        assert_eq!(queries.len(), 1);
        assert_eq!(queries[0].ticker, None);
        assert_eq!(queries[0].execution_date_gte, filter.start);
        assert_eq!(queries[0].execution_date_lte, None);
    }

    #[test]
    fn map_split_computes_forward_ratio() {
        let split = StockSplit {
            ticker: "AAPL".to_string(),
            execution_date: NaiveDate::from_ymd_opt(2020, 8, 31).unwrap(),
            split_to: Decimal::from(4),
            split_from: Decimal::from(1),
        };

        let action = map_split(split).unwrap();
        assert_eq!(action.instrument, SmolStr::new("AAPL"));
        assert_eq!(
            action.kind,
            CorporateActionKind::StockSplit {
                ratio: Decimal::from(4)
            }
        );
        // Pins the provenance: `effective_date` is mapped from the raw split's `execution_date`.
        assert_eq!(action.effective_date, NaiveDate::from_ymd_opt(2020, 8, 31));
    }

    #[test]
    fn map_split_drops_degenerate_ratio() {
        let split = StockSplit {
            ticker: "BAD".to_string(),
            execution_date: NaiveDate::from_ymd_opt(2023, 1, 1).unwrap(),
            split_to: Decimal::from(1),
            split_from: Decimal::ZERO,
        };
        assert!(map_split(split).is_none());
    }

    #[tokio::test]
    async fn fetch_splits_via_trait_object_is_stream_of_descriptors() {
        // A tiny in-test source confirms the trait shape drives end-to-end without a live client.
        struct OneSplit;
        impl StockSplitSource for OneSplit {
            type Error = std::convert::Infallible;
            fn fetch_splits(
                &self,
                _filter: &CorporateActionFilter,
            ) -> impl Stream<Item = Result<CorporateAction<SmolStr>, Self::Error>> + Send
            {
                futures::stream::iter([Ok(CorporateAction::new(
                    SmolStr::new("TSLA"),
                    CorporateActionKind::StockSplit {
                        ratio: Decimal::from(3),
                    },
                    NaiveDate::from_ymd_opt(2022, 8, 25),
                ))])
            }
        }

        let actions: Vec<_> = OneSplit
            .fetch_splits(&CorporateActionFilter::default())
            .collect()
            .await;
        assert_eq!(actions.len(), 1);
        assert_eq!(
            actions[0].as_ref().unwrap().instrument,
            SmolStr::new("TSLA")
        );
    }
}
