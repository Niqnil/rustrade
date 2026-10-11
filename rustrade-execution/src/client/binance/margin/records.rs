//! A margin account's history read over REST: the interest Binance charged, and the orders it
//! placed to liquidate the account.
//!
//! Neither arrives on the account stream, so the caller reads them when it wants them.

use super::BinanceMargin;
use crate::{
    cash_flow::{CashFlow, CashFlowId, CashFlowKind},
    client::binance::shared::{
        RequestKind, classify_rest_query_error, parse_side, response_decode_error,
        rest_call_with_retry,
    },
    error::UnindexedClientError,
    order::id::OrderId,
};
use binance_sdk::margin_trading::rest_api::{
    GetForceLiquidationRecordParams, GetForceLiquidationRecordResponseRowsInner,
    GetInterestHistoryParams, GetInterestHistoryResponseRowsInner,
};
use chrono::{DateTime, TimeDelta, TimeZone, Utc};
use fnv::FnvHashSet;
use futures::{StreamExt as _, TryStreamExt as _};
use rust_decimal::Decimal;
use rustrade_instrument::{
    Side, asset::name::AssetNameExchange, instrument::name::InstrumentNameExchange,
};
use serde::{Deserialize, Serialize};
use smol_str::format_smolstr;
use std::{future::Future, str::FromStr};
use tracing::{debug, warn};

/// How far back Binance serves interest history.
const INTEREST_LOOKBACK: TimeDelta = TimeDelta::days(90);

/// Kept clear of [`INTEREST_LOOKBACK`]'s edge, so that a start at the edge is not refused for any
/// skew between the local clock and Binance's.
const INTEREST_LOOKBACK_MARGIN: TimeDelta = TimeDelta::minutes(1);

/// The span of one interest history request. Binance serves at most 30 days per request, and
/// says the limit is needed for the data to be correct; a day less leaves room for how it counts.
const INTEREST_WINDOW_MS: i64 = 29 * 24 * 60 * 60 * 1000;

/// The most rows Binance returns in one page of either history.
const PAGE_SIZE: i64 = 100;

/// How many isolated pairs are read at once.
const CONCURRENT_PAIRS: usize = 8;

/// An order Binance placed to liquidate a margin account, as its forced-liquidation record
/// reports it.
///
/// Read by [`BinanceMargin::fetch_forced_liquidations`].
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub struct MarginForcedLiquidation {
    /// Binance's id for the liquidation order.
    ///
    /// Binance does not document that a fill of this order reaches the account stream, nor that
    /// one would carry this id as its order id. Until a real liquidation shows it, matching the
    /// two is the caller's assumption.
    pub order_id: OrderId,
    /// The pair the order traded.
    pub instrument: InstrumentNameExchange,
    /// The order's side: [`Sell`](Side::Sell) to close a long, [`Buy`](Side::Buy) a short.
    pub side: Side,
    /// The order's limit price, if Binance reports one.
    pub price: Option<Decimal>,
    /// The order's quantity, if Binance reports one.
    pub quantity: Option<Decimal>,
    /// How much of the order filled.
    pub filled_quantity: Decimal,
    /// The average price of the filled quantity. `None` when nothing filled.
    pub avg_price: Option<Decimal>,
    /// When Binance last updated the order.
    pub time_exchange: DateTime<Utc>,
}

impl BinanceMargin {
    /// Read the interest Binance charged the account from `start` to `end`, each charge as a
    /// [`CashFlow`] of [`MarginInterest`](CashFlowKind::MarginInterest), oldest first.
    ///
    /// A cross-margin client reads the cross account. An isolated one reads each pair in its
    /// [`isolated_symbols`](super::BinanceMarginConfig::isolated_symbols), and attributes each
    /// charge to its pair through [`CashFlow::instrument`]; a cross charge is attributed to none.
    ///
    /// # What a charge is
    /// Binance charges interest hourly, and once when a loan is taken, by adding it to the loan
    /// rather than taking it from the account's free balance. A charge is cash only once the loan
    /// is repaid. The flow's negative [`amount`](CashFlow::amount) is the cost charged at
    /// [`time_exchange`](CashFlow::time_exchange), not a balance that moved then: the account's
    /// balance shows it as interest owed on the asset.
    ///
    /// - [`asset`](CashFlow::asset) is the asset the interest was charged in. That is the asset
    ///   borrowed, unless Binance converted the charge into BNB, as it does for an account that
    ///   pays interest in BNB. Then it is BNB, while
    ///   [`principal`](CashFlowKind::MarginInterest::principal) is still the loan, in the asset
    ///   borrowed.
    /// - [`rate`](CashFlowKind::MarginInterest::rate) is the rate charged for the period, hourly
    ///   for an hourly charge, as Binance quotes it.
    /// - [`id`](CashFlow::id) is Binance's transaction id for the charge.
    ///
    /// The engine adds a flow attributed to an instrument to the carry of its open position only
    /// when the flow is in the instrument's quote asset. Interest on a short's borrowed base asset,
    /// or interest paid in BNB, is not: the engine leaves it out of carry and says so. To count it,
    /// convert it into the quote asset first.
    ///
    /// # Delivery
    /// Nothing is pushed: the caller decides when to read, and must drop the charges it has
    /// already counted, by [`id`](CashFlow::id), before applying a read's flows. Within one read
    /// each charge appears once.
    ///
    /// # Span
    /// - Binance serves interest charged in the last 90 days only. A `start` before that is moved
    ///   up to a minute inside that limit, clear of any skew between the clocks, and logged at
    ///   WARN.
    /// - Charges are read up to `end`, or up to the local clock at the call if that is earlier.
    /// - Binance serves at most 30 days per request, so a span is read in windows of 29 days,
    ///   each paged by 100 charges. A read costs a request weight of 1 per page, at least 1 per
    ///   window, and, on an isolated client, as much again for each pair.
    ///
    /// # Errors
    /// Fails if any request fails. Nothing is returned from a read that failed part way.
    pub async fn fetch_margin_interest(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<CashFlow<AssetNameExchange, InstrumentNameExchange>>, UnindexedClientError>
    {
        let now = Utc::now();
        let horizon = now - INTEREST_LOOKBACK + INTEREST_LOOKBACK_MARGIN;
        let start = if start < horizon {
            warn!(
                requested = %start,
                served_from = %horizon,
                "BinanceMargin fetch_margin_interest: Binance serves 90 days of interest history \
                 — reading from the earliest time it serves"
            );
            horizon
        } else {
            start
        };
        let end = end.min(now);
        if start > end {
            return Ok(Vec::new());
        }
        let (start_ms, end_ms) = (start.timestamp_millis(), end.timestamp_millis());

        let rows = self
            .read_each_account(|pair| self.interest_rows(pair, start_ms, end_ms))
            .await?;

        let mut seen = FnvHashSet::default();
        let mut flows: Vec<_> = rows
            .into_iter()
            .flat_map(|(pair, rows)| {
                rows.into_iter()
                    .filter_map(move |row| convert_interest(row, pair.as_ref()))
            })
            .filter(|flow| (start..=end).contains(&flow.time_exchange))
            // Consecutive windows share their boundary, and a page can repeat a row when a new
            // charge lands mid-walk and shifts the pages after it.
            .filter(|flow| seen.insert(flow.clone()))
            .collect();
        flows.sort_by_key(|flow| flow.time_exchange);
        Ok(flows)
    }

    /// Read the orders Binance placed from `start` to `end` to liquidate the account, oldest
    /// first.
    ///
    /// A cross-margin client reads the cross account. An isolated one reads each pair in its
    /// [`isolated_symbols`](super::BinanceMarginConfig::isolated_symbols).
    ///
    /// Binance marks no liquidation on the account stream, so this client reports a liquidation
    /// order's fills as [`TradeOrigin::Order`](crate::trade::TradeOrigin::Order). A caller that
    /// learns the account is being liquidated, as from a margin-level notice, can read these
    /// records and match their [`order_id`](MarginForcedLiquidation::order_id)s against the fills
    /// it has, with the caveat on that field.
    ///
    /// # Span
    /// Records are read up to `end`, or up to the local clock at the call if that is earlier, in
    /// pages of 100. Binance documents no limit on the span or on how far back it serves. A read
    /// costs a request weight of 1 per page and, on an isolated client, as much again for each
    /// pair.
    ///
    /// # Errors
    /// Fails if any request fails. Nothing is returned from a read that failed part way.
    pub async fn fetch_forced_liquidations(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<MarginForcedLiquidation>, UnindexedClientError> {
        let end = end.min(Utc::now());
        if start > end {
            return Ok(Vec::new());
        }
        let (start_ms, end_ms) = (start.timestamp_millis(), end.timestamp_millis());

        let rows = self
            .read_each_account(|pair| {
                read_all_pages(move |current| {
                    self.forced_liquidation_page(pair.clone(), start_ms, end_ms, current)
                })
            })
            .await?;

        let mut seen = FnvHashSet::default();
        let mut liquidations: Vec<_> = rows
            .into_iter()
            .flat_map(|(_, rows)| rows)
            .filter_map(convert_forced_liquidation)
            .filter(|liquidation| (start..=end).contains(&liquidation.time_exchange))
            // A page can repeat a row when a new record lands mid-walk and shifts the pages after
            // it. Pages run newest first, so the copy kept is the latest.
            .filter(|liquidation| {
                seen.insert((liquidation.instrument.clone(), liquidation.order_id.clone()))
            })
            .collect();
        liquidations.sort_by_key(|liquidation| liquidation.time_exchange);
        Ok(liquidations)
    }

    /// Run `read` once per account this client covers: once for the cross account (`None`), or
    /// once per configured isolated pair. Returns each account's rows with the pair.
    async fn read_each_account<'a, Row, Fut>(
        &'a self,
        read: impl Fn(Option<InstrumentNameExchange>) -> Fut,
    ) -> Result<Vec<(Option<InstrumentNameExchange>, Vec<Row>)>, UnindexedClientError>
    where
        Fut: Future<Output = Result<Vec<Row>, UnindexedClientError>> + 'a,
    {
        let accounts: Vec<Option<InstrumentNameExchange>> = if self.config.is_isolated {
            self.config
                .isolated_symbols
                .iter()
                .cloned()
                .map(Some)
                .collect()
        } else {
            vec![None]
        };
        futures::stream::iter(accounts.into_iter().map(|pair| {
            let rows = read(pair.clone());
            async move { Ok::<_, UnindexedClientError>((pair, rows.await?)) }
        }))
        .buffer_unordered(CONCURRENT_PAIRS)
        .try_collect()
        .await
    }

    /// Every interest row of one account from `start_ms` to `end_ms`, window by window.
    async fn interest_rows(
        &self,
        pair: Option<InstrumentNameExchange>,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<GetInterestHistoryResponseRowsInner>, UnindexedClientError> {
        let mut rows = Vec::new();
        let mut window_start = start_ms;
        loop {
            let window_end = window_start.saturating_add(INTEREST_WINDOW_MS).min(end_ms);
            let pair = &pair;
            rows.extend(
                read_all_pages(|current| {
                    self.interest_page(pair.clone(), window_start, window_end, current)
                })
                .await?,
            );
            if window_end >= end_ms {
                break;
            }
            // The next window starts on this one's end, so a charge stamped on the boundary is
            // read whether Binance treats `endTime` as inclusive or not. The copy is dropped later.
            window_start = window_end;
        }
        debug!(
            pair = ?pair,
            rows = rows.len(),
            "BinanceMargin fetch_margin_interest: account read"
        );
        Ok(rows)
    }

    /// One page of `interestHistory`.
    async fn interest_page(
        &self,
        pair: Option<InstrumentNameExchange>,
        start_ms: i64,
        end_ms: i64,
        current: i64,
    ) -> Result<Vec<GetInterestHistoryResponseRowsInner>, UnindexedClientError> {
        let symbol = pair.as_ref().map(|pair| pair.name().to_string());
        let response =
            rest_call_with_retry(&self.rest, &self.rate_limiter, RequestKind::Query, |rest| {
                let symbol = symbol.clone();
                Box::pin(async move {
                    let params = GetInterestHistoryParams::builder()
                        .isolated_symbol(symbol)
                        .start_time(start_ms)
                        .end_time(end_ms)
                        .current(current)
                        .size(PAGE_SIZE)
                        .build()?;
                    rest.get_interest_history(params).await
                })
            })
            .await
            .map_err(|e| classify_rest_query_error(&e, pair.as_ref()))?;
        let page = response.data().await.map_err(response_decode_error)?;
        Ok(page.rows.unwrap_or_default())
    }

    /// One page of `forceLiquidationRec`.
    async fn forced_liquidation_page(
        &self,
        pair: Option<InstrumentNameExchange>,
        start_ms: i64,
        end_ms: i64,
        current: i64,
    ) -> Result<Vec<GetForceLiquidationRecordResponseRowsInner>, UnindexedClientError> {
        let symbol = pair.as_ref().map(|pair| pair.name().to_string());
        let response =
            rest_call_with_retry(&self.rest, &self.rate_limiter, RequestKind::Query, |rest| {
                let symbol = symbol.clone();
                Box::pin(async move {
                    let params = GetForceLiquidationRecordParams::builder()
                        .isolated_symbol(symbol)
                        .start_time(start_ms)
                        .end_time(end_ms)
                        .current(current)
                        .size(PAGE_SIZE)
                        .build()?;
                    rest.get_force_liquidation_record(params).await
                })
            })
            .await
            .map_err(|e| classify_rest_query_error(&e, pair.as_ref()))?;
        let page = response.data().await.map_err(response_decode_error)?;
        Ok(page.rows.unwrap_or_default())
    }
}

/// Read pages `1, 2, …` of a paged history query of [`PAGE_SIZE`] rows each, up to the first
/// short page.
async fn read_all_pages<Row, Fut>(
    mut page: impl FnMut(i64) -> Fut,
) -> Result<Vec<Row>, UnindexedClientError>
where
    Fut: Future<Output = Result<Vec<Row>, UnindexedClientError>>,
{
    let mut rows = Vec::new();
    let mut current = 1;
    loop {
        let batch = page(current).await?;
        let full = i64::try_from(batch.len()).is_ok_and(|len| len >= PAGE_SIZE);
        rows.extend(batch);
        if !full {
            return Ok(rows);
        }
        current += 1;
    }
}

/// One `interestHistory` row as a [`CashFlow`], or `None`, with a warning, if it lacks what a flow
/// needs. `pair` is the isolated pair the row was read for, if any.
fn convert_interest(
    row: GetInterestHistoryResponseRowsInner,
    pair: Option<&InstrumentNameExchange>,
) -> Option<CashFlow<AssetNameExchange, InstrumentNameExchange>> {
    let tx_id = row.tx_id;
    let Some(asset) = row.asset.as_deref() else {
        warn!(
            ?tx_id,
            ?pair,
            "BinanceMargin interest record missing asset — skipped"
        );
        return None;
    };
    let Some(interest) = row
        .interest
        .as_deref()
        .and_then(|s| Decimal::from_str(s).ok())
    else {
        warn!(
            ?tx_id,
            ?pair,
            "BinanceMargin interest record missing/unparseable interest — skipped"
        );
        return None;
    };
    let Some(time_exchange) = row
        .interest_accured_time
        .and_then(|ms| Utc.timestamp_millis_opt(ms).single())
    else {
        warn!(
            ?tx_id,
            ?pair,
            "BinanceMargin interest record missing/unparseable time — skipped"
        );
        return None;
    };
    let parse = |field: Option<&str>| field.and_then(|s| Decimal::from_str(s).ok());
    let instrument = row
        .isolated_symbol
        .as_deref()
        .map(InstrumentNameExchange::new)
        .or_else(|| pair.cloned());
    Some(CashFlow::new(
        CashFlowKind::MarginInterest {
            rate: parse(row.interest_rate.as_deref()),
            principal: parse(row.principal.as_deref()),
        },
        AssetNameExchange::new(asset),
        -interest,
        instrument,
        time_exchange,
        tx_id.map(|id| CashFlowId(format_smolstr!("{id}"))),
    ))
}

/// One `forceLiquidationRec` row as a [`MarginForcedLiquidation`], or `None`, with a warning, if
/// it lacks what one needs.
fn convert_forced_liquidation(
    row: GetForceLiquidationRecordResponseRowsInner,
) -> Option<MarginForcedLiquidation> {
    let Some(order_id) = row.order_id else {
        warn!(symbol = ?row.symbol, "BinanceMargin forced liquidation record missing orderId — skipped");
        return None;
    };
    let Some(symbol) = row.symbol.as_deref() else {
        warn!(
            order_id,
            "BinanceMargin forced liquidation record missing symbol — skipped"
        );
        return None;
    };
    let Some(side) = row.side.as_deref().and_then(parse_side) else {
        warn!(
            order_id,
            symbol, "BinanceMargin forced liquidation record missing/unknown side — skipped"
        );
        return None;
    };
    let parse = |field: Option<&str>| field.and_then(|s| Decimal::from_str(s).ok());
    let Some(filled_quantity) = parse(row.executed_qty.as_deref()) else {
        warn!(
            order_id,
            symbol,
            "BinanceMargin forced liquidation record missing/unparseable executedQty — skipped"
        );
        return None;
    };
    let Some(time_exchange) = row
        .updated_time
        .and_then(|ms| Utc.timestamp_millis_opt(ms).single())
    else {
        warn!(
            order_id,
            symbol,
            "BinanceMargin forced liquidation record missing/unparseable updatedTime — skipped"
        );
        return None;
    };
    Some(MarginForcedLiquidation {
        order_id: OrderId(format_smolstr!("{order_id}")),
        instrument: InstrumentNameExchange::new(symbol),
        side,
        price: parse(row.price.as_deref()),
        quantity: parse(row.qty.as_deref()),
        avg_price: parse(row.avg_price.as_deref()).filter(|_| !filled_quantity.is_zero()),
        filled_quantity,
        time_exchange,
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::{super::tests::margin_client_at, *};
    use crate::error::ApiError;
    use rust_decimal_macros::dec;
    use serde_json::{Value, json};
    use std::collections::HashMap;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path, query_param},
    };

    const INTEREST_PATH: &str = "/sapi/v1/margin/interestHistory";
    const LIQUIDATION_PATH: &str = "/sapi/v1/margin/forceLiquidationRec";

    fn interest_row(tx_id: i64, time: DateTime<Utc>, isolated: Option<&str>) -> Value {
        let mut row = json!({
            "txId": tx_id, "interestAccuredTime": time.timestamp_millis(), "asset": "USDT",
            "rawAsset": "USDT", "principal": "1000", "interest": "0.0125",
            "interestRate": "0.0000125", "type": "PERIODIC",
        });
        if let Some(symbol) = isolated {
            row["isolatedSymbol"] = json!(symbol);
        }
        row
    }

    fn liquidation_row(order_id: i64, side: &str, filled: &str, time: DateTime<Utc>) -> Value {
        json!({
            "avgPrice": "60000", "executedQty": filled, "orderId": order_id, "price": "59000",
            "qty": "0.5", "side": side, "symbol": "BTCUSDT", "timeInForce": "GTC",
            "isIsolated": false, "updatedTime": time.timestamp_millis(),
        })
    }

    fn page(rows: Vec<Value>) -> ResponseTemplate {
        let total = rows.len();
        ResponseTemplate::new(200).set_body_json(json!({ "rows": rows, "total": total }))
    }

    async fn mount(server: &MockServer, at: &str, response: ResponseTemplate) {
        Mock::given(method("GET"))
            .and(path(at))
            .respond_with(response)
            .mount(server)
            .await;
    }

    /// The query of every request `server` received, in order.
    async fn queries(server: &MockServer) -> Vec<HashMap<String, String>> {
        server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .map(|request| request.url.query_pairs().into_owned().collect())
            .collect()
    }

    fn millis(query: &HashMap<String, String>, key: &str) -> i64 {
        query[key].parse().unwrap()
    }

    #[tokio::test]
    async fn interest_is_read_window_by_window_as_negative_flows_oldest_first_once_each() {
        let server = MockServer::start().await;
        let now = Utc::now();
        // Every window answers with the same rows, one of them outside the span.
        mount(
            &server,
            INTEREST_PATH,
            page(vec![
                interest_row(2, now - TimeDelta::days(1), None),
                interest_row(1, now - TimeDelta::days(35), None),
                interest_row(0, now - TimeDelta::days(50), None),
            ]),
        )
        .await;
        let client = margin_client_at(&server, false);
        let start = now - TimeDelta::days(40);

        let flows = client
            .fetch_margin_interest(start, now + TimeDelta::days(5))
            .await
            .unwrap();

        let ids: Vec<_> = flows.iter().map(|flow| flow.id.clone().unwrap()).collect();
        assert_eq!(ids, [CashFlowId::new("1"), CashFlowId::new("2")]);
        let flow = &flows[0];
        assert_eq!(flow.amount, dec!(-0.0125), "a charge is paid");
        assert_eq!(flow.asset, AssetNameExchange::new("USDT"));
        assert_eq!(flow.instrument, None, "a cross charge names no pair");
        assert_eq!(
            flow.kind,
            CashFlowKind::MarginInterest {
                rate: Some(dec!(0.0000125)),
                principal: Some(dec!(1000)),
            }
        );

        let queries = queries(&server).await;
        let [first, second] = queries.as_slice() else {
            panic!("two windows: {queries:?}");
        };
        assert_eq!(millis(first, "startTime"), start.timestamp_millis());
        assert_eq!(
            millis(first, "endTime") - millis(first, "startTime"),
            INTEREST_WINDOW_MS
        );
        assert_eq!(millis(second, "startTime"), millis(first, "endTime"));
        assert!(
            millis(second, "endTime") <= Utc::now().timestamp_millis(),
            "read up to the local clock, not `end`"
        );
        for query in [first, second] {
            assert_eq!(query["current"], "1");
            assert_eq!(query["size"], "100");
            assert!(!query.contains_key("isolatedSymbol"), "{query:?}");
        }
    }

    #[tokio::test]
    async fn a_full_interest_page_reads_the_next() {
        let server = MockServer::start().await;
        let now = Utc::now();
        let time = now - TimeDelta::hours(1);
        let full = (0..100).map(|id| interest_row(id, time, None)).collect();
        for (current, rows) in [("1", full), ("2", vec![interest_row(100, time, None)])] {
            Mock::given(method("GET"))
                .and(path(INTEREST_PATH))
                .and(query_param("current", current))
                .respond_with(page(rows))
                .mount(&server)
                .await;
        }
        let client = margin_client_at(&server, false);

        let flows = client
            .fetch_margin_interest(now - TimeDelta::days(1), now)
            .await
            .unwrap();

        assert_eq!(flows.len(), 101);
        assert_eq!(queries(&server).await.len(), 2);
    }

    #[tokio::test]
    async fn isolated_interest_is_read_per_pair_and_attributed_to_it() {
        let server = MockServer::start().await;
        let now = Utc::now();
        mount(
            &server,
            INTEREST_PATH,
            page(vec![
                interest_row(2, now - TimeDelta::hours(1), Some("BTCUSDT")),
                // Binance names the pair on an isolated row; one that does not keeps the pair read.
                interest_row(1, now - TimeDelta::hours(2), None),
            ]),
        )
        .await;
        let client = margin_client_at(&server, true);

        let flows = client
            .fetch_margin_interest(now - TimeDelta::days(1), now)
            .await
            .unwrap();

        assert_eq!(flows.len(), 2);
        for flow in flows {
            assert_eq!(
                flow.instrument,
                Some(InstrumentNameExchange::new("BTCUSDT"))
            );
        }
        let queries = queries(&server).await;
        assert_eq!(queries.len(), 1);
        assert_eq!(queries[0]["isolatedSymbol"], "BTCUSDT");
    }

    #[tokio::test]
    async fn interest_older_than_binance_serves_is_read_from_its_horizon() {
        let server = MockServer::start().await;
        mount(&server, INTEREST_PATH, page(Vec::new())).await;
        let client = margin_client_at(&server, false);
        let before = Utc::now();

        let flows = client
            .fetch_margin_interest(before - TimeDelta::days(200), before)
            .await
            .unwrap();

        assert!(flows.is_empty());
        let queries = queries(&server).await;
        let first = millis(&queries[0], "startTime");
        assert!(
            first >= (before - INTEREST_LOOKBACK).timestamp_millis(),
            "{first}"
        );
        assert!(
            first <= (Utc::now() - TimeDelta::days(89)).timestamp_millis(),
            "{first}"
        );
        assert_eq!(queries.len(), 4, "90 days in windows of 29");
    }

    #[tokio::test]
    async fn an_empty_span_reads_nothing() {
        let server = MockServer::start().await;
        let client = margin_client_at(&server, false);
        let now = Utc::now();

        let interest = client
            .fetch_margin_interest(now, now - TimeDelta::hours(1))
            .await
            .unwrap();
        let liquidations = client
            .fetch_forced_liquidations(now + TimeDelta::hours(1), now + TimeDelta::hours(2))
            .await
            .unwrap();

        assert!(interest.is_empty() && liquidations.is_empty());
        assert!(queries(&server).await.is_empty());
    }

    #[tokio::test]
    async fn a_refused_read_fails_the_whole_read() {
        let server = MockServer::start().await;
        let refused =
            ResponseTemplate::new(400).set_body_json(json!({"code": -1100, "msg": "refused"}));
        mount(&server, INTEREST_PATH, refused.clone()).await;
        mount(&server, LIQUIDATION_PATH, refused).await;
        let client = margin_client_at(&server, false);
        let now = Utc::now();

        let interest = client
            .fetch_margin_interest(now - TimeDelta::days(1), now)
            .await;
        let liquidations = client
            .fetch_forced_liquidations(now - TimeDelta::days(1), now)
            .await;

        assert!(
            matches!(
                interest,
                Err(UnindexedClientError::Api(ApiError::RequestRejected(_)))
            ),
            "{interest:?}"
        );
        assert!(
            matches!(
                liquidations,
                Err(UnindexedClientError::Api(ApiError::RequestRejected(_)))
            ),
            "{liquidations:?}"
        );
    }

    #[tokio::test]
    async fn forced_liquidations_are_read_oldest_first_once_each() {
        for is_isolated in [false, true] {
            let server = MockServer::start().await;
            let now = Utc::now();
            let mut no_order_id = liquidation_row(9, "SELL", "0.5", now);
            no_order_id.as_object_mut().unwrap().remove("orderId");
            mount(
                &server,
                LIQUIDATION_PATH,
                page(vec![
                    liquidation_row(2, "SELL", "0.5", now - TimeDelta::hours(1)),
                    liquidation_row(2, "SELL", "0.5", now - TimeDelta::hours(1)),
                    no_order_id,
                    liquidation_row(1, "BUY", "0", now - TimeDelta::hours(2)),
                    liquidation_row(0, "BUY", "0.5", now - TimeDelta::days(3)),
                ]),
            )
            .await;
            let client = margin_client_at(&server, is_isolated);

            let liquidations = client
                .fetch_forced_liquidations(now - TimeDelta::days(1), now)
                .await
                .unwrap();

            let [unfilled, filled] = liquidations.as_slice() else {
                panic!("two in the span: {liquidations:?}");
            };
            assert_eq!(unfilled.order_id, OrderId::new("1"));
            assert_eq!(unfilled.side, Side::Buy);
            assert_eq!(unfilled.avg_price, None, "nothing filled");
            assert_eq!(
                *filled,
                MarginForcedLiquidation {
                    order_id: OrderId::new("2"),
                    instrument: InstrumentNameExchange::new("BTCUSDT"),
                    side: Side::Sell,
                    price: Some(dec!(59000)),
                    quantity: Some(dec!(0.5)),
                    filled_quantity: dec!(0.5),
                    avg_price: Some(dec!(60000)),
                    time_exchange: Utc
                        .timestamp_millis_opt((now - TimeDelta::hours(1)).timestamp_millis())
                        .unwrap(),
                }
            );

            let queries = queries(&server).await;
            let [query] = queries.as_slice() else {
                panic!("one page: {queries:?}");
            };
            assert_eq!(query["size"], "100");
            assert_eq!(
                query.get("isolatedSymbol").map(String::as_str),
                is_isolated.then_some("BTCUSDT"),
            );
        }
    }
}
