//! A margin account's history read over REST: the interest Binance charged, and the orders it
//! placed to liquidate the account.
//!
//! Neither arrives on the account stream, so the caller reads them when it wants them.

use super::BinanceMargin;
use crate::{
    cash_flow::{CashFlow, CashFlowId, CashFlowKind, CashFlowsRead, InterestTiming},
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
use smol_str::{SmolStr, format_smolstr};
use std::{future::Future, str::FromStr};
use tracing::{debug, warn};

/// How far back Binance serves interest history.
const INTEREST_LOOKBACK: TimeDelta = TimeDelta::days(90);

/// Kept clear of [`INTEREST_LOOKBACK`]'s edge, so that a start at the edge is not refused for any
/// skew between the local clock and Binance's.
const INTEREST_LOOKBACK_MARGIN: TimeDelta = TimeDelta::minutes(1);

/// Where an interest read asked to start at `start` at `now` starts instead, because Binance no
/// longer serves charges that old: a minute inside [`INTEREST_LOOKBACK`]. `None` when it serves
/// `start`.
fn interest_held_since(start: DateTime<Utc>, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
    let held_since = now - INTEREST_LOOKBACK + INTEREST_LOOKBACK_MARGIN;
    (start < held_since).then_some(held_since)
}

/// The span of one interest history request. Binance serves at most 30 days per request, and
/// says the limit is needed for the data to be correct; a day less leaves room for how it counts.
const INTEREST_WINDOW_MS: i64 = 29 * 24 * 60 * 60 * 1000;

/// The most rows Binance returns in one page of either history.
const PAGE_SIZE: i64 = 100;

/// The most pages one query is read for: 50,000 rows. A query needing more fails rather than
/// loop on, as it would were Binance to ignore the page asked for and serve the same full page
/// each time.
const MAX_PAGES: i64 = 500;

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
    /// [`CashFlow`] of [`MarginInterest`](CashFlowKind::MarginInterest), oldest first, in
    /// [`CashFlowsRead::flows`].
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
    /// - [`asset`](CashFlow::asset) is the `asset` field of Binance's record. Binance charges
    ///   interest in the asset borrowed, or converts it into BNB for an account that pays interest
    ///   in BNB: a `_CONVERTED` record's `asset` is BNB, so its flow does not name the asset
    ///   borrowed. Binance names that asset in a separate `rawAsset` field, which this method does
    ///   not read, and which Binance does not return on an isolated record.
    /// - [`principal`](CashFlowKind::MarginInterest::principal) is the loan the charge was on. On a
    ///   `_CONVERTED` record it can read 0, and Binance does not document when it does. Binance
    ///   points to its borrow and repay history (`GET /sapi/v1/margin/borrow-repay`) for the loan.
    ///   Looking it up there needs the asset borrowed, which a converted record's flow does not
    ///   carry.
    /// - [`rate`](CashFlowKind::MarginInterest::rate) is Binance's `interestRate`, a **daily** rate
    ///   even on an hourly charge: an hourly charge is about the rate divided by 24 of the
    ///   principal. Binance's API documentation does not state the unit; it is read off the rates
    ///   Binance publishes on its [margin fee page](https://www.binance.com/en/fee/marginFee),
    ///   whose hourly figure is the daily one divided by 24 (BTC's daily rate matches the field,
    ///   its hourly rate is about 24 times smaller).
    /// - [`timing`](CashFlowKind::MarginInterest::timing) is read from Binance's `type`:
    ///   `PERIODIC` (and `PERIODIC_CONVERTED`, and portfolio margin's daily `PORTFOLIO`) is
    ///   [`Periodic`](InterestTiming::Periodic), `ON_BORROW` (and `ON_BORROW_CONVERTED`) is
    ///   [`OnBorrow`](InterestTiming::OnBorrow), and any other value is
    ///   [`Other`](InterestTiming::Other) with Binance's own name. `None` when the record has no
    ///   `type`.
    /// - [`time_exchange`](CashFlow::time_exchange) is Binance's `interestAccuredTime`, the period
    ///   the charge is for, not when Binance posted it (see [Delivery](#delivery)): for an hourly
    ///   charge, the start of its hour. An `ON_BORROW` charge is stamped at the start of the hour
    ///   the loan was taken in, so it can precede the opening of the position it belongs to.
    /// - [`id`](CashFlow::id) is Binance's transaction id for the charge.
    ///
    /// The engine adds a flow attributed to an instrument to the carry of its open position, in
    /// the slot for the flow's asset: the quote asset's carry, the base asset's, or any other
    /// asset's. Interest on a short's borrowed base asset is valued at the price the position
    /// closes at, which is what buying the asset back costs. Interest in BNB, or any asset that is
    /// neither the instrument's base nor quote, is kept as charged and is in no PnL figure.
    ///
    /// # Delivery
    /// Nothing is pushed: the caller decides when to read, and must drop the charges it has
    /// already counted, by [`id`](CashFlow::id), before applying a read's flows. Within one read
    /// each charge appears once: a charge read twice, as one stamped on a window's boundary is, is
    /// kept the first time its id is seen in its asset and pair, or, without an id, once per
    /// identical record.
    ///
    /// A charge is posted some time after the period it is for, by a delay Binance does not
    /// document and that may vary, and is then served stamped at that period. A read whose span
    /// ends near the present can therefore miss a charge that a later read returns at a time
    /// inside the earlier span. A caller that polls by time should start each read before the
    /// previous read's `end`, by at least the longest posting delay it is prepared to allow for,
    /// and drop the charges it has already counted by id within their asset and pair. A charge
    /// with no id can be matched only by its whole record, which cannot tell it from an identical
    /// charge.
    ///
    /// The records need not add up to the interest owed. On a live account, a loan was charged
    /// interest when it was taken, yet a read served only the hourly charge that followed, with no
    /// `ON_BORROW` record. Whether such a record is posted later, or never, is not known. The
    /// account's balance, which reports the interest owed (see
    /// [`MarginDetails::interest`](crate::balance::MarginDetails::interest)), is the figure to rely
    /// on for what is owed.
    ///
    /// # Span
    /// - Binance serves interest charged in the last 90 days only. A `start` before that is moved
    ///   up to a minute inside that limit, clear of any skew between the clocks, and
    ///   [`held_since`](CashFlowsRead::held_since) is where the read then starts: charges before
    ///   it may exist but were not read. A caller adding up the interest on a position opened
    ///   before it has only part of it. `held_since` is `None` when `start` is at or after that
    ///   point, a minute inside the 90 days; a request for the last 90 days reports it.
    /// - Charges are read up to `end`, or up to the local clock at the call if that is earlier.
    /// - The whole span is read in one call, so [`resume`](CashFlowsRead::resume) is always
    ///   `None`.
    /// - Binance serves at most 30 days per request, so a span is read in windows of 29 days,
    ///   each paged by 100 charges. A read costs a request weight of 1 per page, at least 1 per
    ///   window, and, on an isolated client, as much again for each pair.
    ///
    /// # Records left out
    /// A record without an asset, an amount or a time that parses is left out, and logged at WARN.
    /// A rate or principal that does not parse is left `None`, and logged at WARN.
    ///
    /// # Errors
    /// Fails if any request fails, or if one window of one account holds more than 50,000
    /// charges. Nothing is returned from a read that failed part way.
    pub async fn fetch_margin_interest(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<CashFlowsRead<AssetNameExchange, InstrumentNameExchange>, UnindexedClientError>
    {
        let now = Utc::now();
        let held_since = interest_held_since(start, now);
        if let Some(held_since) = held_since {
            debug!(
                requested = %start,
                %held_since,
                "BinanceMargin fetch_margin_interest: Binance serves 90 days of interest \
                 history — reading from the earliest time it serves"
            );
        }
        let start = held_since.unwrap_or(start);
        let end = end.min(now);
        if start > end {
            return Ok(CashFlowsRead::complete(Vec::new()).with_held_since(held_since));
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
            .filter(|flow| seen.insert(ChargeKey::of(flow)))
            .collect();
        flows.sort_by_key(|flow| flow.time_exchange);
        Ok(CashFlowsRead::complete(flows).with_held_since(held_since))
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
    /// pages of 100. Binance documents no limit on the span or on how far back it serves; a
    /// two-year span was accepted when tried in October 2026, which is no guarantee. A read
    /// costs a request weight of 1 per page and, on an isolated client, as much again for each
    /// pair.
    ///
    /// # Records left out
    /// - A record without an order id, a pair, a side, a filled quantity or a time that parses is
    ///   left out, and logged at WARN. A price or quantity that does not parse is left `None`, and
    ///   logged at WARN.
    /// - On a cross client, a record Binance marks as isolated is left out, and logged at WARN:
    ///   the cross query should not return one. An isolated client asks for one pair at a time,
    ///   and keeps what Binance returns for it.
    ///
    /// # Errors
    /// Fails if any request fails, or if one account holds more than 50,000 records in the span.
    /// Nothing is returned from a read that failed part way.
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

        let is_isolated = self.config.is_isolated;
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
            .filter_map(|row| convert_forced_liquidation(row, is_isolated))
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
/// short page. Fails once [`MAX_PAGES`] pages were full.
async fn read_all_pages<Row, Fut>(
    mut page: impl FnMut(i64) -> Fut,
) -> Result<Vec<Row>, UnindexedClientError>
where
    Fut: Future<Output = Result<Vec<Row>, UnindexedClientError>>,
{
    let mut rows = Vec::new();
    for current in 1..=MAX_PAGES {
        let batch = page(current).await?;
        let full = i64::try_from(batch.len()).is_ok_and(|len| len >= PAGE_SIZE);
        rows.extend(batch);
        if !full {
            return Ok(rows);
        }
    }
    Err(UnindexedClientError::Internal(format!(
        "Binance margin history query still full after {MAX_PAGES} pages of {PAGE_SIZE}: \
         narrow the span"
    )))
}

/// What tells two interest charges in one read apart: Binance's transaction id, in the charge's
/// asset and pair, or, for a record without one, the whole flow.
#[derive(PartialEq, Eq, Hash)]
enum ChargeKey {
    Id(
        AssetNameExchange,
        Option<InstrumentNameExchange>,
        CashFlowId,
    ),
    Flow(CashFlow<AssetNameExchange, InstrumentNameExchange>),
}

impl ChargeKey {
    fn of(flow: &CashFlow<AssetNameExchange, InstrumentNameExchange>) -> Self {
        match &flow.id {
            Some(id) => Self::Id(flow.asset.clone(), flow.instrument.clone(), id.clone()),
            None => Self::Flow(flow.clone()),
        }
    }
}

/// An optional decimal field of a `record`: `None` when absent or empty, or, logged at WARN, when
/// it does not parse.
fn optional_decimal(record: &str, field: &str, value: Option<&str>) -> Option<Decimal> {
    let value = value.filter(|value| !value.is_empty())?;
    Decimal::from_str(value)
        .inspect_err(|error| {
            warn!(
                record,
                field,
                value,
                %error,
                "BinanceMargin record field does not parse — left out"
            );
        })
        .ok()
}

/// The [`InterestTiming`] of an `interestHistory` row's `type`. A `_CONVERTED` type is the same
/// charge settled in BNB, and `PORTFOLIO` is portfolio margin's daily charge on a negative balance.
fn interest_timing(r#type: &str) -> InterestTiming {
    match r#type {
        "PERIODIC" | "PERIODIC_CONVERTED" | "PORTFOLIO" => InterestTiming::Periodic,
        "ON_BORROW" | "ON_BORROW_CONVERTED" => InterestTiming::OnBorrow,
        other => InterestTiming::Other(SmolStr::new(other)),
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
    let instrument = row
        .isolated_symbol
        .as_deref()
        .map(InstrumentNameExchange::new)
        .or_else(|| pair.cloned());
    Some(CashFlow::new(
        CashFlowKind::MarginInterest {
            rate: optional_decimal("interest", "interestRate", row.interest_rate.as_deref()),
            principal: optional_decimal("interest", "principal", row.principal.as_deref()),
            timing: row.r#type.as_deref().map(interest_timing),
        },
        AssetNameExchange::new(asset),
        -interest,
        instrument,
        time_exchange,
        tx_id.map(|id| CashFlowId(format_smolstr!("{id}"))),
    ))
}

/// One `forceLiquidationRec` row as a [`MarginForcedLiquidation`], or `None`, with a warning, if
/// it lacks what one needs. `None` too, logged at WARN, on a cross client (`is_isolated` false)
/// when Binance marks the row as isolated.
fn convert_forced_liquidation(
    row: GetForceLiquidationRecordResponseRowsInner,
    is_isolated: bool,
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
    // An isolated client's query names its pair, so what it returns is that pair's. A cross
    // query names none, so an isolated pair's record there is not the cross account's.
    if !is_isolated && row.is_isolated == Some(true) {
        warn!(
            order_id,
            symbol,
            "BinanceMargin cross forced liquidation read returned an isolated record — left out"
        );
        return None;
    }
    let Some(side) = row.side.as_deref().and_then(parse_side) else {
        warn!(
            order_id,
            symbol, "BinanceMargin forced liquidation record missing/unknown side — skipped"
        );
        return None;
    };
    let Some(filled_quantity) = row
        .executed_qty
        .as_deref()
        .and_then(|s| Decimal::from_str(s).ok())
    else {
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
        price: optional_decimal("forced liquidation", "price", row.price.as_deref()),
        quantity: optional_decimal("forced liquidation", "qty", row.qty.as_deref()),
        avg_price: (!filled_quantity.is_zero())
            .then(|| optional_decimal("forced liquidation", "avgPrice", row.avg_price.as_deref()))
            .flatten(),
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

    fn liquidation_row(
        order_id: i64,
        side: &str,
        filled: &str,
        time: DateTime<Utc>,
        is_isolated: bool,
    ) -> Value {
        json!({
            "avgPrice": "60000", "executedQty": filled, "orderId": order_id, "price": "59000",
            "qty": "0.5", "side": side, "symbol": "BTCUSDT", "timeInForce": "GTC",
            "isIsolated": is_isolated, "updatedTime": time.timestamp_millis(),
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

    /// Answer page `current` of the query at `at` with `response`.
    async fn mount_page(server: &MockServer, at: &str, current: &str, response: ResponseTemplate) {
        Mock::given(method("GET"))
            .and(path(at))
            .and(query_param("current", current))
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
        let start = now - TimeDelta::days(40);
        let boundary = start + TimeDelta::milliseconds(INTEREST_WINDOW_MS);
        let mut no_interest = interest_row(9, now - TimeDelta::days(2), None);
        no_interest.as_object_mut().unwrap().remove("interest");
        let mut bad_rate = interest_row(3, now - TimeDelta::days(3), None);
        bad_rate["interestRate"] = json!("n/a");
        // Every window answers with the same rows: one stamped on the windows' boundary, one
        // outside the span, and one that cannot be a flow.
        mount(
            &server,
            INTEREST_PATH,
            page(vec![
                interest_row(2, now - TimeDelta::days(1), None),
                no_interest,
                bad_rate,
                interest_row(4, boundary, None),
                interest_row(1, now - TimeDelta::days(35), None),
                interest_row(0, now - TimeDelta::days(50), None),
            ]),
        )
        .await;
        let client = margin_client_at(&server, false);

        let read = client
            .fetch_margin_interest(start, now + TimeDelta::days(5))
            .await
            .unwrap();
        assert_eq!((read.resume, read.held_since), (None, None));
        let flows = read.flows;

        let ids: Vec<_> = flows.iter().map(|flow| flow.id.clone().unwrap()).collect();
        assert_eq!(ids, ["1", "4", "3", "2"].map(CashFlowId::new));
        let flow = &flows[0];
        assert_eq!(flow.amount, dec!(-0.0125), "a charge is paid");
        assert_eq!(flow.asset, AssetNameExchange::new("USDT"));
        assert_eq!(flow.instrument, None, "a cross charge names no pair");
        assert_eq!(
            flow.kind,
            CashFlowKind::MarginInterest {
                rate: Some(dec!(0.0000125)),
                principal: Some(dec!(1000)),
                timing: Some(InterestTiming::Periodic),
            }
        );
        assert_eq!(
            flows[2].kind,
            CashFlowKind::MarginInterest {
                rate: None,
                principal: Some(dec!(1000)),
                timing: Some(InterestTiming::Periodic),
            },
            "a rate that does not parse is left out, not the charge"
        );

        let queries = queries(&server).await;
        let [first, second] = queries.as_slice() else {
            panic!("two windows: {queries:?}");
        };
        assert_eq!(millis(first, "startTime"), start.timestamp_millis());
        assert_eq!(millis(first, "endTime"), boundary.timestamp_millis());
        assert_eq!(millis(second, "startTime"), boundary.timestamp_millis());
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

    #[test]
    fn interest_timing_is_read_from_the_record_type() {
        for (r#type, expected) in [
            ("PERIODIC", InterestTiming::Periodic),
            ("PERIODIC_CONVERTED", InterestTiming::Periodic),
            ("PORTFOLIO", InterestTiming::Periodic),
            ("ON_BORROW", InterestTiming::OnBorrow),
            ("ON_BORROW_CONVERTED", InterestTiming::OnBorrow),
            (
                "ON_LIQUIDATION",
                InterestTiming::Other("ON_LIQUIDATION".into()),
            ),
        ] {
            assert_eq!(interest_timing(r#type), expected, "{type}");
        }
    }

    #[tokio::test]
    async fn a_charge_on_borrow_has_its_timing() {
        let server = MockServer::start().await;
        let now = Utc::now();
        let mut row = interest_row(1, now - TimeDelta::hours(1), None);
        row["type"] = json!("ON_BORROW");
        mount(&server, INTEREST_PATH, page(vec![row])).await;
        let client = margin_client_at(&server, false);

        let flows = client
            .fetch_margin_interest(now - TimeDelta::days(1), now)
            .await
            .unwrap()
            .flows;

        let [flow] = flows.as_slice() else {
            panic!("one flow: {flows:?}");
        };
        let CashFlowKind::MarginInterest { timing, .. } = &flow.kind else {
            panic!("margin interest: {flow:?}");
        };
        assert_eq!(*timing, Some(InterestTiming::OnBorrow));
    }

    /// A charge converted into BNB is in BNB, from `asset` rather than the `rawAsset` borrowed,
    /// and its principal of 0 is read as served.
    #[tokio::test]
    async fn a_charge_converted_into_bnb_is_in_bnb() {
        let server = MockServer::start().await;
        let now = Utc::now();
        let mut row = interest_row(1, now - TimeDelta::hours(1), None);
        row["asset"] = json!("BNB");
        row["principal"] = json!("0");
        row["interest"] = json!("0.00002");
        row["type"] = json!("PERIODIC_CONVERTED");
        mount(&server, INTEREST_PATH, page(vec![row])).await;
        let client = margin_client_at(&server, false);

        let flows = client
            .fetch_margin_interest(now - TimeDelta::days(1), now)
            .await
            .unwrap()
            .flows;

        let [flow] = flows.as_slice() else {
            panic!("one flow: {flows:?}");
        };
        assert_eq!(flow.asset, AssetNameExchange::new("BNB"));
        assert_eq!(flow.amount, dec!(-0.00002));
        let CashFlowKind::MarginInterest {
            principal, timing, ..
        } = &flow.kind
        else {
            panic!("margin interest: {flow:?}");
        };
        assert_eq!(*principal, Some(Decimal::ZERO));
        assert_eq!(*timing, Some(InterestTiming::Periodic));
    }

    #[tokio::test]
    async fn a_charge_without_a_type_has_no_timing() {
        let server = MockServer::start().await;
        let now = Utc::now();
        let mut row = interest_row(1, now - TimeDelta::hours(1), None);
        row.as_object_mut().unwrap().remove("type");
        mount(&server, INTEREST_PATH, page(vec![row])).await;
        let client = margin_client_at(&server, false);

        let flows = client
            .fetch_margin_interest(now - TimeDelta::days(1), now)
            .await
            .unwrap()
            .flows;

        let [flow] = flows.as_slice() else {
            panic!("one flow: {flows:?}");
        };
        let CashFlowKind::MarginInterest { timing, .. } = &flow.kind else {
            panic!("margin interest: {flow:?}");
        };
        assert_eq!(*timing, None);
    }

    #[tokio::test]
    async fn a_charge_repeated_with_its_id_is_kept_once() {
        let server = MockServer::start().await;
        let now = Utc::now();
        let mut corrected = interest_row(1, now - TimeDelta::hours(1), None);
        corrected["interest"] = json!("0.02");
        mount(
            &server,
            INTEREST_PATH,
            page(vec![
                corrected,
                interest_row(1, now - TimeDelta::hours(1), None),
            ]),
        )
        .await;
        let client = margin_client_at(&server, false);

        let flows = client
            .fetch_margin_interest(now - TimeDelta::days(1), now)
            .await
            .unwrap()
            .flows;

        let [flow] = flows.as_slice() else {
            panic!("one charge: {flows:?}");
        };
        assert_eq!(flow.amount, dec!(-0.02), "the first copy read is kept");
    }

    #[tokio::test]
    async fn a_span_of_one_instant_is_read_in_one_request() {
        let server = MockServer::start().await;
        // In whole milliseconds, as Binance stamps a charge, so the charge is inside the span.
        let at =
            DateTime::from_timestamp_millis((Utc::now() - TimeDelta::hours(1)).timestamp_millis())
                .unwrap();
        mount(
            &server,
            INTEREST_PATH,
            page(vec![interest_row(1, at, None)]),
        )
        .await;
        let client = margin_client_at(&server, false);

        let flows = client.fetch_margin_interest(at, at).await.unwrap().flows;

        assert_eq!(flows.len(), 1);
        let queries = queries(&server).await;
        let [query] = queries.as_slice() else {
            panic!("one request: {queries:?}");
        };
        assert_eq!(millis(query, "startTime"), at.timestamp_millis());
        assert_eq!(millis(query, "endTime"), at.timestamp_millis());
    }

    #[tokio::test]
    async fn a_full_page_reads_the_next() {
        let server = MockServer::start().await;
        let now = Utc::now();
        let time = now - TimeDelta::hours(1);
        let full = (0..100).map(|id| interest_row(id, time, None)).collect();
        mount_page(&server, INTEREST_PATH, "1", page(full)).await;
        mount_page(
            &server,
            INTEREST_PATH,
            "2",
            page(vec![interest_row(100, time, None)]),
        )
        .await;
        let client = margin_client_at(&server, false);

        let flows = client
            .fetch_margin_interest(now - TimeDelta::days(1), now)
            .await
            .unwrap()
            .flows;

        assert_eq!(flows.len(), 101);
        assert_eq!(queries(&server).await.len(), 2);
    }

    #[tokio::test]
    async fn a_query_still_full_after_the_most_pages_fails() {
        let server = MockServer::start().await;
        let now = Utc::now();
        let time = now - TimeDelta::hours(1);
        // As were Binance to ignore the page asked for.
        let full = (0..100).map(|id| interest_row(id, time, None)).collect();
        mount(&server, INTEREST_PATH, page(full)).await;
        let client = margin_client_at(&server, false);

        let result = client
            .fetch_margin_interest(now - TimeDelta::days(1), now)
            .await;

        assert!(
            matches!(result, Err(UnindexedClientError::Internal(_))),
            "{result:?}"
        );
        assert_eq!(
            i64::try_from(queries(&server).await.len()).unwrap(),
            MAX_PAGES
        );
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
            .unwrap()
            .flows;

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

        let read = client
            .fetch_margin_interest(before - TimeDelta::days(200), before)
            .await
            .unwrap();
        let after = Utc::now();

        assert!(read.flows.is_empty());
        let queries = queries(&server).await;
        let first = millis(&queries[0], "startTime");
        let horizon = |now: DateTime<Utc>| {
            (now - INTEREST_LOOKBACK + INTEREST_LOOKBACK_MARGIN).timestamp_millis()
        };
        assert!(
            (horizon(before)..=horizon(after)).contains(&first),
            "{first}"
        );
        assert_eq!(
            read.held_since
                .map(|held_since| held_since.timestamp_millis()),
            Some(first),
            "held_since is where the read started"
        );
        assert_eq!(queries.len(), 4, "90 days in windows of 29");
    }

    #[test]
    fn interest_is_held_since_a_minute_inside_ninety_days_only_for_an_older_start() {
        let now = Utc.with_ymd_and_hms(2026, 10, 25, 12, 0, 0).unwrap();
        let held_since = now - TimeDelta::days(90) + TimeDelta::minutes(1);
        let before = held_since - TimeDelta::milliseconds(1);

        assert_eq!(interest_held_since(before, now), Some(held_since));
        assert_eq!(
            interest_held_since(now - TimeDelta::days(90), now),
            Some(held_since),
            "a request for the last 90 days misses its first minute"
        );
        assert_eq!(interest_held_since(held_since, now), None);
        assert_eq!(interest_held_since(now, now), None);
    }

    #[tokio::test]
    async fn interest_entirely_older_than_binance_serves_reads_nothing_and_says_so() {
        let server = MockServer::start().await;
        let client = margin_client_at(&server, false);
        let end = Utc::now() - TimeDelta::days(100);

        let read = client
            .fetch_margin_interest(end - TimeDelta::days(1), end)
            .await
            .unwrap();

        assert!(read.flows.is_empty());
        assert!(
            read.held_since.is_some_and(|held_since| held_since > end),
            "{:?}",
            read.held_since
        );
        assert!(queries(&server).await.is_empty());
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

        assert!(interest.flows.is_empty() && liquidations.is_empty());
        assert_eq!((interest.resume, interest.held_since), (None, None));
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
    async fn forced_liquidations_of_the_clients_account_are_read_oldest_first_once_each() {
        for is_isolated in [false, true] {
            let server = MockServer::start().await;
            let now = Utc::now();
            let row = |order_id, side, filled, time| {
                liquidation_row(order_id, side, filled, time, is_isolated)
            };
            let mut no_order_id = row(9, "SELL", "0.5", now);
            no_order_id.as_object_mut().unwrap().remove("orderId");
            mount(
                &server,
                LIQUIDATION_PATH,
                page(vec![
                    row(2, "SELL", "0.5", now - TimeDelta::hours(1)),
                    row(2, "SELL", "0.5", now - TimeDelta::hours(1)),
                    no_order_id,
                    // Marked isolated: not the cross account's, but the isolated pair's.
                    liquidation_row(8, "SELL", "0.5", now, true),
                    row(1, "BUY", "0", now - TimeDelta::hours(2)),
                    row(0, "BUY", "0.5", now - TimeDelta::days(3)),
                ]),
            )
            .await;
            let client = margin_client_at(&server, is_isolated);

            let liquidations = client
                .fetch_forced_liquidations(now - TimeDelta::days(1), now)
                .await
                .unwrap();

            let (unfilled, filled) = match (is_isolated, liquidations.as_slice()) {
                (false, [unfilled, filled]) => (unfilled, filled),
                (true, [unfilled, filled, marked]) => {
                    assert_eq!(marked.order_id, OrderId::new("8"));
                    (unfilled, filled)
                }
                _ => panic!("is_isolated = {is_isolated}: {liquidations:?}"),
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

    #[tokio::test]
    async fn a_full_page_of_forced_liquidations_reads_the_next() {
        let server = MockServer::start().await;
        let now = Utc::now();
        let time = now - TimeDelta::hours(1);
        let full = (0..100)
            .map(|id| liquidation_row(id, "SELL", "0.5", time, false))
            .collect();
        mount_page(&server, LIQUIDATION_PATH, "1", page(full)).await;
        mount_page(
            &server,
            LIQUIDATION_PATH,
            "2",
            page(vec![liquidation_row(100, "SELL", "0.5", time, false)]),
        )
        .await;
        let client = margin_client_at(&server, false);

        let liquidations = client
            .fetch_forced_liquidations(now - TimeDelta::days(1), now)
            .await
            .unwrap();

        assert_eq!(liquidations.len(), 101);
        assert_eq!(queries(&server).await.len(), 2);
    }
}
