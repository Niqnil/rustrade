//! Live streaming connector for Databento.
//!
//! Provides real-time market data streaming via [`DatabentoLive`].
//!
//! # Architecture
//!
//! - **One connection per dataset**: Each `DatabentoLive` connects to one dataset (e.g., GLBX.MDP3).
//!   For multiple datasets, create multiple instances.
//! - **Multiple symbols per connection**: Databento recommends consolidating subscriptions within
//!   a dataset. Subscribe to multiple symbols before calling `start()`.
//! - **Symbol resolution**: DBN records contain numeric `instrument_id`. We use `PitSymbolMap` to
//!   resolve these to symbol strings, then map to user-provided instrument keys.
//!
//! # Example
//!
//! ```ignore
//! use rustrade_data::exchange::databento::DatabentoLive;
//! use rustrade_instrument::exchange::ExchangeId;
//! use databento::dbn::Schema;
//! use std::collections::HashMap;
//! use futures::StreamExt;
//!
//! // Map symbols to your instrument keys
//! let instruments: HashMap<String, String> = [
//!     ("ESM5".to_string(), "ES-front".to_string()),
//!     ("NQM5".to_string(), "NQ-front".to_string()),
//! ].into_iter().collect();
//!
//! let mut client = DatabentoLive::from_env(
//!     "GLBX.MDP3",
//!     ExchangeId::DatabentoGlbx,
//!     instruments,
//! ).await?;
//!
//! // Subscribe to symbols (can call multiple times)
//! client.subscribe(&["ESM5", "NQM5"], Schema::Trades).await?;
//!
//! // Start streaming (consumes client)
//! let mut stream = client.start().await?;
//!
//! while let Some(event) = stream.next().await {
//!     match event {
//!         Ok(market_event) => println!("{:?}", market_event),
//!         Err(e) => eprintln!("Error: {}", e),
//!     }
//! }
//! ```

use super::error::{DatabentoErrorKind, DatabentoResultExt};
use super::transformer::{
    dbn_mbp1_to_orderbook_l1, dbn_ohlcv_to_candle, dbn_trade_to_public_trade,
    ensure_databento_ohlcv_supports, rtype_to_candle_interval,
};
use crate::error::DataError;
use crate::event::{DataKind, MarketEvent};
use crate::subscription::candle::CandleInterval;
use chrono::Utc;
use databento::LiveClient;
use databento::dbn::{Mbp1Msg, OhlcvMsg, PitSymbolMap, RecordRef, Schema, TradeMsg};
use databento::live::Subscription;
use futures::Stream;
use rustrade_instrument::exchange::ExchangeId;
use std::collections::HashMap;
use tracing::{debug, trace, warn};

/// Live streaming client for Databento.
///
/// Wraps [`LiveClient`] for real-time market data streaming. Each instance connects
/// to a single dataset but can subscribe to multiple symbols within that dataset.
///
/// # Connection Limits
///
/// - Standard tier: 10 concurrent connections per dataset
/// - Enterprise tier: 50 concurrent connections per dataset
///
/// Consolidate symbol subscriptions within one `DatabentoLive` instance per dataset
/// rather than creating multiple instances for the same dataset.
///
/// # Performance
///
/// The instrument key is cloned for each record. For high-frequency data, use
/// [`Arc<K>`](std::sync::Arc) to avoid per-record heap allocations:
///
/// ```ignore
/// use std::sync::Arc;
///
/// let instruments: HashMap<String, Arc<String>> = [
///     ("ESM5".to_string(), Arc::new("ES-front".to_string())),
/// ].into_iter().collect();
/// ```
#[derive(Debug)]
pub struct DatabentoLive<K> {
    client: LiveClient,
    instruments: HashMap<String, K>,
    exchange: ExchangeId,
}

impl<K> DatabentoLive<K> {
    /// Create a new live client using API key from environment.
    ///
    /// Reads `DATABENTO_API_KEY` from environment variables.
    ///
    /// # Arguments
    ///
    /// * `dataset` - Databento dataset identifier (e.g., "GLBX.MDP3", "XNAS.ITCH")
    /// * `exchange` - ExchangeId to tag events with (should match dataset)
    /// * `instruments` - Map from Databento symbol strings to user's instrument keys
    ///
    /// # Errors
    ///
    /// Returns error if `DATABENTO_API_KEY` is not set or client construction fails.
    pub async fn from_env(
        dataset: &str,
        exchange: ExchangeId,
        instruments: HashMap<String, K>,
    ) -> Result<Self, DataError> {
        debug!(dataset, "Creating Databento live client from env");

        let client = LiveClient::builder()
            .key_from_env()
            .with_context("reading API key from env")?
            .dataset(dataset)
            .build()
            .await
            .with_context("building live client")?;

        Ok(Self {
            client,
            instruments,
            exchange,
        })
    }

    /// Create a new live client with an explicit API key.
    ///
    /// # Errors
    ///
    /// Returns error if client construction fails.
    pub async fn new(
        api_key: &str,
        dataset: &str,
        exchange: ExchangeId,
        instruments: HashMap<String, K>,
    ) -> Result<Self, DataError> {
        debug!(dataset, "Creating Databento live client");

        let client = LiveClient::builder()
            .key(api_key)
            .with_context("setting API key")?
            .dataset(dataset)
            .build()
            .await
            .with_context("building live client")?;

        Ok(Self {
            client,
            instruments,
            exchange,
        })
    }

    /// Subscribe to symbols with a specific schema.
    ///
    /// Can be called multiple times before `start()` to add more subscriptions.
    /// All subscriptions are multiplexed over the same connection.
    ///
    /// # Arguments
    ///
    /// * `symbols` - Symbol identifiers (e.g., `["ESM5", "NQM5"]`)
    /// * `schema` - Data schema to subscribe to (e.g., `Schema::Trades`, `Schema::Mbp1`)
    ///
    /// # OHLCV note
    ///
    /// This is the low-level escape hatch: it subscribes to any `Schema` as-is and
    /// performs **no** interval validation. Subscribing to an OHLCV schema here
    /// (e.g. `Schema::Ohlcv1H`/`Schema::Ohlcv1D`) bypasses the `Sec1`/`Min1`
    /// live-only check in [`subscribe_candles`](Self::subscribe_candles), and any
    /// resulting bars whose `rtype` maps to a [`CandleInterval`] will be emitted as
    /// `DataKind::Candle`. Prefer [`subscribe_candles`](Self::subscribe_candles) for
    /// candles unless you deliberately want that lower-level behaviour.
    ///
    /// # Errors
    ///
    /// Returns error if subscription request fails.
    pub async fn subscribe(&mut self, symbols: &[&str], schema: Schema) -> Result<(), DataError> {
        debug!(?symbols, ?schema, "Subscribing to Databento live feed");

        let subscription = Subscription::builder()
            .symbols(symbols)
            .schema(schema)
            .build();

        self.client
            .subscribe(subscription)
            .await
            .with_context("subscribing to live feed")?;

        Ok(())
    }

    /// Subscribe to OHLCV candles for `symbols` at the given interval.
    ///
    /// Validates the interval and maps it to the Databento OHLCV schema before
    /// subscribing. The resulting stream emits [`DataKind::Candle`] events whose
    /// interval is derived per-record from each bar's `rtype`, so a single
    /// connection may safely carry multiple OHLCV intervals.
    ///
    /// # Live interval scope
    ///
    /// Only `Sec1`/`Min1` are accepted live. Databento's live gateway does not
    /// reliably stream the larger `Hour1`/`Day1` bars, so those are
    /// **historical-only** here — use [`DatabentoHistorical::fetch_candles`].
    /// They are rejected at subscribe time (observable failure) rather than
    /// yielding a silently empty stream.
    ///
    /// [`DatabentoHistorical::fetch_candles`]: super::historical::DatabentoHistorical::fetch_candles
    ///
    /// # Errors
    ///
    /// Returns [`DataError::UnsupportedInterval`] for any interval other than
    /// `Sec1`/`Min1`, or [`DataError::Databento`] if the subscription request fails.
    pub async fn subscribe_candles(
        &mut self,
        symbols: &[&str],
        interval: CandleInterval,
    ) -> Result<(), DataError> {
        let schema = ensure_databento_live_ohlcv_supports(self.exchange, interval)?;
        self.subscribe(symbols, schema).await
    }

    /// Returns a reference to the underlying client for advanced use cases.
    pub fn client(&self) -> &LiveClient {
        &self.client
    }

    /// Returns a mutable reference to the underlying client.
    pub fn client_mut(&mut self) -> &mut LiveClient {
        &mut self.client
    }
}

impl<K: Clone + Send + 'static> DatabentoLive<K> {
    /// Start streaming and return an owned stream of market events.
    ///
    /// Consumes `self` because `LiveClient` requires mutable access for polling.
    /// The stream will emit events until the connection closes or an error occurs.
    ///
    /// # Returns
    ///
    /// A stream of `MarketEvent<K, DataKind>` where `DataKind` is one of `Trade`,
    /// `OrderBookL1`, or `Candle` depending on the subscribed schema(s).
    ///
    /// Records for symbols not in the `instruments` map are skipped with a warning.
    ///
    /// # Errors
    ///
    /// Returns error if starting the stream fails. Stream items may also contain
    /// errors for individual record processing failures.
    pub async fn start(
        mut self,
    ) -> Result<impl Stream<Item = Result<MarketEvent<K, DataKind>, DataError>>, DataError> {
        debug!("Starting Databento live stream");

        self.client
            .start()
            .await
            .with_context("starting live stream")?;

        let stream = futures::stream::unfold(
            StreamState {
                client: self.client,
                symbol_map: PitSymbolMap::new(),
                instruments: self.instruments,
                exchange: self.exchange,
            },
            |mut state| async move {
                loop {
                    let record = match state.client.next_record().await {
                        Ok(Some(rec)) => rec,
                        Ok(None) => {
                            debug!("Databento live stream ended");
                            return None;
                        }
                        Err(e) => {
                            let err = DataError::Databento {
                                kind: DatabentoErrorKind::Network,
                                context: "receiving record".to_string(),
                                message: e.to_string(),
                            };
                            return Some((Err(err), state));
                        }
                    };

                    // Update symbol map with every record (InstrumentDefMsg populates mappings)
                    if let Err(e) = state.symbol_map.on_record(record) {
                        warn!(error = %e, "Failed to update symbol map");
                    }

                    // Try to convert to market event
                    if let Some(result) = process_record(
                        record,
                        &state.symbol_map,
                        &state.instruments,
                        state.exchange,
                    ) {
                        return Some((result, state));
                    }

                    // Record type not handled or symbol not in instruments map, continue polling
                }
            },
        );

        Ok(stream)
    }
}

struct StreamState<K> {
    client: LiveClient,
    symbol_map: PitSymbolMap,
    instruments: HashMap<String, K>,
    exchange: ExchangeId,
}

/// Process a DBN record into a MarketEvent if applicable.
///
/// Returns `None` for record types we don't handle (e.g., InstrumentDefMsg, SymbolMappingMsg)
/// or for symbols not in the instruments map.
fn process_record<K: Clone>(
    record: RecordRef<'_>,
    symbol_map: &PitSymbolMap,
    instruments: &HashMap<String, K>,
    exchange: ExchangeId,
) -> Option<Result<MarketEvent<K, DataKind>, DataError>> {
    // Try TradeMsg first
    if let Some(trade) = record.get::<TradeMsg>() {
        // Resolve symbol from instrument_id via PitSymbolMap
        let symbol = symbol_map.get(trade.hd.instrument_id)?;

        let instrument = match instruments.get(symbol) {
            Some(key) => key.clone(),
            None => {
                trace!(%symbol, "Skipping trade for unknown symbol");
                return None;
            }
        };

        match dbn_trade_to_public_trade(trade) {
            Ok((time_exchange, public_trade)) => {
                return Some(Ok(MarketEvent {
                    time_exchange,
                    time_received: Utc::now(),
                    exchange,
                    instrument,
                    kind: DataKind::Trade(public_trade),
                }));
            }
            Err(e) => {
                // A record has exactly one rtype; no need to try Mbp1Msg.
                debug!(error = %e, %symbol, "Skipping invalid trade record");
                return None;
            }
        }
    }

    // Try Mbp1Msg for quotes/L1
    if let Some(mbp1) = record.get::<Mbp1Msg>() {
        let symbol = symbol_map.get(mbp1.hd.instrument_id)?;

        let instrument = match instruments.get(symbol) {
            Some(key) => key.clone(),
            None => {
                trace!(%symbol, "Skipping quote for unknown symbol");
                return None;
            }
        };

        match dbn_mbp1_to_orderbook_l1(mbp1) {
            Ok((time_exchange, l1)) => {
                return Some(Ok(MarketEvent {
                    time_exchange,
                    time_received: Utc::now(),
                    exchange,
                    instrument,
                    kind: DataKind::OrderBookL1(l1),
                }));
            }
            Err(e) => {
                debug!(error = %e, %symbol, "Skipping invalid quote record");
                return None;
            }
        }
    }

    // Try OhlcvMsg for candles
    if let Some(ohlcv) = record.get::<OhlcvMsg>() {
        // Derive the interval from this record's own rtype: one connection may
        // interleave multiple OHLCV schemas. `None` => an rtype with no
        // CandleInterval (ohlcv-eod / ohlcv-deprecated); skip it observably
        // rather than crashing the stream (a caller can subscribe ohlcv-eod).
        let interval = match rtype_to_candle_interval(ohlcv.hd.rtype) {
            Some(interval) => interval,
            None => {
                debug!(
                    rtype = ohlcv.hd.rtype,
                    "Skipping OHLCV record with no CandleInterval (eod/deprecated)"
                );
                return None;
            }
        };

        let symbol = symbol_map.get(ohlcv.hd.instrument_id)?;

        let instrument = match instruments.get(symbol) {
            Some(key) => key.clone(),
            None => {
                trace!(%symbol, "Skipping candle for unknown symbol");
                return None;
            }
        };

        // Native OHLCV bars are always final/closed, so every record yields a
        // candle. Conversion only fails on timestamp/close_time overflow, which
        // the close_time contract requires be surfaced, never skipped.
        return Some(
            dbn_ohlcv_to_candle(ohlcv, interval).map(|(time_exchange, candle)| MarketEvent {
                time_exchange,
                time_received: Utc::now(),
                exchange,
                instrument,
                kind: DataKind::Candle(candle),
            }),
        );
    }

    // Other record types (InstrumentDefMsg, etc.) are silently skipped
    None
}

/// Validate and map a candle interval for the **live** feed (`Sec1`/`Min1` only).
///
/// First rejects the intervals Databento serves on no OHLCV path (via
/// [`ensure_databento_ohlcv_supports`]), then additionally rejects `Hour1`/`Day1`:
/// those are valid Databento schemas but are not reliably emitted by the live
/// gateway, so they are historical-only here (the G21.1 hedge). Widening this is
/// an additive change if a live key later confirms the larger bars stream.
fn ensure_databento_live_ohlcv_supports(
    exchange: ExchangeId,
    interval: CandleInterval,
) -> Result<Schema, DataError> {
    let schema = ensure_databento_ohlcv_supports(exchange, interval)?;
    match interval {
        CandleInterval::Sec1 | CandleInterval::Min1 => Ok(schema),
        _ => Err(DataError::UnsupportedInterval { exchange, interval }),
    }
}

#[cfg(test)]
// Test code may unwrap freely since panics indicate test failure
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn live_ohlcv_accepts_only_sec1_and_min1() {
        let ex = ExchangeId::DatabentoGlbx;
        assert_eq!(
            ensure_databento_live_ohlcv_supports(ex, CandleInterval::Sec1).unwrap(),
            Schema::Ohlcv1S
        );
        assert_eq!(
            ensure_databento_live_ohlcv_supports(ex, CandleInterval::Min1).unwrap(),
            Schema::Ohlcv1M
        );

        // 1h/1d are valid Databento schemas but historical-only on the live feed
        // (G21.1 hedge): rejected observably, not silently subscribed.
        for interval in [CandleInterval::Hour1, CandleInterval::Day1] {
            assert!(
                matches!(
                    ensure_databento_live_ohlcv_supports(ex, interval),
                    Err(DataError::UnsupportedInterval { interval: i, .. }) if i == interval
                ),
                "expected {interval} to be rejected live"
            );
        }

        // A non-native interval is rejected too.
        assert!(ensure_databento_live_ohlcv_supports(ex, CandleInterval::Min5).is_err());
    }
}
