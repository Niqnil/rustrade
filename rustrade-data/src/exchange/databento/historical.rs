//! Historical data fetcher for Databento.
//!
//! Provides one-shot historical data queries and DBN file loading via [`DatabentoHistorical`].
//!
//! # Example
//!
//! ```ignore
//! use rustrade_data::exchange::databento::DatabentoHistorical;
//! use databento::historical::timeseries::GetRangeParams;
//!
//! let mut client = DatabentoHistorical::from_env()?;
//!
//! // Fetch trades for ES futures
//! let params = GetRangeParams::builder()
//!     .dataset("GLBX.MDP3")
//!     .symbols("ES.FUT")
//!     .schema(dbn::Schema::Trades)
//!     .date_time_range(start..end)
//!     .build();
//!
//! let trades = client.fetch_trades(&params, ExchangeId::DatabentoGlbx, "ES").await?;
//! ```

use super::error::{DatabentoResultExt, decode_error};
use super::transformer::{dbn_mbp1_to_quote, dbn_trade_to_public_trade};
use crate::error::DataError;
use crate::event::MarketEvent;
use crate::subscription::{quote::Quote, trade::PublicTrade};
use chrono::Utc;
use databento::HistoricalClient;
use databento::dbn::decode::DynDecoder;
use databento::dbn::enums::VersionUpgradePolicy;
use databento::dbn::{self, decode::DecodeRecord};
use databento::historical::timeseries::GetRangeParams;
use futures::Stream;
use rustrade_instrument::exchange::ExchangeId;
use std::path::Path;
use tracing::{debug, info};

/// Historical data fetcher for Databento.
///
/// Wraps [`HistoricalClient`] for fetching historical market data older than 24 hours.
#[derive(Debug)]
pub struct DatabentoHistorical {
    client: HistoricalClient,
}

impl DatabentoHistorical {
    /// Create a new historical client using API key from environment.
    ///
    /// Reads `DATABENTO_API_KEY` from environment variables.
    ///
    /// # Errors
    ///
    /// Returns error if `DATABENTO_API_KEY` is not set or client construction fails.
    pub fn from_env() -> Result<Self, DataError> {
        debug!("Creating Databento historical client from env");
        let client = HistoricalClient::builder()
            .key_from_env()
            .with_context("reading API key from env")?
            .build()
            .with_context("building historical client")?;

        Ok(Self { client })
    }

    /// Create a new historical client with an explicit API key.
    ///
    /// # Errors
    ///
    /// Returns error if client construction fails.
    pub fn new(api_key: &str) -> Result<Self, DataError> {
        debug!("Creating Databento historical client");
        let client = HistoricalClient::builder()
            .key(api_key)
            .with_context("setting API key")?
            .build()
            .with_context("building historical client")?;

        Ok(Self { client })
    }

    /// Fetch historical trades for the given parameters.
    ///
    /// Collects all records into memory before returning. For large queries
    /// (millions of records), consider [`fetch_trades_stream`](Self::fetch_trades_stream)
    /// to process records incrementally.
    ///
    /// # Arguments
    ///
    /// * `params` - Query parameters (dataset, symbols, time range)
    /// * `exchange` - ExchangeId to tag events with (should match dataset)
    /// * `instrument` - Instrument key to tag events with
    ///
    /// # Performance
    ///
    /// The instrument key is cloned for each record. For high-frequency data,
    /// use [`Arc<K>`](std::sync::Arc) to avoid per-record heap allocations:
    ///
    /// ```ignore
    /// let instrument = Arc::new("ES".to_string());
    /// let trades = client.fetch_trades(&params, exchange, instrument).await?;
    /// ```
    ///
    /// # Returns
    ///
    /// Vector of trades converted to [`PublicTrade`] events. Each event's
    /// `time_received` is stamped at decode time on the local machine.
    pub async fn fetch_trades<K: Clone>(
        &mut self,
        params: &GetRangeParams,
        exchange: ExchangeId,
        instrument: K,
    ) -> Result<Vec<MarketEvent<K, PublicTrade>>, DataError> {
        debug!(?params, "Fetching historical trades from Databento");

        let mut decoder = self
            .client
            .timeseries()
            .get_range(params)
            .await
            .with_context("fetching trades")?;

        let mut trades = Vec::with_capacity(4096);

        while let Some(record) = decoder
            .decode_record::<dbn::TradeMsg>()
            .await
            .with_context("decoding trade record")?
        {
            match dbn_trade_to_public_trade(record) {
                Ok((time_exchange, trade)) => {
                    trades.push(MarketEvent {
                        time_exchange,
                        time_received: Utc::now(),
                        exchange,
                        instrument: instrument.clone(),
                        kind: trade,
                    });
                }
                Err(e) => {
                    debug!(error = %e, "Skipping invalid trade record");
                }
            }
        }

        info!(count = trades.len(), "Fetched historical trades");
        Ok(trades)
    }

    /// Fetch historical quotes (top-of-book) for the given parameters.
    ///
    /// Uses MBP-1 (Market By Price level 1) schema. Collects all records into
    /// memory before returning. For large queries, consider
    /// [`fetch_quotes_stream`](Self::fetch_quotes_stream).
    ///
    /// # Arguments
    ///
    /// * `params` - Query parameters (dataset, symbols, time range). Schema should be Mbp1.
    /// * `exchange` - ExchangeId to tag events with
    /// * `instrument` - Instrument key to tag events with
    ///
    /// # Performance
    ///
    /// The instrument key is cloned for each record. For high-frequency data,
    /// use [`Arc<K>`](std::sync::Arc) to avoid per-record heap allocations.
    ///
    /// Each event's `time_received` is stamped at decode time on the local machine.
    pub async fn fetch_quotes<K: Clone>(
        &mut self,
        params: &GetRangeParams,
        exchange: ExchangeId,
        instrument: K,
    ) -> Result<Vec<MarketEvent<K, Quote>>, DataError> {
        debug!(?params, "Fetching historical quotes from Databento");

        let mut decoder = self
            .client
            .timeseries()
            .get_range(params)
            .await
            .with_context("fetching quotes")?;

        let mut quotes = Vec::with_capacity(4096);

        while let Some(record) = decoder
            .decode_record::<dbn::Mbp1Msg>()
            .await
            .with_context("decoding quote record")?
        {
            match dbn_mbp1_to_quote(record) {
                Ok((time_exchange, quote)) => {
                    quotes.push(MarketEvent {
                        time_exchange,
                        time_received: Utc::now(),
                        exchange,
                        instrument: instrument.clone(),
                        kind: quote,
                    });
                }
                Err(e) => {
                    debug!(error = %e, "Skipping invalid quote record");
                }
            }
        }

        info!(count = quotes.len(), "Fetched historical quotes");
        Ok(quotes)
    }

    /// Stream historical trades without collecting into memory.
    ///
    /// Unlike [`fetch_trades`](Self::fetch_trades), this returns a stream that
    /// yields records as they're decoded, avoiding memory spikes for large queries.
    ///
    /// # Arguments
    ///
    /// * `params` - Query parameters (dataset, symbols, time range)
    /// * `exchange` - ExchangeId to tag events with
    /// * `instrument` - Instrument key to tag events with (use `Arc<K>` for efficiency)
    ///
    /// # Errors
    ///
    /// The outer `Result` returns [`DataError::Databento`] if the initial
    /// `get_range` request fails (e.g. authentication, network, or invalid
    /// params).
    ///
    /// Per-item errors are yielded as `Err` items on the stream and indicate
    /// a DBN decode failure. After a decode error the underlying decoder is
    /// in an unspecified state; callers should drop the stream rather than
    /// continue polling for more records. Records that successfully decode
    /// but fail conversion to [`PublicTrade`] are logged at `debug` and
    /// skipped silently.
    pub async fn fetch_trades_stream<K: Clone + Send + 'static>(
        &mut self,
        params: &GetRangeParams,
        exchange: ExchangeId,
        instrument: K,
    ) -> Result<impl Stream<Item = Result<MarketEvent<K, PublicTrade>, DataError>>, DataError> {
        debug!(?params, "Streaming historical trades from Databento");

        let decoder = self
            .client
            .timeseries()
            .get_range(params)
            .await
            .with_context("fetching trades")?;

        Ok(futures::stream::unfold(
            TradeStreamState {
                decoder,
                exchange,
                instrument,
            },
            |mut state| async move {
                loop {
                    match state.decoder.decode_record::<dbn::TradeMsg>().await {
                        Ok(Some(record)) => match dbn_trade_to_public_trade(record) {
                            Ok((time_exchange, trade)) => {
                                let event = MarketEvent {
                                    time_exchange,
                                    time_received: Utc::now(),
                                    exchange: state.exchange,
                                    instrument: state.instrument.clone(),
                                    kind: trade,
                                };
                                return Some((Ok(event), state));
                            }
                            Err(e) => {
                                debug!(error = %e, "Skipping invalid trade record");
                                continue;
                            }
                        },
                        Ok(None) => return None,
                        Err(e) => {
                            return Some((Err(decode_error(e.to_string())), state));
                        }
                    }
                }
            },
        ))
    }

    /// Stream historical quotes without collecting into memory.
    ///
    /// Unlike [`fetch_quotes`](Self::fetch_quotes), this returns a stream that
    /// yields records as they're decoded, avoiding memory spikes for large queries.
    ///
    /// # Arguments
    ///
    /// * `params` - Query parameters (dataset, symbols, time range). Schema should be Mbp1.
    /// * `exchange` - ExchangeId to tag events with
    /// * `instrument` - Instrument key to tag events with (use `Arc<K>` for efficiency)
    ///
    /// # Errors
    ///
    /// The outer `Result` returns [`DataError::Databento`] if the initial
    /// `get_range` request fails (e.g. authentication, network, or invalid
    /// params).
    ///
    /// Per-item errors are yielded as `Err` items on the stream and indicate
    /// a DBN decode failure. After a decode error the underlying decoder is
    /// in an unspecified state; callers should drop the stream rather than
    /// continue polling for more records. Records that successfully decode
    /// but fail conversion to [`Quote`] are logged at `debug` and skipped
    /// silently.
    pub async fn fetch_quotes_stream<K: Clone + Send + 'static>(
        &mut self,
        params: &GetRangeParams,
        exchange: ExchangeId,
        instrument: K,
    ) -> Result<impl Stream<Item = Result<MarketEvent<K, Quote>, DataError>>, DataError> {
        debug!(?params, "Streaming historical quotes from Databento");

        let decoder = self
            .client
            .timeseries()
            .get_range(params)
            .await
            .with_context("fetching quotes")?;

        Ok(futures::stream::unfold(
            QuoteStreamState {
                decoder,
                exchange,
                instrument,
            },
            |mut state| async move {
                loop {
                    match state.decoder.decode_record::<dbn::Mbp1Msg>().await {
                        Ok(Some(record)) => match dbn_mbp1_to_quote(record) {
                            Ok((time_exchange, quote)) => {
                                let event = MarketEvent {
                                    time_exchange,
                                    time_received: Utc::now(),
                                    exchange: state.exchange,
                                    instrument: state.instrument.clone(),
                                    kind: quote,
                                };
                                return Some((Ok(event), state));
                            }
                            Err(e) => {
                                debug!(error = %e, "Skipping invalid quote record");
                                continue;
                            }
                        },
                        Ok(None) => return None,
                        Err(e) => {
                            return Some((Err(decode_error(e.to_string())), state));
                        }
                    }
                }
            },
        ))
    }

    /// Returns the underlying client for advanced use cases.
    pub fn client(&self) -> &HistoricalClient {
        &self.client
    }

    /// Returns a mutable reference to the underlying client.
    pub fn client_mut(&mut self) -> &mut HistoricalClient {
        &mut self.client
    }
}

/// Load trades from a pre-downloaded DBN file.
///
/// Returns an iterator to avoid loading entire file into memory.
/// Useful for backtesting with previously downloaded data.
///
/// # Arguments
///
/// * `path` - Path to `.dbn` or `.dbn.zst` file
/// * `exchange` - ExchangeId to tag events with
/// * `instrument` - Instrument key to tag events with (use `Arc<K>` for efficiency)
///
/// # Errors
///
/// Returns [`DataError::Databento`] if the file cannot be opened or the DBN
/// header is invalid.
pub fn load_trades_from_dbn<K: Clone>(
    path: &Path,
    exchange: ExchangeId,
    instrument: K,
) -> Result<impl Iterator<Item = Result<MarketEvent<K, PublicTrade>, DataError>>, DataError> {
    let decoder =
        DynDecoder::from_file(path, VersionUpgradePolicy::AsIs).with_context("opening DBN file")?;

    Ok(DbnTradeIterator {
        decoder,
        exchange,
        instrument,
    })
}

/// Load quotes from a pre-downloaded DBN file.
///
/// # Arguments
///
/// * `path` - Path to `.dbn` or `.dbn.zst` file
/// * `exchange` - ExchangeId to tag events with
/// * `instrument` - Instrument key to tag events with (use `Arc<K>` for efficiency)
///
/// # Errors
///
/// Returns [`DataError::Databento`] if the file cannot be opened or the DBN
/// header is invalid.
pub fn load_quotes_from_dbn<K: Clone>(
    path: &Path,
    exchange: ExchangeId,
    instrument: K,
) -> Result<impl Iterator<Item = Result<MarketEvent<K, Quote>, DataError>>, DataError> {
    let decoder =
        DynDecoder::from_file(path, VersionUpgradePolicy::AsIs).with_context("opening DBN file")?;

    Ok(DbnQuoteIterator {
        decoder,
        exchange,
        instrument,
    })
}

// Stream state types for async streaming
struct TradeStreamState<K, D> {
    decoder: D,
    exchange: ExchangeId,
    instrument: K,
}

struct QuoteStreamState<K, D> {
    decoder: D,
    exchange: ExchangeId,
    instrument: K,
}

struct DbnTradeIterator<K> {
    decoder: DynDecoder<'static, std::io::BufReader<std::fs::File>>,
    exchange: ExchangeId,
    instrument: K,
}

impl<K: Clone> Iterator for DbnTradeIterator<K> {
    type Item = Result<MarketEvent<K, PublicTrade>, DataError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.decoder.decode_record::<dbn::TradeMsg>() {
                Ok(Some(record)) => match dbn_trade_to_public_trade(record) {
                    Ok((time_exchange, trade)) => {
                        return Some(Ok(MarketEvent {
                            time_exchange,
                            time_received: time_exchange,
                            exchange: self.exchange,
                            instrument: self.instrument.clone(),
                            kind: trade,
                        }));
                    }
                    Err(e) => {
                        debug!(error = %e, "Skipping invalid trade record");
                        continue;
                    }
                },
                Ok(None) => return None,
                Err(e) => {
                    return Some(Err(decode_error(e.to_string())));
                }
            }
        }
    }
}

struct DbnQuoteIterator<K> {
    decoder: DynDecoder<'static, std::io::BufReader<std::fs::File>>,
    exchange: ExchangeId,
    instrument: K,
}

impl<K: Clone> Iterator for DbnQuoteIterator<K> {
    type Item = Result<MarketEvent<K, Quote>, DataError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.decoder.decode_record::<dbn::Mbp1Msg>() {
                Ok(Some(record)) => match dbn_mbp1_to_quote(record) {
                    Ok((time_exchange, quote)) => {
                        return Some(Ok(MarketEvent {
                            time_exchange,
                            time_received: time_exchange,
                            exchange: self.exchange,
                            instrument: self.instrument.clone(),
                            kind: quote,
                        }));
                    }
                    Err(e) => {
                        debug!(error = %e, "Skipping invalid quote record");
                        continue;
                    }
                },
                Ok(None) => return None,
                Err(e) => {
                    return Some(Err(decode_error(e.to_string())));
                }
            }
        }
    }
}
