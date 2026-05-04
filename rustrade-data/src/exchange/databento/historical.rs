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

use super::error::DatabentoResultExt;
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
    /// # Arguments
    ///
    /// * `params` - Query parameters (dataset, symbols, time range)
    /// * `exchange` - ExchangeId to tag events with (should match dataset)
    /// * `instrument` - Instrument key to tag events with
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
    /// Uses MBP-1 (Market By Price level 1) schema.
    ///
    /// # Arguments
    ///
    /// * `params` - Query parameters (dataset, symbols, time range). Schema should be Mbp1.
    /// * `exchange` - ExchangeId to tag events with
    /// * `instrument` - Instrument key to tag events with
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
/// * `instrument` - Instrument key to tag events with
///
/// # Errors
///
/// Returns [`DataError::Socket`] if the file cannot be opened or the DBN
/// header is invalid.
pub fn load_trades_from_dbn<K: Clone>(
    path: &Path,
    exchange: ExchangeId,
    instrument: K,
) -> Result<impl Iterator<Item = Result<MarketEvent<K, PublicTrade>, DataError>>, DataError> {
    let decoder = DynDecoder::from_file(path, VersionUpgradePolicy::AsIs)
        .map_err(|e| DataError::Socket(format!("opening DBN file: {e}")))?;

    Ok(DbnTradeIterator {
        decoder,
        exchange,
        instrument,
    })
}

/// Load quotes from a pre-downloaded DBN file.
///
/// # Errors
///
/// Returns [`DataError::Socket`] if the file cannot be opened or the DBN
/// header is invalid.
pub fn load_quotes_from_dbn<K: Clone>(
    path: &Path,
    exchange: ExchangeId,
    instrument: K,
) -> Result<impl Iterator<Item = Result<MarketEvent<K, Quote>, DataError>>, DataError> {
    let decoder = DynDecoder::from_file(path, VersionUpgradePolicy::AsIs)
        .map_err(|e| DataError::Socket(format!("opening DBN file: {e}")))?;

    Ok(DbnQuoteIterator {
        decoder,
        exchange,
        instrument,
    })
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
                    return Some(Err(DataError::Socket(format!("decoding DBN record: {e}"))));
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
                    return Some(Err(DataError::Socket(format!("decoding DBN record: {e}"))));
                }
            }
        }
    }
}
