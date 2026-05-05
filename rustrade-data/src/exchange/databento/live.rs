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

use super::error::DatabentoResultExt;
use super::transformer::{dbn_mbp1_to_orderbook_l1, dbn_trade_to_public_trade};
use crate::error::DataError;
use crate::event::{DataKind, MarketEvent};
use chrono::Utc;
use databento::LiveClient;
use databento::dbn::{Mbp1Msg, PitSymbolMap, RecordRef, Schema, TradeMsg};
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
    /// A stream of `MarketEvent<K, DataKind>` where `DataKind` is either `Trade` or
    /// `OrderBookL1` depending on the subscribed schema.
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
                            let err = DataError::Socket(format!("receiving record: {e}"));
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

    // Other record types (InstrumentDefMsg, etc.) are silently skipped
    None
}
