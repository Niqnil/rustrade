//! Databento market data connectors for historical and live data.
//!
//! Provides access to institutional-grade market data with nanosecond precision
//! across equities, futures, options, and crypto futures via CME.
//!
//! # Architecture
//!
//! Unlike WebSocket-based connectors (Binance, Alpaca), Databento uses high-level
//! client wrappers that handle connection management internally:
//!
//! - [`DatabentoHistorical`]: One-shot queries for data older than 24 hours
//! - Live streaming and intraday replay support is planned.
//!
//! # Authentication
//!
//! Requires `DATABENTO_API_KEY` environment variable. All endpoints are authenticated.
//!
//! # Datasets
//!
//! | Dataset | ExchangeId | Description |
//! |---------|------------|-------------|
//! | GLBX.MDP3 | `DatabentoGlbx` | CME Globex futures |
//! | XNAS.ITCH | `DatabentoXnas` | Nasdaq equities |
//! | XNYS.PILLAR | `DatabentoXnys` | NYSE equities |
//! | DBEQ.MAX | `DatabentoDbeq` | Composite US equities |
//! | OPRA.PILLAR | `DatabentoOpra` | US options consolidated |

mod error;
pub mod historical;
pub(crate) mod transformer;

pub use historical::{DatabentoHistorical, load_quotes_from_dbn, load_trades_from_dbn};
