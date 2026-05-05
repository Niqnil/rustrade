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
//! - [`DatabentoLive`]: Real-time streaming for live and intraday replay data
//!
//! # Connection Model
//!
//! - **One connection per dataset**: Each client connects to one dataset (e.g., GLBX.MDP3)
//! - **Multiple symbols per connection**: Databento recommends consolidating subscriptions
//! - **Connection limits**: 10/dataset (standard) or 50/dataset (enterprise)
//!
//! # Authentication
//!
//! Requires `DATABENTO_API_KEY` environment variable from an active Databento
//! subscription.
//!
//! # Testing
//!
//! **Live data integration is NOT TESTED** — Databento does not offer development/sandbox
//! keys and we do not have a subscription.
//!
//! - **Offline tests** (`databento_transformer.rs`): Verify DBN-to-rustrade
//!   transformation using fixture files. Runs in CI.
//! - **Integration tests** (`databento_integration.rs`): Written but not run.
//!   Network calls (auth, API, WebSocket) are unverified.
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
pub mod live;
pub(crate) mod transformer;

pub use historical::{DatabentoHistorical, load_quotes_from_dbn, load_trades_from_dbn};
pub use live::DatabentoLive;
