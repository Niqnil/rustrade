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
//! - [`DatabentoHistorical`](crate::exchange::databento::DatabentoHistorical): One-shot queries for data older than 24 hours
//! - [`DatabentoLive`](crate::exchange::databento::DatabentoLive): Real-time streaming for live and intraday replay data
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
//! # ⚠️ Data licensing — redistribution is prohibited
//!
//! This crate is MIT-licensed. That covers **our code** and says nothing about the **data** a
//! Databento subscription retrieves through it.
//!
//! Databento licenses market data per subscriber. Its User Agreement defines "Redistribution" to
//! cover the publication or distribution of covered data and "all other means of furnishing such
//! data or other information derived from the same to entities other than Customer", and requires
//! the customer to limit use to internal purposes and "not to engage in any Redistribution of
//! Third-Party Data" — except where permitted by the relevant exchange's own agreement or by prior
//! written approval from both that exchange and Databento, each in their sole discretion. Much of
//! what is reachable here is exchange data (`GLBX.MDP3` is CME Group), so both gates apply.
//!
//! In practice, for anyone using this module:
//!
//! - **Do not commit retrieved data** — not as test fixtures, example datasets, CI artifacts or
//!   golden files. This repository's offline tests generate synthetic DBN records for exactly this
//!   reason rather than replaying a capture.
//! - Records you receive are for your own internal use; passing them on, or deriving a feed or
//!   product from them for third parties, needs written permission you almost certainly do not
//!   have by default.
//!
//! Terms: <https://databento.com/legal/databento-user-agreement>
//!
//! # Testing Status
//!
//! **NOT tested in CI** — no permission to use credentials for CI.
//!
//! **Tested locally:**
//! - Offline fixture tests (`databento_transformer.rs`): DBN-to-rustrade transformation
//!
//! **NOT tested locally (no subscription, no sandbox keys):**
//! - Historical API (`databento_integration.rs`): authentication, queries
//! - Live streaming (`databento_integration.rs`): WebSocket connection, data reception
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

pub use error::DatabentoErrorKind;
pub use historical::{
    DatabentoHistorical, DatabentoOhlcvParams, load_quotes_from_dbn, load_trades_from_dbn,
};
pub use live::DatabentoLive;
