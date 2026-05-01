//! Interactive Brokers support types.
//!
//! This module provides shared types for IB integration used by both
//! rustrade-execution and rustrade-data.

pub mod contract;

pub use contract::ContractRegistry;
