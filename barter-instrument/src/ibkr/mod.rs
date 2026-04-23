//! Interactive Brokers support types.
//!
//! This module provides shared types for IB integration used by both
//! barter-execution and barter-data.

pub mod contract;

pub use contract::ContractRegistry;
