/// Data stream abstractions and configuration.
pub mod data;

/// Stream extension traits.
pub mod ext;

/// Stream utility functions.
pub mod util;

// `ExchangeStream` parses protocol messages, so it needs `protocol` on top of `stream`.
#[cfg(feature = "protocol")]
mod exchange;
#[cfg(feature = "protocol")]
pub use exchange::ExchangeStream;
