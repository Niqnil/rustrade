#![forbid(unsafe_code)]
#![deny(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
#![warn(
    unused,
    clippy::cognitive_complexity,
    unused_crate_dependencies,
    unused_extern_crates,
    clippy::unused_self,
    clippy::useless_let_if_seq,
    missing_debug_implementations,
    rust_2018_idioms
)]
// Inherited from the barter-rs upstream: the generic, trait-heavy public API legitimately produces
// complex nested types and many-argument constructors, and `type_alias_bounds` fires on
// documentation-bearing alias bounds kept intentionally. Denying these adds churn without improving
// the API.
#![allow(clippy::type_complexity, clippy::too_many_arguments, type_alias_bounds)]
// Clippy 1.99 checks each field's span rather than the whole expression's, so the
// `Self { field: field }` that `derive_more::Constructor` expands to is reported at our own
// field declarations. Remove once rust-lang/rust-clippy#17525 is fixed in a stable release.
#![allow(clippy::redundant_field_names)]

//! # rustrade-integration
//! A low-level framework for composing web integrations.
//!
//! The other rustrade crates build their exchange integrations on it, for public market data and
//! for order execution. It is:
//! * **Low-level**: translates raw data streams received over the web into any data model, using
//!   arbitrary transformations.
//! * **Protocol-agnostic**: not tied to one protocol, input or output model, or transformation.
//!   HTTP and WebSocket implementations are included.
//!
//! ## Core abstractions
//! - **RestClient**: configurable, signed HTTP communication between client and server.
//! - **ExchangeStream**: configurable communication over an asynchronous stream protocol, such as
//!   WebSocket.
//!
//! Both translate between the server's data model and the client's.

// Silence unused_crate_dependencies for dev-dependencies: each is used only by the tests or
// examples of some features, so a test build with other features alone would report it.
#[cfg(test)]
use {
    rustrade_instrument as _, serde_json as _, sha2 as _, tokio as _, tokio_stream as _,
    tokio_test as _, wiremock as _,
};

use ::serde::{Deserialize, Serialize};

/// All [`Error`](std::error::Error)s this crate generates.
#[cfg(feature = "error")]
pub mod error;

/// Contains `StreamParser` implementations for transforming communication protocol specific
/// messages into a generic output data structure.
#[cfg(feature = "protocol")]
pub mod protocol;

/// Contains the flexible `Metric` type used for representing real-time metrics generically.
#[cfg(feature = "metric")]
pub mod metric;

/// Defines a [`SubscriptionId`](subscription::SubscriptionId) new type representing a unique
/// `SmolStr` identifier for a data stream (market data, account data) that has been
/// subscribed to.
#[cfg(feature = "subscription")]
pub mod subscription;

/// Defines a trait [`Tx`](channel::Tx) abstraction over different channel kinds, as well as
/// other channel utilities.
///
/// eg/ `UnboundedTx`, `ChannelTxDroppable`, etc.
#[cfg(feature = "channel")]
pub mod channel;

#[cfg(feature = "collection")]
pub mod collection;

/// Serialisation and deserialisation transformers and other utilities.
#[cfg(feature = "serde")]
pub mod serde;

/// `Stream` extensions and utilities.
#[cfg(feature = "stream")]
pub mod stream;

/// `ReconnectingSocket` extension and utilities.
#[cfg(feature = "socket")]
pub mod socket;

/// PULL-based corporate-action sourcing: the [`StockSplitSource`](corporate_action::StockSplitSource)
/// trait + [`CorporateActionFilter`](corporate_action::CorporateActionFilter), re-exporting the
/// `CorporateAction` descriptor from `rustrade-instrument`.
#[cfg(feature = "corporate-action")]
pub mod corporate_action;

/// [`Validator`]s are capable of determining if their internal state is satisfactory to fulfill
/// some use case defined by the implementor.
pub trait Validator {
    type Error;

    /// Check if `Self` is valid for some use case.
    fn validate(self) -> Result<Self, Self::Error>
    where
        Self: Sized;
}

/// [`Transformer`]s are capable of transforming any `Input` into an iterator of
/// `Result<Self::Output, Self::Error>`s.
pub trait Transformer {
    type Error;
    type Input;
    type Output;
    type OutputIter: IntoIterator<Item = Result<Self::Output, Self::Error>>;

    fn transform(&mut self, input: Self::Input) -> Self::OutputIter;
}

/// Determines if something is considered "unrecoverable", such as an unrecoverable error.
///
/// Note that the meaning of [`Unrecoverable`] may vary depending on the context.
pub trait Unrecoverable {
    fn is_unrecoverable(&self) -> bool;
}

/// Trait that communicates if something is terminal (eg/ requires shutdown or restart).
pub trait Terminal {
    fn is_terminal(&self) -> bool;
}

/// Indicates an `Iterator` or `Stream` has ended.
#[derive(
    Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Deserialize, Serialize,
)]
pub struct FeedEnded;

/// Either an "Admin" or a "Payload" message.
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
pub enum Message<A, T> {
    Admin(A),
    Payload(T),
}

/// Admin message that's either a "Protocol" or "Application" level event.
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
pub enum Admin<P, A> {
    Protocol(P),
    Application(A),
}
