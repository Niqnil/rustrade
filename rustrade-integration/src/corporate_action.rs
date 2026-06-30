//! PULL-based corporate-action **sourcing** abstractions.
//!
//! This module models the *global-PULL reference-data* shape shared by providers such as
//! Massive/Polygon and Alpaca: query a provider by symbol + effective-date range and receive a
//! stream of corporate-action facts. The yielded fact is the
//! [`CorporateAction`](rustrade_instrument::corporate_action::CorporateAction) descriptor
//! (re-exported here), keyed by an unresolved provider symbol ([`SmolStr`]); a wrapper resolves the
//! symbol to its engine instrument key and supplies the rounding policy + stamping instant before
//! injecting an engine event.
//!
//! # The kind lives in the trait name, not a filter
//!
//! There is deliberately **no unified `CorporateActionSource` with a `kinds` filter**. Reference
//! providers expose a *separate endpoint per action type* (splits vs dividends), so the action kind
//! is encoded in the trait itself: [`StockSplitSource`] fetches splits. A `DividendSource` sibling
//! trait is the natural future addition when dividends are needed — each fully typed, with no
//! discriminant enum to keep in lockstep with
//! [`CorporateActionKind`](rustrade_instrument::corporate_action::CorporateActionKind).
//!
//! # What is intentionally out of scope
//!
//! Account-scoped or push-based sources (e.g. Interactive Brokers' WSH / Flex Query feeds) do not
//! fit the global-by-symbol PULL model and are **intentionally not** expressed through this trait.
//! Such a source maps its data onto the same [`CorporateAction`] descriptor via its own adapter.

use chrono::NaiveDate;
use derive_more::Constructor;
use futures::Stream;
use smol_str::SmolStr;

#[doc(inline)]
pub use rustrade_instrument::corporate_action::{CorporateAction, CorporateActionKind};

/// Filter for a PULL corporate-action reference-data query.
///
/// All fields are optional restrictions; the default (empty `symbols`, no date bounds) requests
/// every action a source knows about, subject to provider limits. Date bounds are inclusive and
/// expressed against the action's **effective date**.
///
/// # Symbols
///
/// `symbols` holds **provider** ticker strings (e.g. `"AAPL"`), not resolved engine keys. An empty
/// `symbols` list means "no symbol restriction". How a source maps multiple symbols onto its
/// transport (one request per symbol vs a single batched request) is an implementation detail.
// Public filter likely to grow (e.g. pagination cursor/limit). `#[non_exhaustive]` keeps adding
// fields non-breaking for downstream users; construct via `CorporateActionFilter::new(..)`, or
// `Default::default()` followed by per-field assignment when forward-compatibility matters
// (struct-update syntax is unavailable to downstream crates on a `#[non_exhaustive]` struct).
#[derive(Debug, Clone, Default, PartialEq, Eq, Constructor)]
#[non_exhaustive]
pub struct CorporateActionFilter {
    /// Provider ticker symbols to fetch; empty means no symbol restriction.
    pub symbols: Vec<SmolStr>,
    /// Inclusive lower bound on the action's effective date, if any.
    pub start: Option<NaiveDate>,
    /// Inclusive upper bound on the action's effective date, if any.
    pub end: Option<NaiveDate>,
}

/// A source of **stock-split** corporate actions, fetched by symbol + effective-date range.
///
/// Implementors wrap a provider's splits endpoint (e.g. Massive's `/v3/reference/splits`) and yield
/// [`CorporateAction<SmolStr>`] descriptors — the split ratio computed via the shared
/// [`CorporateActionKind::stock_split`] helper, keyed by the provider symbol. The consumer resolves
/// the symbol to an engine instrument key and constructs the engine event.
///
/// The returned stream is lazy: items are produced as it is polled, and the stream is `Send` so it
/// can be driven from any async runtime/task.
///
/// # Object safety
///
/// This trait is **not object-safe**: `fetch_splits` returns `impl Stream + Send` (return-position
/// `impl Trait` in a trait method), which the compiler cannot erase to `dyn`. So
/// `Box<dyn StockSplitSource>` and `Vec<Box<dyn StockSplitSource>>` (e.g. a multi-provider routing
/// table) cannot be formed directly. To hold heterogeneous sources, dispatch over an enum of the
/// concrete types, or write a thin wrapper whose method calls `.boxed()` on the stream and erases to
/// `BoxStream<'_, Result<CorporateAction<SmolStr>, E>>`.
pub trait StockSplitSource {
    /// Error yielded as a stream item when a fetch fails (e.g. a transport or deserialisation
    /// error). Per-item so a partial result set is still observable up to the failure point.
    ///
    /// No bound is imposed here; consumers add whatever their use-case requires at the call site
    /// (e.g. `S::Error: Display` for logging, or `S::Error: std::error::Error` for `?` propagation).
    type Error;

    /// Fetch the stock splits matching `filter`, as a stream of [`CorporateAction<SmolStr>`]
    /// descriptors (or per-item errors).
    ///
    /// # `effective_date` contract
    ///
    /// Every implementation MUST populate each descriptor's
    /// [`effective_date`](rustrade_instrument::corporate_action::CorporateAction::effective_date)
    /// with the **market execution date** — the
    /// calendar date on which the split takes effect on-exchange (shares outstanding adjust, the
    /// price re-bases). It is **not** the declaration date, ex-date, or record date: those can
    /// precede execution by days to weeks, and stamping the engine adjustment to one of them applies
    /// the split early and silently corrupts backtest results.
    ///
    /// Providers expose this date under different field names (e.g. Massive's `execution_date`), and
    /// some surface several candidate dates per split. Which provider field satisfies the
    /// execution-date semantic is a **static, per-implementation decision** — it is the same field
    /// for every event a given source yields, so it is a property of the implementation, not of the
    /// individual fact. Each implementation MUST therefore document, in its own `fetch_splits`
    /// rustdoc, which provider field it maps onto `effective_date`, and SHOULD pin that mapping with
    /// a unit test against a known fixture so a later refactor cannot change it unnoticed.
    fn fetch_splits(
        &self,
        filter: &CorporateActionFilter,
    ) -> impl Stream<Item = Result<CorporateAction<SmolStr>, Self::Error>> + Send;
}
