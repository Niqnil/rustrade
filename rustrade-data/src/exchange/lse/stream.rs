//! The London Strategic Edge market stream.

use super::{connection::LseAttachment, transformer::LseTransformer};
use crate::subscriber::shared_stream::SharedStream;

/// The market stream every London Strategic Edge subscription kind is served over: a
/// [`SharedStream`] reading an [`LseAttachment`], its view of the subscriber's shared connection.
///
/// The connection hands the stream what it needs to resume, if its subscriber resumes, and the
/// stream hands that to its [`LseTransformer`] before any frame reaches it — see
/// [`LseResume`](super::transformer::LseResume).
pub type LseStream<Exchange, InstrumentKey, Kind> =
    SharedStream<LseAttachment, LseTransformer<Exchange, InstrumentKey, Kind>>;
