//! The Massive market stream.

use super::connection::MassiveAttachment;
use crate::subscriber::shared_stream::SharedStream;

/// The market stream every Massive subscription kind is served over: a [`SharedStream`] reading a
/// [`MassiveAttachment`], its view of the cluster's shared connection.
pub type MassiveStream<StreamTransformer> = SharedStream<MassiveAttachment, StreamTransformer>;
