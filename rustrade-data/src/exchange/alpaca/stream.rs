//! The Alpaca market stream.

use super::connection::AlpacaAttachment;
use crate::subscriber::shared_stream::SharedStream;

/// The market stream every Alpaca subscription kind is served over: a [`SharedStream`] reading an
/// [`AlpacaAttachment`], its view of the feed's shared connection.
pub type AlpacaStream<StreamTransformer> = SharedStream<AlpacaAttachment, StreamTransformer>;
