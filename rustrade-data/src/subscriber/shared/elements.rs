//! Routing for providers that pack several messages into one frame, as a JSON array.
//!
//! A frame such as `[{..AAPL trade..},{..MSFT quote..}]` can carry elements for several streams,
//! and a stream decoding an element for a subscription it does not hold would report it as an
//! error. So the frame is split rather than forwarded whole: a stream every element of a frame
//! belongs to receives the frame itself, a reference-counted clone; any other receives a frame
//! holding only its own elements, copied verbatim. The provider reads only what routing needs from
//! each element, so each stream's decoder stays the only full parse.

use super::{AttachId, Protocol, Registry};
use fnv::FnvHashMap;
use rustrade_integration::protocol::websocket::WsMessage;
use serde_json::value::RawValue;
use tracing::{debug, warn};

/// One data element of a frame, as routing reads it.
#[derive(Debug)]
pub(crate) struct Element<'a, Slot> {
    /// The subscription the element belongs to.
    pub(crate) slot: Slot,
    /// The element exactly as the provider sent it.
    pub(crate) raw: &'a RawValue,
}

/// What a frame holds, before its elements are classified.
pub(crate) enum Elements<'a> {
    /// Every element of the frame, in order.
    Array(Vec<&'a RawValue>),
    /// The provider closed the socket, for the reason given.
    Closed(String),
    /// Nothing to route or answer: a ping, a pong, or a frame that is not an array, which is
    /// logged.
    Ignored,
}

/// Split `message` into its elements, without reading any of them.
pub(crate) fn elements<P: Protocol>(message: &WsMessage) -> Elements<'_> {
    let text = match message {
        WsMessage::Text(text) => text.as_str(),
        WsMessage::Binary(bytes) => match std::str::from_utf8(bytes) {
            Ok(text) => text,
            Err(_) => {
                warn!(
                    len = bytes.len(),
                    "{} sent a binary frame that is not UTF-8; ignored",
                    P::NAME,
                );
                return Elements::Ignored;
            }
        },
        WsMessage::Close(frame) => {
            return Elements::Closed(format!("closed by {}: {frame:?}", P::NAME));
        }
        // Pings are answered by the socket itself, and pongs are the connection's own business.
        _ => return Elements::Ignored,
    };

    match serde_json::from_str::<Vec<&RawValue>>(text) {
        Ok(elements) => Elements::Array(elements),
        Err(error) => {
            warn!(
                %error,
                frame = %excerpt(text),
                "{} sent a frame that is not a JSON array; ignored",
                P::NAME,
            );
            Elements::Ignored
        }
    }
}

/// The start of `text`, short enough for a log line.
pub(crate) fn excerpt(text: &str) -> String {
    text.chars().take(200).collect()
}

/// Deliver a frame's data `elements` to the registrations holding them.
///
/// A registration every element of `frame` belongs to — `total` counts the frame's elements of
/// every kind — receives `frame` itself. Any other receives a frame of only its own elements, in
/// their original order and spelling.
pub(crate) fn route<P: Protocol>(
    registry: &mut Registry<P>,
    frame: &WsMessage,
    total: usize,
    elements: &[Element<'_, P::Slot>],
) {
    if elements.is_empty() {
        return;
    }

    // Counted before anything is copied, so a stream owning the whole frame costs no copy.
    let mut counts = FnvHashMap::<AttachId, usize>::default();

    for element in elements {
        let Some(holders) = registry.holders(&element.slot) else {
            // Normal for a moment after an unsubscribe, while frames already in flight arrive.
            debug!(
                slot = ?element.slot,
                "{} message for a subscription no stream holds",
                P::NAME,
            );
            continue;
        };

        for id in holders {
            *counts.entry(*id).or_default() += 1;
        }
    }

    for (id, count) in counts {
        let frame = if count == total {
            frame.clone()
        } else {
            WsMessage::text(share(registry, id, elements))
        };

        registry.deliver(id, frame);
    }
}

/// A frame of only the `elements` `id` holds, in their original order and spelling.
fn share<P: Protocol>(
    registry: &Registry<P>,
    id: AttachId,
    elements: &[Element<'_, P::Slot>],
) -> String {
    // Sized for every element, so it never grows: bounded by the frame it is cut from.
    let capacity = elements
        .iter()
        .map(|element| element.raw.get().len() + 1)
        .sum::<usize>()
        + 1;
    let mut share = String::with_capacity(capacity);

    share.push('[');
    for element in elements.iter().filter(|element| {
        registry
            .holders(&element.slot)
            .is_some_and(|holders| holders.contains(&id))
    }) {
        if share.len() > 1 {
            share.push(',');
        }
        share.push_str(element.raw.get());
    }
    share.push(']');

    share
}
