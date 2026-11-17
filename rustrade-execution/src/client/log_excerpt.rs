//! Excerpts of venue text for log lines.

/// The most characters of a stream frame that a log line quotes.
pub(crate) const FRAME_EXCERPT_CHARS: usize = 200;

/// The first `max_chars` characters of `text`, for a log line about it.
///
/// The cut is counted in characters, so it never falls inside a multi-byte character, where a
/// byte slice would panic.
pub(crate) fn excerpt(text: &str, max_chars: usize) -> &str {
    text.char_indices()
        .nth(max_chars)
        .map_or(text, |(end, _)| &text[..end])
}

/// The first [`FRAME_EXCERPT_CHARS`] characters of a stream frame, for a log line about it.
pub(crate) fn frame_excerpt(frame: &str) -> &str {
    excerpt(frame, FRAME_EXCERPT_CHARS)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn excerpt_cuts_at_max_chars_on_a_char_boundary() {
        assert_eq!(excerpt("short", 3), "sho");
        assert_eq!(excerpt("short", 5), "short");
        assert_eq!(excerpt("short", 9), "short");
        // "é" is two bytes, so three characters are six bytes: a byte cut at 3 would panic.
        assert_eq!(excerpt("éééé", 3), "ééé");
    }

    #[test]
    fn frame_excerpt_cuts_at_200_characters() {
        let exact = "a".repeat(FRAME_EXCERPT_CHARS);
        assert_eq!(frame_excerpt(&exact), exact);
        let long = "é".repeat(300);
        assert_eq!(frame_excerpt(&long), "é".repeat(200));
    }
}
