//! Small HTTP helpers shared by the REST-based exchange integrations.

/// Truncate `s` to at most `max_bytes` bytes, rounding down to a UTF-8 char boundary.
///
/// A byte budget can land mid-character, which `String::truncate` and a naive `&s[..n]` both
/// reject (panicking on a non-boundary index). [`str::floor_char_boundary`] rounds down to the
/// nearest boundary — one always exists at or below any index — so the result is always a valid
/// `str`, at the cost of dropping the straddling character.
///
/// The cap is a parameter rather than a constant because callers budget differently for different
/// contexts: a full error message, a short inline snippet, and a proxy/CDN error page are not the
/// same size problem. Sharing the *function* keeps the boundary-safety logic in one place; sharing
/// one *constant* across those contexts would be false consistency.
pub(crate) fn truncate_str(s: &str, max_bytes: usize) -> String {
    s[..s.floor_char_boundary(max_bytes)].to_owned()
}

/// Cap for error-path body reads (see [`read_body_capped`]).
///
/// Generous relative to any real diagnostic envelope — so it never truncates a legitimate error or
/// status response the caller still needs to parse — while bounding a pathological proxy/CDN error
/// page that would otherwise be buffered without limit.
pub(crate) const MAX_ERROR_BODY_DOWNLOAD_BYTES: usize = 64 * 1024;

/// Read a response body, stopping after at most `cap` bytes and discarding the rest of the stream.
///
/// Intended for **error paths only**. A non-success response body is a human-facing diagnostic that
/// callers truncate further for storage anyway, so a pathological proxy/CDN error page must not be
/// buffered in full just to extract a short message. Success bodies are real payload and should
/// still be read in full via [`reqwest::Response::text`].
///
/// `cap` should be chosen well above any legitimate error/status envelope the caller may still need
/// to parse, yet far below an unbounded page — its purpose is to bound the pathological case, not to
/// trim normal responses (see [`MAX_ERROR_BODY_DOWNLOAD_BYTES`]). A byte cap can land mid-character,
/// so an invalid trailing sequence is decoded lossily (`U+FFFD`), which is acceptable for a
/// diagnostic string. Reaching the cap drops the response, aborting the remainder of the download.
///
/// Any `reqwest::Error` from the underlying stream is propagated unchanged, so a caller whose
/// `From<reqwest::Error>` conversion strips the URL (e.g. to avoid leaking a token in the query
/// string) keeps that protection.
pub(crate) async fn read_body_capped(
    mut response: reqwest::Response,
    cap: usize,
) -> Result<String, reqwest::Error> {
    let mut buf: Vec<u8> = Vec::new();
    while buf.len() < cap {
        let Some(chunk) = response.chunk().await? else {
            break;
        };
        let remaining = cap - buf.len();
        if chunk.len() <= remaining {
            buf.extend_from_slice(&chunk);
        } else {
            buf.extend_from_slice(&chunk[..remaining]);
            break;
        }
    }
    // `from_utf8` consumes `buf` without copying on the valid-UTF-8 path (the overwhelmingly common
    // case for a JSON/XML diagnostic); the lossy re-decode only pays for a second buffer when the cap
    // actually split a multi-byte character.
    Ok(String::from_utf8(buf)
        .unwrap_or_else(|error| String::from_utf8_lossy(error.as_bytes()).into_owned()))
}

#[cfg(test)]
mod truncate_str_tests {
    use super::*;

    #[test]
    fn shorter_than_the_cap_is_returned_whole() {
        assert_eq!(truncate_str("abc", 512), "abc");
    }

    #[test]
    fn longer_than_the_cap_is_cut_at_the_exact_byte() {
        assert_eq!(truncate_str("0123456789", 4), "0123");
    }

    #[test]
    fn a_cap_splitting_a_multi_byte_char_rounds_down_instead_of_panicking() {
        // `€` is 3 bytes, so a cap of 2 lands inside it. Rounding down drops the whole character
        // rather than slicing it into an invalid `str` (which would panic).
        assert_eq!(truncate_str("€", 2), "");
        assert_eq!(truncate_str("a€", 3), "a");
        assert_eq!(truncate_str("a€", 4), "a€");
    }

    #[test]
    fn a_cap_beyond_the_string_is_not_out_of_bounds() {
        // `floor_char_boundary` saturates at `len`, so an over-large cap is a no-op rather than a
        // panicking slice.
        assert_eq!(truncate_str("abc", usize::MAX), "abc");
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // Tests should panic on unexpected values.
mod tests {
    use super::*;
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// Serve `body` under a non-success status and read it back through [`read_body_capped`] at `cap`,
    /// exercising the real `reqwest` chunk stream rather than an in-memory buffer.
    async fn read_capped(body: impl Into<String>, cap: usize) -> String {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(500).set_body_string(body.into()))
            .mount(&server)
            .await;
        let response = reqwest::Client::new()
            .get(server.uri())
            .send()
            .await
            .unwrap();
        read_body_capped(response, cap).await.unwrap()
    }

    #[tokio::test]
    async fn body_under_cap_is_returned_whole() {
        // The common case: a real diagnostic envelope is far below the cap and must survive intact.
        assert_eq!(
            read_capped("{\"error\":\"not found\"}", 1024).await,
            "{\"error\":\"not found\"}"
        );
    }

    #[tokio::test]
    async fn body_over_cap_is_truncated_at_the_exact_byte() {
        assert_eq!(read_capped("0123456789", 4).await, "0123");
    }

    #[tokio::test]
    async fn cap_landing_mid_character_decodes_lossily_instead_of_panicking() {
        // `é` is 0xC3 0xA9, so a cap of 1 splits it. A byte cap has no obligation to respect char
        // boundaries — the contract is a valid `String` out, with the partial sequence as U+FFFD.
        assert_eq!(read_capped("é", 1).await, "\u{FFFD}");
    }

    #[tokio::test]
    async fn empty_body_is_read_as_empty_rather_than_erroring() {
        assert_eq!(read_capped("", 1024).await, "");
    }

    #[tokio::test]
    async fn pathological_body_is_bounded_to_the_shared_download_cap() {
        // The case the helper exists for: a body far larger than the cap, delivered over multiple
        // chunks, is bounded to exactly `MAX_ERROR_BODY_DOWNLOAD_BYTES` instead of being buffered whole.
        let oversized = "a".repeat(MAX_ERROR_BODY_DOWNLOAD_BYTES * 4);
        let read = read_capped(oversized, MAX_ERROR_BODY_DOWNLOAD_BYTES).await;
        assert_eq!(read.len(), MAX_ERROR_BODY_DOWNLOAD_BYTES);
    }
}
