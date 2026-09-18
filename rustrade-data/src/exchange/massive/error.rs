//! Error types for Massive integration.

use crate::error::DataError;
use std::time::Duration;

/// Byte cap applied to a URL when rendering it into an error message.
///
/// URLs in these variants are stored in full — the value is often exactly what a
/// caller needs to debug a misbehaving server — but a server-supplied URL can be
/// long, and every `Display` of the error would otherwise carry it whole into a
/// log line. Matches the 512-byte budget the `Api`/`Auth` messages use; the
/// tighter 100-byte cap on `Deserialize` is for an inline snippet inside an
/// already-descriptive message, a different context.
const DISPLAY_URL_BYTES: usize = 512;

/// Truncate `s` to at most `max_bytes` for rendering, returning the kept prefix and an ellipsis
/// marker that is non-empty only when bytes were actually dropped.
///
/// A *silent* cut is worse than a visible one: a reader who cannot tell the value was shortened may
/// chase a URL whose informative `cursor=...` tail is simply missing. Every truncating `Display` arm
/// in this file goes through here so they all mark truncation the same way.
///
/// Borrows rather than reusing [`truncate_str`](crate::exchange::http::truncate_str) — that helper
/// returns an owned `String` for values a caller *stores*, whereas a `Display` arm only needs to
/// write the prefix straight out.
fn truncate_for_display(s: &str, max_bytes: usize) -> (&str, &'static str) {
    let boundary = s.floor_char_boundary(max_bytes);
    (&s[..boundary], if boundary < s.len() { "..." } else { "" })
}

/// Massive-specific errors.
///
/// The library returns these errors without automatic retry or reconnection.
/// Consumers decide how to handle rate limits, disconnections, and auth failures.
///
/// `#[non_exhaustive]`: new variants may be added without a major-version bump, so
/// downstream `match`es must include a wildcard arm.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum MassiveError {
    /// Rate limited by the API. Contains optional retry-after duration.
    ///
    /// Returned on HTTP 429 responses. The consumer decides whether and when to retry.
    RateLimited { retry_after: Option<Duration> },

    /// WebSocket connection dropped or ping timeout exceeded.
    ///
    /// Returned when:
    /// - WebSocket connection closes unexpectedly
    /// - Pong not received within 19 seconds of ping
    ///
    /// The consumer owns reconnection policy (backoff, credential refresh, dedup).
    Disconnected { reason: String },

    /// Authentication failed.
    ///
    /// Returned when:
    /// - API key is invalid or expired
    /// - API key lacks permission for the requested resource
    Auth { message: String },

    /// API returned an error response.
    ///
    /// Covers non-auth, non-rate-limit API errors (invalid parameters, not found, etc.)
    Api { status: u16, message: String },

    /// Network or HTTP client error.
    Http { message: String },

    /// JSON deserialization failed.
    Deserialize { message: String, payload: String },

    /// Environment variable not set.
    EnvVar { var: &'static str },

    /// Client-side input validation failed.
    ///
    /// Returned when input parameters are invalid before making an API request
    /// (e.g., timestamp out of representable range).
    InvalidInput { message: String },

    /// A paginated fetch exceeded the maximum number of pages before the API
    /// reported the end of the result set.
    ///
    /// Yielded as a terminal error on the affected stream. For a market-data
    /// client a silent truncation is indistinguishable from a genuinely small
    /// result set, so pagination fails loudly rather than returning a partial
    /// `Vec`. Hitting this limit indicates either an unexpectedly large query or
    /// a server that never signals the final page — inspect the query before
    /// retrying.
    PaginationLimitExceeded { pages: usize, limit: usize },

    /// A paginated fetch revisited a URL it had already fetched.
    ///
    /// Yielded as a terminal error on the affected stream. A `next_url` that
    /// points back to an already-visited page would otherwise loop forever;
    /// detecting the cycle turns silent non-termination into an observable
    /// failure.
    CyclicPagination { url: String },

    /// A paginated fetch encountered a URL longer than the paginator will follow.
    ///
    /// Yielded as a terminal error *before* the URL is recorded for cycle
    /// detection or requested. A page's `next_url` is server-controlled and
    /// success bodies are read unbounded, so without this bound a misbehaving
    /// origin could hand back arbitrarily long URLs — one retained per page, for
    /// the life of the stream.
    ///
    /// A real Massive cursor URL is a few hundred bytes, so hitting this
    /// indicates response tampering, a server-side bug, or (far less likely, but
    /// covered by the same check) a caller query that expands into an
    /// unreasonable first-page URL.
    PaginationUrlTooLong {
        /// Length of the rejected URL, in bytes.
        len: usize,
        /// The byte limit that was exceeded.
        limit: usize,
        /// A bounded prefix of the rejected URL, for diagnosis. Unlike
        /// [`UntrustedNextUrl::next_url`](MassiveError::UntrustedNextUrl) this is
        /// truncated by construction — retaining the value in full is the very
        /// thing the variant exists to prevent.
        ///
        /// [`Display`](std::fmt::Display) marks the value as truncated only when
        /// `prefix` is actually shorter than `len`, so a prefix that happens to
        /// be complete does not render a misleading trailing `...`.
        prefix: String,
    },

    /// A paginated fetch received a `next_url` outside the client's configured
    /// origin (scheme + host + port), or one that failed to parse as a URL.
    ///
    /// Yielded as a terminal error *before* the request is issued. The client
    /// attaches its API key as an `Authorization: Bearer` header only after this
    /// origin check passes, so a `next_url` that names an unexpected origin is
    /// rejected before the token is ever attached. A prefix check is insufficient
    /// — a look-alike host such as `https://api.massive.com.evil.example` (or the
    /// separator-less `https://api.massive.comevil.example`) shares the prefix
    /// yet is a different origin — so origins are parsed and compared, and a
    /// mismatched or unparseable `next_url` is rejected (fail-closed).
    ///
    /// A well-behaved Massive API only ever returns a `next_url` under its own
    /// origin (the path and `cursor` query differ; the origin is fixed). Hitting
    /// this indicates a server-side bug, response tampering, or a misconfigured
    /// base URL. Consumers may match on it to alert distinctly from ordinary
    /// input-validation failures.
    UntrustedNextUrl {
        /// The rejected `next_url`, exactly as received (unparsed if it failed
        /// to parse as a URL at all).
        ///
        /// Stored in full; [`Display`](std::fmt::Display) bounds the *rendered*
        /// form so an oversized value cannot flood a log line.
        next_url: String,
        /// ASCII-serialized origin (`scheme://host[:port]`) the client trusts,
        /// derived from its configured base URL.
        expected_origin: String,
    },
}

impl std::fmt::Display for MassiveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MassiveError::RateLimited { retry_after } => {
                write!(f, "Massive rate limited")?;
                if let Some(duration) = retry_after {
                    write!(f, " (retry after {:?})", duration)?;
                }
                Ok(())
            }
            MassiveError::Disconnected { reason } => {
                write!(f, "Massive disconnected: {}", reason)
            }
            MassiveError::Auth { message } => {
                write!(f, "Massive auth failed: {}", message)
            }
            MassiveError::Api { status, message } => {
                write!(f, "Massive API error ({}): {}", status, message)
            }
            MassiveError::Http { message } => {
                write!(f, "Massive HTTP error: {}", message)
            }
            MassiveError::Deserialize { message, payload } => {
                let (truncated, ellipsis) = truncate_for_display(payload, 100);
                write!(
                    f,
                    "Massive deserialize error: {} (payload: {truncated}{ellipsis})",
                    message
                )
            }
            MassiveError::EnvVar { var } => {
                write!(f, "Massive environment variable not set: {}", var)
            }
            MassiveError::InvalidInput { message } => {
                write!(f, "Massive invalid input: {}", message)
            }
            MassiveError::PaginationLimitExceeded { pages, limit } => {
                write!(
                    f,
                    "Massive pagination exceeded {limit}-page limit (fetched {pages} pages) — result may be incomplete"
                )
            }
            MassiveError::PaginationUrlTooLong { len, limit, prefix } => {
                // Derive the ellipsis from the data rather than assuming the cut happened. The
                // internal constructor always truncates, but `#[non_exhaustive]` sits on the enum,
                // not this variant, so it does not stop downstream code from building one with a
                // whole `prefix` — which the unconditional `...` would then misreport as truncated.
                // Comparing against `len` (the full URL's length) is exact and needs no cap constant.
                let ellipsis = if prefix.len() < *len { "..." } else { "" };
                write!(
                    f,
                    "Massive pagination URL exceeds the {limit}-byte limit ({len} bytes): {prefix}{ellipsis}"
                )
            }
            MassiveError::CyclicPagination { url } => {
                let (url, ellipsis) = truncate_for_display(url, DISPLAY_URL_BYTES);
                write!(
                    f,
                    "Massive pagination cycle detected: next_url revisits {url}{ellipsis}"
                )
            }
            MassiveError::UntrustedNextUrl {
                next_url,
                expected_origin,
            } => {
                let (next_url, ellipsis) = truncate_for_display(next_url, DISPLAY_URL_BYTES);
                write!(
                    f,
                    "Massive rejected next_url outside trusted origin {expected_origin}: {next_url}{ellipsis}"
                )
            }
        }
    }
}

impl std::error::Error for MassiveError {}

impl From<MassiveError> for DataError {
    fn from(err: MassiveError) -> Self {
        DataError::Socket(err.to_string())
    }
}

impl From<reqwest::Error> for MassiveError {
    fn from(err: reqwest::Error) -> Self {
        MassiveError::Http {
            message: err.to_string(),
        }
    }
}

impl From<tokio_tungstenite::tungstenite::Error> for MassiveError {
    fn from(err: tokio_tungstenite::tungstenite::Error) -> Self {
        MassiveError::Disconnected {
            reason: err.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A URL long enough to exceed [`DISPLAY_URL_BYTES`], built from a repeated ASCII byte so
    /// `len()` and char count coincide.
    fn oversized_url() -> String {
        format!("https://api.massive.com/v3/x?cursor={}", "a".repeat(4096))
    }

    #[test]
    fn cyclic_pagination_display_is_bounded() {
        let url = oversized_url();
        let rendered = MassiveError::CyclicPagination { url: url.clone() }.to_string();

        assert!(
            rendered.len() < url.len(),
            "an oversized URL must not reach the log line in full"
        );
        assert!(rendered.contains("cycle detected"));
        // The bounded prefix is still the real URL, so the message stays diagnostic.
        assert!(rendered.contains("https://api.massive.com/v3/x?cursor=aaa"));
        // A cut must be visible: an unmarked prefix reads as a complete URL, sending a reader
        // after a `cursor` value that was silently dropped.
        assert!(rendered.ends_with("..."), "an elided URL is marked");
    }

    #[test]
    fn untrusted_next_url_display_is_bounded_but_stores_the_url_in_full() {
        let next_url = oversized_url();
        let error = MassiveError::UntrustedNextUrl {
            next_url: next_url.clone(),
            expected_origin: "https://api.massive.com".to_owned(),
        };
        let rendered = error.to_string();

        assert!(rendered.len() < next_url.len(), "Display must be bounded");
        assert!(rendered.contains("https://api.massive.com"));
        assert!(rendered.ends_with("..."), "an elided URL is marked");
        // The field keeps the value exactly as received — only rendering is capped, so a consumer
        // matching on the variant can still inspect the whole URL.
        match error {
            MassiveError::UntrustedNextUrl {
                next_url: stored, ..
            } => {
                assert_eq!(stored, next_url)
            }
            other => panic!("expected UntrustedNextUrl, got {other:?}"),
        }
    }

    #[test]
    fn display_of_a_short_url_is_not_truncated() {
        // The bound must be invisible in the overwhelmingly common case.
        let url = "https://api.massive.com/v3/reference/tickers?cursor=abc";
        let rendered = MassiveError::CyclicPagination {
            url: url.to_owned(),
        }
        .to_string();

        assert!(rendered.ends_with(url), "got: {rendered}");
    }

    #[test]
    fn pagination_url_too_long_display_reports_both_sizes() {
        let rendered = MassiveError::PaginationUrlTooLong {
            len: 9000,
            limit: 8192,
            prefix: "https://api.massive.com/v3/x".to_owned(),
        }
        .to_string();

        assert!(rendered.contains("9000"), "got: {rendered}");
        assert!(rendered.contains("8192"), "got: {rendered}");
        assert!(rendered.contains("https://api.massive.com/v3/x"));
        assert!(
            rendered.ends_with("..."),
            "a prefix shorter than `len` is truncated and must say so, got: {rendered}"
        );
    }

    #[test]
    fn pagination_url_too_long_display_omits_the_ellipsis_when_nothing_was_cut() {
        // `#[non_exhaustive]` is on the enum, not this variant, so downstream code can build one
        // with a complete `prefix`. The ellipsis is derived from `prefix.len() < len` rather than
        // assumed from the internal constructor's behaviour, so a whole URL is not misreported as
        // truncated.
        let url = "https://api.massive.com/v3/x";
        let rendered = MassiveError::PaginationUrlTooLong {
            len: url.len(),
            limit: 8192,
            prefix: url.to_owned(),
        }
        .to_string();

        assert!(rendered.contains(url), "got: {rendered}");
        assert!(
            !rendered.ends_with("..."),
            "an untruncated prefix must not be marked as cut, got: {rendered}"
        );
    }

    #[test]
    fn deserialize_display_truncates_its_payload_snippet() {
        let rendered = MassiveError::Deserialize {
            message: "expected `,`".to_owned(),
            payload: "x".repeat(500),
        }
        .to_string();

        assert!(rendered.contains("expected `,`"));
        assert!(rendered.contains("..."), "an elided payload is marked");
        assert!(rendered.len() < 500);
    }

    #[test]
    fn display_covers_every_variant_without_panicking() {
        // `Display` slices strings by byte index, so a multi-byte payload in any variant is a
        // panic risk; this walks all of them at once.
        let multibyte = "€".repeat(400);
        let variants = [
            MassiveError::RateLimited {
                retry_after: Some(Duration::from_secs(5)),
            },
            MassiveError::RateLimited { retry_after: None },
            MassiveError::Disconnected {
                reason: multibyte.clone(),
            },
            MassiveError::Auth {
                message: multibyte.clone(),
            },
            MassiveError::Api {
                status: 500,
                message: multibyte.clone(),
            },
            MassiveError::Http {
                message: multibyte.clone(),
            },
            MassiveError::Deserialize {
                message: "bad".to_owned(),
                payload: multibyte.clone(),
            },
            MassiveError::EnvVar { var: "MASSIVE_KEY" },
            MassiveError::InvalidInput {
                message: multibyte.clone(),
            },
            MassiveError::PaginationLimitExceeded {
                pages: 10_001,
                limit: 10_000,
            },
            MassiveError::CyclicPagination {
                url: multibyte.clone(),
            },
            MassiveError::PaginationUrlTooLong {
                len: 9000,
                limit: 8192,
                prefix: multibyte.clone(),
            },
            MassiveError::UntrustedNextUrl {
                next_url: multibyte.clone(),
                expected_origin: "https://api.massive.com".to_owned(),
            },
        ];

        for variant in variants {
            assert!(
                !variant.to_string().is_empty(),
                "every variant renders something: {variant:?}"
            );
        }
    }
}
