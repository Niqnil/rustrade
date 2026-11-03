//! Reading back a value saved in a versioned format, so that one saved by another release is
//! refused with the version it was written in rather than with a parse error.

use serde::Deserialize;
use thiserror::Error;

/// A value that carries the version of the format it was written in.
///
/// A field added within a version takes `#[serde(default)]`, so a value written before the field
/// existed still loads in a self-describing format such as JSON. A change an older value cannot
/// be read under bumps [`VERSION`](Self::VERSION). A value holding another versioned value bumps
/// its own version whenever the inner one's rises, so that its version alone says whether it
/// reads.
pub trait FormatVersion {
    /// The format version this release writes, and the only one it reads.
    const VERSION: u32;

    /// The format version this value was written in.
    fn format_version(&self) -> u32;
}

/// A saved `T`, read whatever format version it was written in: see
/// [`into_current`](Self::into_current).
///
/// Deserialising a `T` itself fails in the deserialiser on a value whose shape a later format
/// changed, with an error that does not say why. A `Stored<T>` reads such a value as
/// [`Unread`](Self::Unread), keeping only its top-level `version` field, so the version it was
/// written in can still be reported.
///
/// # Formats
/// Needs a self-describing format, such as JSON, because it tries `T` first and then the
/// `version` field alone, which a format like bincode cannot. In such a format deserialise `T`
/// itself: whatever restores it then checks [`FormatVersion::format_version`].
///
/// It buffers the whole value while it reads it, so reading one takes about twice its memory.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(untagged)]
pub enum Stored<T> {
    /// The value read as a `T`.
    Read(T),
    /// A value that does not read as a `T`, written in format version `version`.
    Unread {
        /// The value's top-level `version` field.
        version: u32,
    },
}

impl<T: FormatVersion> Stored<T> {
    /// The value, if it was written in the format version this release reads.
    ///
    /// # Errors
    /// - [`StoredFormatError::UnsupportedVersion`] if it was written in another version, whether
    ///   or not its shape still reads as a `T`.
    /// - [`StoredFormatError::Malformed`] if it claims this release's version but does not read as
    ///   a `T`, such as a value edited by hand. Deserialise a `T` itself to learn why.
    pub fn into_current(self) -> Result<T, StoredFormatError> {
        let found = match self {
            Self::Read(value) if value.format_version() == T::VERSION => return Ok(value),
            Self::Read(value) => value.format_version(),
            Self::Unread { version } if version == T::VERSION => {
                return Err(StoredFormatError::Malformed { version });
            }
            Self::Unread { version } => version,
        };
        Err(StoredFormatError::UnsupportedVersion {
            found,
            supported: T::VERSION,
        })
    }
}

/// Why a [`Stored`] value is not one this release can use.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum StoredFormatError {
    /// The value was written in a format version this release does not read.
    #[error("saved in format version {found}; this release reads version {supported}")]
    UnsupportedVersion {
        /// The version the value was written in.
        found: u32,
        /// The version this release reads.
        supported: u32,
    },

    /// The value claims the format version this release reads, but does not read in it.
    #[error(
        "saved in format version {version}, which this release reads, but it does not read in \
         that format"
    )]
    Malformed {
        /// The version the value claims, which is this release's.
        version: u32,
    },
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use serde::Serialize;

    #[derive(Debug, PartialEq, Deserialize, Serialize)]
    struct Saved {
        version: u32,
        quantity: u32,
        #[serde(default)]
        note: String,
    }

    impl FormatVersion for Saved {
        const VERSION: u32 = 3;

        fn format_version(&self) -> u32 {
            self.version
        }
    }

    fn read(json: serde_json::Value) -> Result<Saved, StoredFormatError> {
        serde_json::from_value::<Stored<Saved>>(json)
            .unwrap()
            .into_current()
    }

    #[test]
    fn a_value_in_the_current_version_reads_and_one_without_a_later_field_takes_its_default() {
        let saved = Saved {
            version: 3,
            quantity: 7,
            note: "kept".into(),
        };
        assert_eq!(read(serde_json::to_value(&saved).unwrap()), Ok(saved));
        assert_eq!(
            read(serde_json::json!({ "version": 3, "quantity": 7 })),
            Ok(Saved {
                version: 3,
                quantity: 7,
                note: String::new(),
            })
        );
    }

    #[test]
    fn a_value_in_another_version_is_refused_with_it_whatever_its_shape() {
        let unsupported = Err(StoredFormatError::UnsupportedVersion {
            found: 4,
            supported: 3,
        });
        // Still reads as a `Saved`.
        assert_eq!(
            read(serde_json::json!({ "version": 4, "quantity": 7 })),
            unsupported
        );
        // Does not.
        assert_eq!(
            read(serde_json::json!({ "version": 4, "quantity": "seven", "units": [] })),
            unsupported
        );
    }

    #[test]
    fn a_value_claiming_the_current_version_in_another_shape_is_malformed() {
        assert_eq!(
            read(serde_json::json!({ "version": 3, "quantity": "seven" })),
            Err(StoredFormatError::Malformed { version: 3 })
        );
    }

    #[test]
    fn a_value_without_a_version_is_not_read() {
        assert!(
            serde_json::from_value::<Stored<Saved>>(serde_json::json!({ "quantity": 7 })).is_err()
        );
    }
}
