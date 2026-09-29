/// `NoneOneOrMany` enum.
pub mod none_one_or_many;

/// `OneOrMany` enum.
pub mod one_or_many;

/// Serde adapter that writes an `IndexMap` as a sequence of `(key, value)` pairs, for maps with
/// non-string keys.
pub mod pair_seq;

/// `Snapshot<T>` new type wrapper.
pub mod snapshot;

pub type FnvIndexMap<K, V> = indexmap::IndexMap<K, V, fnv::FnvBuildHasher>;
pub type FnvIndexSet<T> = indexmap::IndexSet<T, fnv::FnvBuildHasher>;
