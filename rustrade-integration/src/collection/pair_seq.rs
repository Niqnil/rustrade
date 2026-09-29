//! Serde adapter that writes an [`IndexMap`](indexmap::IndexMap) as a sequence of `(key, value)`
//! pairs.
//!
//! Use it with `#[serde(with = "rustrade_integration::collection::pair_seq")]` on a map field
//! whose key is not a string, such as a struct key. JSON object keys must be strings, so
//! `serde_json` rejects such a map outright when it is serialised as a map. As a sequence of
//! pairs, the map serialises in every format, and JSON reads `[[key, value], …]`.
//!
//! Insertion order is kept in both directions. Deserialising rejects a repeated key rather than
//! letting the later pair overwrite the earlier one.

use indexmap::{IndexMap, map::Entry};
use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{self, SeqAccess, Visitor},
};
use std::{
    fmt,
    hash::{BuildHasher, Hash},
    marker::PhantomData,
};

/// Upper bound on the capacity reserved from a format's size hint, so a hostile length prefix
/// cannot force a large allocation before any element has been read.
const MAX_PREALLOCATE: usize = 4096;

/// Serialise `map` as a sequence of `(key, value)` pairs, in insertion order.
pub fn serialize<K, V, H, S>(map: &IndexMap<K, V, H>, serializer: S) -> Result<S::Ok, S::Error>
where
    K: Serialize,
    V: Serialize,
    S: Serializer,
{
    serializer.collect_seq(map)
}

/// Deserialise a sequence of `(key, value)` pairs into a map, in sequence order.
///
/// # Errors
/// Fails if an element is not a `(key, value)` pair, or if a key repeats an earlier one. The
/// duplicate error names the positions of both elements.
pub fn deserialize<'de, K, V, H, D>(deserializer: D) -> Result<IndexMap<K, V, H>, D::Error>
where
    K: Deserialize<'de> + Eq + Hash,
    V: Deserialize<'de>,
    H: BuildHasher + Default,
    D: Deserializer<'de>,
{
    deserializer.deserialize_seq(PairSeqVisitor(PhantomData))
}

struct PairSeqVisitor<K, V, H>(PhantomData<fn() -> IndexMap<K, V, H>>);

impl<'de, K, V, H> Visitor<'de> for PairSeqVisitor<K, V, H>
where
    K: Deserialize<'de> + Eq + Hash,
    V: Deserialize<'de>,
    H: BuildHasher + Default,
{
    type Value = IndexMap<K, V, H>;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a sequence of (key, value) pairs")
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        let capacity = seq.size_hint().unwrap_or(0).min(MAX_PREALLOCATE);
        let mut map = IndexMap::with_capacity_and_hasher(capacity, H::default());

        while let Some((key, value)) = seq.next_element::<(K, V)>()? {
            // Every earlier pair was inserted, so this pair's position is the map's length.
            let position = map.len();
            match map.entry(key) {
                Entry::Vacant(entry) => {
                    entry.insert(value);
                }
                Entry::Occupied(entry) => {
                    return Err(de::Error::custom(format_args!(
                        "duplicate key: the pair at position {} repeats the key of the pair at \
                         position {}",
                        position,
                        entry.index()
                    )));
                }
            }
        }

        Ok(map)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use crate::collection::FnvIndexMap;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
    struct Key {
        exchange: String,
        name: String,
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Wrapper {
        #[serde(with = "super")]
        map: FnvIndexMap<Key, u32>,
    }

    fn key(exchange: &str, name: &str) -> Key {
        Key {
            exchange: exchange.to_owned(),
            name: name.to_owned(),
        }
    }

    #[test]
    fn test_struct_keyed_map_round_trips_through_json_in_insertion_order() {
        let mut map = FnvIndexMap::default();
        map.insert(key("kraken", "usdt"), 3);
        map.insert(key("binance_spot", "btc"), 1);
        map.insert(key("binance_spot", "usdt"), 2);
        let wrapper = Wrapper { map };

        let json = serde_json::to_string(&wrapper).unwrap();
        assert_eq!(
            json,
            r#"{"map":[[{"exchange":"kraken","name":"usdt"},3],[{"exchange":"binance_spot","name":"btc"},1],[{"exchange":"binance_spot","name":"usdt"},2]]}"#
        );

        let back: Wrapper = serde_json::from_str(&json).unwrap();
        assert_eq!(back, wrapper);
        assert!(back.map.keys().eq(wrapper.map.keys()));
    }

    #[test]
    fn test_empty_map_round_trips() {
        let wrapper = Wrapper {
            map: FnvIndexMap::default(),
        };
        let json = serde_json::to_string(&wrapper).unwrap();
        assert_eq!(json, r#"{"map":[]}"#);
        assert_eq!(serde_json::from_str::<Wrapper>(&json).unwrap(), wrapper);
    }

    #[test]
    fn test_duplicate_key_is_rejected_naming_both_positions() {
        let json = r#"{"map":[
            [{"exchange":"binance_spot","name":"btc"},1],
            [{"exchange":"binance_spot","name":"usdt"},2],
            [{"exchange":"binance_spot","name":"btc"},3]
        ]}"#;

        let err = serde_json::from_str::<Wrapper>(json)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains(
                "duplicate key: the pair at position 2 repeats the key of the pair at position 0"
            ),
            "{err}"
        );
    }

    #[test]
    fn test_element_that_is_not_a_pair_is_rejected() {
        let json = r#"{"map":[[{"exchange":"binance_spot","name":"btc"}]]}"#;
        assert!(serde_json::from_str::<Wrapper>(json).is_err());

        let json = r#"{"map":{"a":1}}"#;
        assert!(serde_json::from_str::<Wrapper>(json).is_err());
    }
}
