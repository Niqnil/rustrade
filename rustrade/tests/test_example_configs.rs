#![allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics acceptable

//! Every shipped example config must still deserialize and index.
//!
//! # Why this exists
//!
//! The configs under `examples/config/` are the copy-paste starting point for anyone adopting this
//! library, so a config that no longer parses is a broken first impression rather than a broken
//! test. They are not otherwise protected: examples are compiled by `cargo check --all-targets` but
//! never *run*, so a config only reaches a parser when some test happens to load it. Two of the
//! three were incidentally covered that way; `lse_backtest_config.json` was covered by nothing, and
//! any rename of a serde field, `ExchangeId` variant or `InstrumentKind` tag would have landed
//! silently.
//!
//! This sweeps the directory rather than naming files, so a config added tomorrow is covered
//! without anyone remembering to extend this list.
//!
//! # It indexes, not merely parses
//!
//! Deserializing proves the JSON matches the structs. Building [`IndexedInstruments`] proves the
//! result is a *usable* instrument set — unique internal names, resolvable assets — which is the
//! next thing every example does with it and the check that a naming change would actually trip.

use std::{
    fs,
    path::{Path, PathBuf},
};

use rustrade::system::config::SystemConfig;
use rustrade_instrument::index::IndexedInstruments;
use serde::Deserialize;
use serde_json::Value;

const CONFIG_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/examples/config");

/// The nested shape: `{ risk_free_return, system: { instruments, executions } }`.
///
/// Used by the backtest examples, which need a risk-free return alongside the system itself.
#[derive(Deserialize)]
struct NestedConfig {
    // `risk_free_return` is also present in the JSON but irrelevant here; serde ignores it.
    system: SystemConfig,
}

fn config_paths() -> Vec<PathBuf> {
    let mut paths = fs::read_dir(CONFIG_DIR)
        .expect("the example config directory must exist")
        .map(|entry| entry.expect("a readable directory entry").path())
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "json")
        })
        .collect::<Vec<_>>();

    // `read_dir` order is filesystem-dependent; sort so a failure names the same file every run.
    paths.sort();
    paths
}

/// Deserialize one config, whichever of the two documented shapes it uses.
///
/// Dispatching on the `system` key rather than an untagged enum is deliberate: untagged variants
/// collapse every underlying error into "data did not match any variant", which would turn a
/// one-field typo into an unactionable failure.
fn parse(path: &Path) -> SystemConfig {
    let raw = fs::read_to_string(path).expect("a readable config");
    let display = path.display();

    let value = serde_json::from_str::<Value>(&raw)
        .unwrap_or_else(|error| panic!("{display} is not valid JSON: {error}"));

    if value.get("system").is_some() {
        serde_json::from_value::<NestedConfig>(value)
            .unwrap_or_else(|error| {
                panic!("{display} must deserialize as a system config: {error}")
            })
            .system
    } else {
        serde_json::from_value::<SystemConfig>(value)
            .unwrap_or_else(|error| panic!("{display} must deserialize as a SystemConfig: {error}"))
    }
}

#[test]
fn every_example_config_deserializes_and_indexes() {
    let paths = config_paths();
    assert!(
        !paths.is_empty(),
        "no configs found under {CONFIG_DIR}; this test would pass while checking nothing"
    );

    for path in &paths {
        let config = parse(path);
        let display = path.display();

        assert!(
            !config.instruments.is_empty(),
            "{display} declares no instruments; an example that indexes nothing teaches nothing"
        );

        IndexedInstruments::try_new(config.instruments)
            .unwrap_or_else(|error| panic!("{display} deserializes but does not index: {error}"));
    }
}
