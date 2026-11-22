//! [`init_logging`] installs the global subscriber it documents.
//!
//! It sets process-global state, so it has this test binary to itself, and
//! [`init_json_logging`](rustrade::logging::init_json_logging) has its own in
//! `test_logging_init_json.rs`.

use rustrade::logging::{ReplicaLogFilter, init_logging};
use tracing_subscriber::{
    filter::{EnvFilter, LevelFilter},
    fmt::format::{DefaultFields, Format, JsonFields},
};

#[test]
fn init_logging_installs_a_text_subscriber_with_the_replica_filter() {
    assert!(!tracing::dispatcher::has_been_set());

    init_logging();

    assert!(tracing::dispatcher::has_been_set());
    tracing::dispatcher::get_default(|dispatch| {
        assert!(dispatch.downcast_ref::<ReplicaLogFilter>().is_some());
        assert!(dispatch.downcast_ref::<EnvFilter>().is_some());
        assert!(dispatch.downcast_ref::<Format>().is_some());
        assert!(dispatch.downcast_ref::<DefaultFields>().is_some());
        assert!(dispatch.downcast_ref::<JsonFields>().is_none());
    });
    // The default level applies only when `RUST_LOG` does not override it.
    if std::env::var_os("RUST_LOG").is_none() {
        assert_eq!(LevelFilter::current(), LevelFilter::INFO);
    }

    // A second call finds the global subscriber set, and panics as documented.
    assert!(std::panic::catch_unwind(init_logging).is_err());
}
