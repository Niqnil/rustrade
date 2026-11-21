use crate::engine::audit::state_replica::updating_replica;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

/// Initialise default non-JSON logging.
///
/// Installs [`ReplicaLogFilter`], so a log line a
/// [`StateReplicaManager`](crate::engine::audit::state_replica::StateReplicaManager) repeats while
/// updating its replica `EngineState` is written once.
pub fn init_logging() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::filter::EnvFilter::builder()
                .with_default_directive(tracing_subscriber::filter::LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .with(tracing_subscriber::fmt::layer())
        .with(ReplicaLogFilter)
        .init()
}

/// Initialise default JSON logging.
///
/// Installs [`ReplicaLogFilter`], so a log line a
/// [`StateReplicaManager`](crate::engine::audit::state_replica::StateReplicaManager) repeats while
/// updating its replica `EngineState` is written once.
pub fn init_json_logging() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::filter::EnvFilter::builder()
                .with_default_directive(tracing_subscriber::filter::LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .with(tracing_subscriber::fmt::layer().json().flatten_event(true))
        .with(ReplicaLogFilter)
        .init()
}

/// A [`Layer`](tracing_subscriber::layer::Layer) that drops every event logged while a
/// [`StateReplicaManager`] applies the engine's audit stream to its replica.
///
/// The replica applies each event through the same [`EngineState`] methods as the live `Engine`,
/// so without this filter every log line on that path is written twice: once by the engine, and
/// once by the replica. [`StateReplicaManager::update_from_event`] marks its thread while it
/// applies an event, and this filter drops the events logged on that thread meanwhile. Unlike a
/// span, the mark holds whatever the subscriber's level or target filters enable.
///
/// [`init_logging`] and [`init_json_logging`] install it. A subscriber built by hand needs it
/// added to see each line once:
///
/// ```
/// use rustrade::logging::ReplicaLogFilter;
/// use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
///
/// tracing_subscriber::registry()
///     .with(tracing_subscriber::fmt::layer())
///     .with(ReplicaLogFilter)
///     .init();
/// ```
///
/// It filters every layer of the subscriber, not only the ones added before it.
///
/// [`StateReplicaManager`]: crate::engine::audit::state_replica::StateReplicaManager
/// [`StateReplicaManager::update_from_event`]: crate::engine::audit::state_replica::StateReplicaManager::update_from_event
/// [`EngineState`]: crate::engine::state::EngineState
#[derive(Debug)]
pub struct ReplicaLogFilter;

impl<S: tracing::Subscriber> tracing_subscriber::layer::Layer<S> for ReplicaLogFilter {
    fn event_enabled(
        &self,
        _: &tracing::Event<'_>,
        _: tracing_subscriber::layer::Context<'_, S>,
    ) -> bool {
        !updating_replica()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use crate::engine::audit::state_replica::{
        AUDIT_REPLICA_STATE_UPDATE_SPAN_NAME, UpdatingReplica,
    };
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use tracing::info_span;
    use tracing_subscriber::Layer;

    /// Counts the events the subscriber dispatches to it.
    struct Count(Arc<AtomicUsize>);

    impl<S: tracing::Subscriber> Layer<S> for Count {
        fn on_event(&self, _: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// How many of the events `f` logs reach a layer behind [`ReplicaLogFilter`].
    fn events_kept(f: impl FnOnce()) -> usize {
        let count = Arc::new(AtomicUsize::new(0));
        rustrade_instrument::test_utils::logs::with_layer(
            ReplicaLogFilter.and_then(Count(Arc::clone(&count))),
            f,
        );
        count.load(Ordering::Relaxed)
    }

    /// Outside a replica update, events are kept, in a span or not, including one with the
    /// replica span's name: the span alone marks nothing.
    #[test]
    fn an_event_outside_a_replica_update_is_kept() {
        let kept = events_kept(|| {
            tracing::info!("no span");
            let _other = info_span!("other").entered();
            tracing::info!("another span");
            let _named = info_span!(AUDIT_REPLICA_STATE_UPDATE_SPAN_NAME).entered();
            tracing::info!("the replica span's name, outside an update");
        });
        assert_eq!(kept, 3);
    }

    /// During a replica update every event is dropped, under any span.
    #[test]
    fn an_event_during_a_replica_update_is_dropped() {
        let kept = events_kept(|| {
            let _updating = UpdatingReplica::enter();
            tracing::warn!("replica");
            let _inner = info_span!("inner").entered();
            tracing::warn!("beneath another span");
        });
        assert_eq!(kept, 0);
    }

    /// The mark ends with its guard, and a nested guard restores the outer mark rather than
    /// clearing it.
    #[test]
    fn the_mark_ends_with_its_guard_and_nests() {
        let kept = events_kept(|| {
            {
                let _outer = UpdatingReplica::enter();
                {
                    let _inner = UpdatingReplica::enter();
                }
                tracing::warn!("still in the outer update");
            }
            tracing::info!("after the update");
        });
        assert_eq!(kept, 1);
    }

    /// A panic during an update clears the mark as it unwinds.
    #[test]
    fn the_mark_is_cleared_on_unwind() {
        let unwound = std::panic::catch_unwind(|| {
            let _updating = UpdatingReplica::enter();
            panic!("during an update");
        });
        assert!(unwound.is_err());
        assert!(!updating_replica());
    }

    /// The mark is per thread: another thread logging during an update is kept.
    #[test]
    fn the_mark_is_per_thread() {
        let _updating = UpdatingReplica::enter();
        let elsewhere = std::thread::spawn(updating_replica).join().unwrap();
        assert!(updating_replica());
        assert!(!elsewhere);
    }
}
