use crate::engine::audit::state_replica::AUDIT_REPLICA_STATE_UPDATE_SPAN_NAME;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

/// Initialise default non-JSON logging.
///
/// Installs [`AuditSpanFilter`], so a log line a
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
        .with(AuditSpanFilter)
        .init()
}

/// Initialise default JSON logging.
///
/// Installs [`AuditSpanFilter`], so a log line a
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
        .with(AuditSpanFilter)
        .init()
}

/// A [`Layer`](tracing_subscriber::layer::Layer) that drops every event logged while a
/// [`StateReplicaManager`] applies the engine's audit stream to its replica.
///
/// The replica applies each event through the same [`EngineState`] methods as the live `Engine`,
/// so without this filter every log line on that path is written twice: once by the engine, and
/// once by the replica. The replica applies them inside a span named
/// [`AUDIT_REPLICA_STATE_UPDATE_SPAN_NAME`]; this filter drops an event when that span is
/// anywhere in its scope, including under a span entered beneath it.
///
/// [`init_logging`] and [`init_json_logging`] install it. A subscriber built by hand needs it
/// added to see each line once:
///
/// ```
/// use rustrade::logging::AuditSpanFilter;
/// use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
///
/// tracing_subscriber::registry()
///     .with(tracing_subscriber::fmt::layer())
///     .with(AuditSpanFilter)
///     .init();
/// ```
///
/// It filters every layer of the subscriber, not only the ones added before it.
///
/// [`StateReplicaManager`]: crate::engine::audit::state_replica::StateReplicaManager
/// [`EngineState`]: crate::engine::state::EngineState
#[derive(Debug)]
pub struct AuditSpanFilter;

impl<S> tracing_subscriber::layer::Layer<S> for AuditSpanFilter
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    fn event_enabled(
        &self,
        event: &tracing::Event<'_>,
        ctx: tracing_subscriber::layer::Context<'_, S>,
    ) -> bool {
        !ctx.event_scope(event).is_some_and(|mut scope| {
            scope.any(|span| span.name() == AUDIT_REPLICA_STATE_UPDATE_SPAN_NAME)
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
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

    /// How many of the events `f` logs reach a layer behind [`AuditSpanFilter`].
    fn events_kept(f: impl FnOnce()) -> usize {
        let count = Arc::new(AtomicUsize::new(0));
        rustrade_instrument::test_utils::logs::with_layer(
            AuditSpanFilter.and_then(Count(Arc::clone(&count))),
            f,
        );
        count.load(Ordering::Relaxed)
    }

    #[test]
    fn an_event_outside_the_replica_span_is_kept() {
        let kept = events_kept(|| {
            tracing::info!("no span");
            let _other = info_span!("other").entered();
            tracing::info!("another span");
        });
        assert_eq!(kept, 2);
    }

    #[test]
    fn an_event_in_the_replica_span_is_dropped() {
        let kept = events_kept(|| {
            let _replica = info_span!(AUDIT_REPLICA_STATE_UPDATE_SPAN_NAME).entered();
            tracing::warn!("replica");
        });
        assert_eq!(kept, 0);
    }

    /// A span entered beneath the replica's, by the library or by the caller's own code, does not
    /// let the replica's copy through.
    #[test]
    fn an_event_in_a_span_beneath_the_replica_span_is_dropped() {
        let kept = events_kept(|| {
            let _replica = info_span!(AUDIT_REPLICA_STATE_UPDATE_SPAN_NAME).entered();
            let _inner = info_span!("inner").entered();
            tracing::warn!("beneath the replica");
        });
        assert_eq!(kept, 0);
    }

    /// A replica run inside a caller's span is filtered, and the caller's span is not.
    #[test]
    fn only_the_replica_span_is_filtered_within_a_callers_span() {
        let kept = events_kept(|| {
            let _caller = info_span!("caller").entered();
            {
                let _replica = info_span!(AUDIT_REPLICA_STATE_UPDATE_SPAN_NAME).entered();
                tracing::warn!("replica");
            }
            tracing::info!("caller");
        });
        assert_eq!(kept, 1);
    }

    /// An event given the replica span as its explicit parent is dropped, and one given another
    /// parent inside the replica span is kept, as its scope says.
    #[test]
    fn an_events_explicit_parent_decides_its_scope() {
        let kept = events_kept(|| {
            let replica = info_span!(AUDIT_REPLICA_STATE_UPDATE_SPAN_NAME);
            let other = info_span!(parent: None, "other");
            tracing::info!(parent: &replica, "explicitly in the replica");
            let _replica = replica.enter();
            tracing::info!(parent: &other, "explicitly elsewhere");
        });
        assert_eq!(kept, 1);
    }
}
