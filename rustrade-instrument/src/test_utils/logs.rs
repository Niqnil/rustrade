//! Capturing what code under test logs on the test's own thread.
//!
//! A thread-local subscriber (`tracing::subscriber::with_default`) alone does not reliably see
//! the events it should. `tracing` caches whether a call site is of interest the first time any
//! thread reaches it. While only one subscriber is registered, it asks just the subscriber of
//! the thread that got there first. When that is a parallel test's thread with no subscriber, the
//! call site is cached as of no interest, and a test capturing it on its own thread misses the
//! event.
//!
//! So before capturing, every function here installs, once per process, a global default
//! subscriber that answers "sometimes" for every call site and enables none. Interests from
//! different subscribers combine to "sometimes", which leaves the decision to each event's own
//! thread, so no call site can be cached as of no interest. A test binary that installs its own
//! global default first keeps it, and keeps this guarantee only if that subscriber is interested
//! in every call site.

use std::{
    fmt,
    sync::{Arc, Mutex, MutexGuard, Once, PoisonError},
};
use tracing::{
    Event, Level, Metadata, Subscriber,
    field::{Field, Visit},
    span,
    subscriber::{DefaultGuard, Interest},
};
use tracing_subscriber::{
    Layer, Registry,
    layer::{Context, SubscriberExt},
};

/// One event logged on the capturing thread.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Logged {
    /// The event's level.
    pub level: Level,
    /// Every field in the order it was recorded, the message as `message`, each value rendered
    /// with `Debug`.
    pub fields: Vec<(&'static str, String)>,
}

impl Logged {
    /// The event's message, or `""` if it has none.
    pub fn message(&self) -> &str {
        self.fields
            .iter()
            .find(|(name, _)| *name == "message")
            .map_or("", |(_, value)| value)
    }
}

/// Run `f`, returning its value and every event it logged on this thread.
pub fn capture<T>(f: impl FnOnce() -> T) -> (T, Vec<Logged>) {
    let capture = Capture::start();
    let value = f();
    (value, capture.events())
}

/// Run `f` with `layer` seeing every event it logs on this thread, returning its value. For a
/// test that must act when an event is logged, rather than read the events afterwards.
pub fn with_layer<L, T>(layer: L, f: impl FnOnce() -> T) -> T
where
    L: Layer<Registry> + Send + Sync + 'static,
{
    install_floor();
    tracing::subscriber::with_default(tracing_subscriber::registry().with(layer), f)
}

/// Records every event logged on this thread from [`Capture::start`] until it is dropped.
///
/// For an async test, whose futures a current-thread runtime polls on the test's thread. An event
/// logged on another thread, such as a `spawn_blocking` task's or a multi-thread runtime
/// worker's, is not recorded.
#[derive(Debug)]
#[must_use = "events are recorded only while the capture is held"]
pub struct Capture {
    events: Arc<Mutex<Vec<Logged>>>,
    _guard: DefaultGuard,
}

impl Capture {
    /// Starts recording this thread's events, until the capture is dropped.
    pub fn start() -> Self {
        install_floor();
        let events = Arc::default();
        let guard = tracing::subscriber::set_default(
            tracing_subscriber::registry().with(Record(Arc::clone(&events))),
        );
        Self {
            events,
            _guard: guard,
        }
    }

    /// The events recorded so far, in the order they were logged.
    pub fn events(&self) -> Vec<Logged> {
        lock(&self.events).clone()
    }
}

struct Record(Arc<Mutex<Vec<Logged>>>);

impl<S: Subscriber> Layer<S> for Record {
    fn on_event(&self, event: &Event<'_>, _: Context<'_, S>) {
        let mut fields = Fields(Vec::new());
        event.record(&mut fields);
        lock(&self.0).push(Logged {
            level: *event.metadata().level(),
            fields: fields.0,
        });
    }
}

/// The lock's events, even if a thread panicked holding it: each push completes or never starts.
fn lock(events: &Mutex<Vec<Logged>>) -> MutexGuard<'_, Vec<Logged>> {
    events.lock().unwrap_or_else(PoisonError::into_inner)
}

struct Fields(Vec<(&'static str, String)>);

impl Visit for Fields {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.0.push((field.name(), format!("{value:?}")));
    }
}

/// Installs [`Floor`] as the global default, once per process.
fn install_floor() {
    static FLOOR: Once = Once::new();
    FLOOR.call_once(|| {
        // Fails only when the binary set a global default first, which then stays (module docs).
        let _ = tracing::subscriber::set_global_default(Floor);
    });
}

/// Interested "sometimes" in every call site, so no call site's cached interest is "never";
/// enables nothing, so a thread with no subscriber of its own logs nothing.
struct Floor;

impl Subscriber for Floor {
    fn register_callsite(&self, _: &'static Metadata<'static>) -> Interest {
        Interest::sometimes()
    }

    fn enabled(&self, _: &Metadata<'_>) -> bool {
        false
    }

    fn new_span(&self, _: &span::Attributes<'_>) -> span::Id {
        // Never called: a span is created only when `enabled` is true.
        span::Id::from_u64(1)
    }

    fn record(&self, _: &span::Id, _: &span::Record<'_>) {}

    fn record_follows_from(&self, _: &span::Id, _: &span::Id) {}

    fn event(&self, _: &Event<'_>) {}

    fn enter(&self, _: &span::Id) {}

    fn exit(&self, _: &span::Id) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `capture` records only the closure's events, and `with_layer` hands each to the layer.
    #[test]
    fn capture_and_with_layer_see_the_closures_events() {
        let (value, events) = capture(|| {
            tracing::info!("inside");
            7
        });
        tracing::info!("outside");
        assert_eq!(value, 7);
        assert_eq!(
            events.iter().map(Logged::message).collect::<Vec<_>>(),
            ["inside"]
        );

        let events = Arc::default();
        with_layer(Record(Arc::clone(&events)), || tracing::error!("seen"));
        let events = lock(&events);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].level, Level::ERROR);
    }

    /// Outside a capture, a thread with no subscriber of its own enables nothing, once the
    /// process-wide subscriber is installed.
    #[test]
    fn a_thread_without_a_capture_enables_nothing() {
        let _ = capture(|| ());
        std::thread::spawn(|| assert!(!tracing::enabled!(Level::ERROR)))
            .join()
            .unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    }
}
