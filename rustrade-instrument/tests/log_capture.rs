//! The one test in this binary, so no other capture is open beside it: a second registered
//! subscriber would hide the race it checks for.

use rustrade_instrument::test_utils::logs::{Capture, Logged};
use tracing::Level;

fn reach() {
    tracing::warn!(answer = 42, "reached");
}

/// A call site first reached on a thread with no subscriber, while a capture is open on this one,
/// is still captured here. Without the process-wide floor, the other thread's first reach caches
/// the call site as of no interest, and the capture records nothing.
#[test]
fn a_call_site_first_reached_on_another_thread_is_still_captured() {
    let capture = Capture::start();
    std::thread::spawn(reach).join().unwrap();
    reach();

    let events = capture.events();
    assert_eq!(
        events,
        [Logged {
            level: Level::WARN,
            fields: vec![
                ("message", "reached".to_owned()),
                ("answer", "42".to_owned())
            ],
        }]
    );
    assert_eq!(events[0].message(), "reached");
}
