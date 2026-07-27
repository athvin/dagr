//! Compile-fail: a task struct captures a non-`Send` value (an `Rc`) held across
//! the `run` body. A task value is moved onto a worker thread, so it must be
//! `Send`; the natural borrow/`Send` diagnostic reds the build. This snapshot
//! pins that diagnostic verbatim — the concrete example of the "diagnostics may
//! point at the `#[task]` site" limitation, cross-referenced from the cookbook
//! "common mistakes" section.

use std::rc::Rc;

use dagr_core::task;
use dagr_core::task::Task;

/// `Rc<u32>` is `!Send`, so `NonSend` is `!Send`.
struct NonSend {
    shared: Rc<u32>,
}

#[task]
impl NonSend {
    async fn run(&mut self, input: u64) -> Result<u64, dagr_core::TaskError> {
        // Hold the non-`Send` value across the (trivially async) body.
        let held = self.shared.clone();
        Ok(input + u64::from(*held))
    }
}

/// A `Send + 'static` bound stands in for what the framework demands of every
/// task; naming `NonSend` here forces the `!Send` diagnostic at a stable point.
fn assert_send<T: Task + Send + 'static>() {}

fn main() {
    assert_send::<NonSend>();
}
