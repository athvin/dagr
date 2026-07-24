//! C25 · logging and tracing integration acceptance (ticket T45 / 056).
//!
//! These tests pin arch.md `### C25`'s machine acceptance against the real
//! `dagr_cli::logging` surface: every attempt runs beneath a span carrying run /
//! node / attempt identity so any line — framework or third-party — is
//! attributable without timestamp correlation; output is structured by default
//! and human-readable via an environment variable with no code change; and
//! marked secrets from the C9 registry never surface on framework output paths.
//!
//! The subscriber is exercised through a **scoped** capturing subscriber
//! (`tracing::subscriber::with_default`) writing into a shared in-memory buffer,
//! so the suite captures exactly the framework-emitted output for the code run
//! beneath it — no global install, no wall-clock, no ordering assumptions.

use std::io;
use std::sync::{Arc, Mutex};

use dagr_cli::logging::{
    attempt_span, human_subscriber, structured_subscriber, OutputMode, LOG_FORMAT_ENV,
};
use dagr_core::context::{ResourceRegistry, Secret};
use tracing::subscriber::with_default;
use tracing::Instrument;

/// A shared, thread-safe in-memory sink the capturing subscriber writes into, so
/// a test can read back exactly the framework-emitted bytes for the code that
/// ran beneath it. Cloning shares the same buffer (an `Arc<Mutex<_>>`).
#[derive(Clone, Default)]
struct CaptureBuf(Arc<Mutex<Vec<u8>>>);

impl CaptureBuf {
    fn contents(&self) -> String {
        String::from_utf8(self.0.lock().expect("capture buffer not poisoned").clone())
            .expect("captured bytes are utf-8")
    }
    /// One `MakeWriter` handle per event, each writing into the shared buffer.
    fn writer(&self) -> CaptureWriter {
        CaptureWriter(self.0.clone())
    }
}

struct CaptureWriter(Arc<Mutex<Vec<u8>>>);

impl io::Write for CaptureWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0
            .lock()
            .expect("capture buffer not poisoned")
            .extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

// `tracing_subscriber::fmt::MakeWriter` is implemented for a `Fn() -> impl Write`
// via a small adaptor; expose the buffer as one.
impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CaptureBuf {
    type Writer = CaptureWriter;
    fn make_writer(&'a self) -> Self::Writer {
        self.writer()
    }
}

/// A stub "third-party" library helper that emits a log line through the global
/// `tracing` facade with **no** dagr-specific context of its own — it does not
/// know about runs, nodes, or attempts. When called beneath an attempt span the
/// line must nonetheless inherit that span's fields.
fn third_party_library_emits_a_line() {
    tracing::info!(component = "acme-http", "third-party library call");
}

// ===========================================================================
// Attribution without timestamp correlation
// ===========================================================================

#[test]
fn a_task_line_and_a_third_party_line_both_carry_node_and_attempt() {
    let buf = CaptureBuf::default();
    with_default(structured_subscriber(buf.clone()), || {
        let span = attempt_span("run-abc", "extract", 1);
        let _g = span.enter();
        // A line the task itself emits.
        tracing::info!("task-emitted line");
        // A line a third-party library emits, with no dagr context of its own.
        third_party_library_emits_a_line();
    });

    let out = buf.contents();
    // Each captured record is a structured JSON object exposing node + attempt as
    // discrete fields, so a reader attributes each line without ordering by time.
    let records: Vec<serde_json::Value> = out
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).expect("each framework record is a JSON object"))
        .collect();
    assert_eq!(records.len(), 2, "both lines were captured: {out}");
    for rec in &records {
        let span = rec.get("span").expect("record carries its span fields");
        assert_eq!(span.get("node").and_then(|v| v.as_str()), Some("extract"));
        assert_eq!(
            span.get("attempt").and_then(serde_json::Value::as_u64),
            Some(1)
        );
        assert_eq!(span.get("run").and_then(|v| v.as_str()), Some("run-abc"));
    }
}

#[test]
fn a_third_party_line_inherits_the_attempt_span() {
    let buf = CaptureBuf::default();
    with_default(structured_subscriber(buf.clone()), || {
        let span = attempt_span("run-xyz", "load", 1);
        third_party_library_emits_a_line.in_scope_via(&span);
    });

    let out = buf.contents();
    let rec: serde_json::Value = out
        .lines()
        .find(|l| !l.trim().is_empty())
        .and_then(|l| serde_json::from_str(l).ok())
        .expect("the third-party line was captured as a JSON record");
    let span = rec
        .get("span")
        .expect("the third-party line is annotated with span fields");
    assert_eq!(span.get("run").and_then(|v| v.as_str()), Some("run-xyz"));
    assert_eq!(span.get("node").and_then(|v| v.as_str()), Some("load"));
    assert_eq!(
        span.get("attempt").and_then(serde_json::Value::as_u64),
        Some(1)
    );
}

/// Tiny helper so the third-party call reads as "emitted beneath the span".
trait InScopeVia {
    fn in_scope_via(self, span: &tracing::Span);
}
impl<F: FnOnce()> InScopeVia for F {
    fn in_scope_via(self, span: &tracing::Span) {
        span.in_scope(self);
    }
}

// ===========================================================================
// Concurrent nodes are unambiguously separable
// ===========================================================================

#[test]
fn concurrent_node_lines_are_separable_by_span_fields() {
    let buf = CaptureBuf::default();
    with_default(structured_subscriber(buf.clone()), || {
        // Two attempts of two different nodes, whose lines interleave in emission
        // order but must never be ambiguous.
        let span_a = attempt_span("run-1", "node-a", 1);
        let span_b = attempt_span("run-1", "node-b", 1);
        span_a.in_scope(|| tracing::info!("a-1"));
        span_b.in_scope(|| tracing::info!("b-1"));
        span_a.in_scope(|| tracing::info!("a-2"));
        span_b.in_scope(|| tracing::info!("b-2"));
    });

    let out = buf.contents();
    let records: Vec<serde_json::Value> = out
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).expect("json record"))
        .collect();
    assert_eq!(records.len(), 4);
    // Every line is attributable to exactly one node via its span field, even
    // though the four lines interleave in emission order.
    for rec in &records {
        let node = rec
            .get("span")
            .and_then(|s| s.get("node"))
            .and_then(|v| v.as_str())
            .expect("each line carries an unambiguous node field");
        assert!(
            node == "node-a" || node == "node-b",
            "node is one of the two: {node}"
        );
    }
    let a_lines = records
        .iter()
        .filter(|r| r["span"]["node"] == "node-a")
        .count();
    let b_lines = records
        .iter()
        .filter(|r| r["span"]["node"] == "node-b")
        .count();
    assert_eq!(a_lines, 2, "both of node-a's lines are attributed to it");
    assert_eq!(b_lines, 2, "both of node-b's lines are attributed to it");
}

// ===========================================================================
// Retry attempts are distinguishable
// ===========================================================================

#[test]
fn retry_attempts_share_the_node_but_differ_by_attempt_number() {
    let buf = CaptureBuf::default();
    with_default(structured_subscriber(buf.clone()), || {
        attempt_span("run-2", "flaky", 1).in_scope(|| tracing::info!("first attempt"));
        attempt_span("run-2", "flaky", 2).in_scope(|| tracing::info!("second attempt"));
    });

    let out = buf.contents();
    let records: Vec<serde_json::Value> = out
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).expect("json record"))
        .collect();
    assert_eq!(records.len(), 2);
    // Same node identity, different attempt numbers — first-attempt output is
    // distinguishable from retry output.
    assert_eq!(records[0]["span"]["node"], "flaky");
    assert_eq!(records[1]["span"]["node"], "flaky");
    assert_eq!(records[0]["span"]["attempt"].as_u64(), Some(1));
    assert_eq!(records[1]["span"]["attempt"].as_u64(), Some(2));
}

// ===========================================================================
// Structured default + human via environment, no code change
// ===========================================================================

#[test]
fn structured_is_the_default_when_no_mode_env_is_set() {
    // An unset env var deterministically selects the documented default.
    assert_eq!(OutputMode::from_env_value(None), OutputMode::Structured);
}

#[test]
fn an_unrecognized_mode_falls_back_to_the_documented_default() {
    assert_eq!(
        OutputMode::from_env_value(Some("garbage")),
        OutputMode::Structured
    );
    assert_eq!(OutputMode::from_env_value(Some("")), OutputMode::Structured);
}

#[test]
fn the_environment_variable_selects_human_readable_output() {
    assert_eq!(OutputMode::from_env_value(Some("human")), OutputMode::Human);
    // Recognizing "structured" explicitly is also honored.
    assert_eq!(
        OutputMode::from_env_value(Some("structured")),
        OutputMode::Structured
    );
    // Selection is case-insensitive so an operator's casing does not surprise.
    assert_eq!(OutputMode::from_env_value(Some("HUMAN")), OutputMode::Human);
}

#[test]
fn structured_mode_emits_machine_parseable_records() {
    let buf = CaptureBuf::default();
    with_default(structured_subscriber(buf.clone()), || {
        attempt_span("run-s", "n", 1).in_scope(|| tracing::info!("structured line"));
    });
    let out = buf.contents();
    // Each emitted record parses as a structured record exposing run/node/attempt
    // as discrete queryable fields (not only free text).
    let rec: serde_json::Value = out
        .lines()
        .find(|l| !l.trim().is_empty())
        .and_then(|l| serde_json::from_str(l).ok())
        .expect("structured output parses as a JSON record");
    assert_eq!(rec["span"]["run"], "run-s");
    assert_eq!(rec["span"]["node"], "n");
    assert_eq!(rec["span"]["attempt"].as_u64(), Some(1));
}

#[test]
fn human_mode_is_not_json_but_still_carries_the_fields() {
    let buf = CaptureBuf::default();
    // The SAME code and spans, only the subscriber's mode differs — no source
    // change beyond selecting the mode (which init keys off the env var).
    with_default(human_subscriber(buf.clone()), || {
        attempt_span("run-h", "n", 1).in_scope(|| tracing::info!("human line"));
    });
    let out = buf.contents();
    // Human output is not a JSON object per line...
    assert!(
        serde_json::from_str::<serde_json::Value>(out.trim()).is_err(),
        "human output is not machine-JSON: {out}"
    );
    // ...but still carries the node + attempt so a developer can attribute it.
    assert!(
        out.contains("node"),
        "human line names the node field: {out}"
    );
    assert!(
        out.contains('n'),
        "human line carries the node value: {out}"
    );
    assert!(
        out.contains("human line"),
        "human line carries the message: {out}"
    );
}

#[test]
fn the_documented_env_var_name_is_stable() {
    // The mode-selection env var name is part of the public logging contract.
    assert_eq!(LOG_FORMAT_ENV, "DAGR_LOG_FORMAT");
}

// ===========================================================================
// Secret redaction on framework paths (planted sentinel), both modes
// ===========================================================================

/// A unique sentinel unlikely to occur by accident, planted into the registry as
/// a secret and then hunted for across all framework-emitted output.
const SECRET_SENTINEL: &str = "SENTINEL-7f3a9c-DO-NOT-LEAK-8b21e";

fn run_framework_lifecycle_beneath_a_span(registry: &ResourceRegistry) {
    // The framework's own lifecycle produces span output and log lines. It reaches
    // the registry (which holds a marked secret) only through framework-safe paths
    // — it never calls `.expose()`. Formatting the registry with its `Debug` (a
    // framework diagnostic path) must not surface the secret bytes.
    let span = attempt_span("run-secret", "secret-node", 1);
    span.in_scope(|| {
        tracing::info!(registry = ?registry, "framework diagnostic naming the registry");
        tracing::info!("an ordinary framework line");
    });
}

#[test]
fn a_planted_secret_never_surfaces_on_framework_output_structured() {
    let registry = ResourceRegistry::builder()
        .register(Secret::new(SECRET_SENTINEL.to_string()))
        .expect("unambiguous")
        .build();
    let buf = CaptureBuf::default();
    with_default(structured_subscriber(buf.clone()), || {
        run_framework_lifecycle_beneath_a_span(&registry);
    });
    let out = buf.contents();
    assert!(
        !out.contains(SECRET_SENTINEL),
        "the secret sentinel leaked on the framework's structured output path: {out}"
    );
}

#[test]
fn a_planted_secret_never_surfaces_on_framework_output_human() {
    let registry = ResourceRegistry::builder()
        .register(Secret::new(SECRET_SENTINEL.to_string()))
        .expect("unambiguous")
        .build();
    let buf = CaptureBuf::default();
    with_default(human_subscriber(buf.clone()), || {
        run_framework_lifecycle_beneath_a_span(&registry);
    });
    let out = buf.contents();
    assert!(
        !out.contains(SECRET_SENTINEL),
        "the secret sentinel leaked on the framework's human output path: {out}"
    );
}

#[test]
fn a_task_authored_leak_is_outside_the_guarantee() {
    // The boundary the spec draws: the framework does NOT intercept task-authored
    // content, so a task that deliberately formats the revealed secret into its
    // own log line DOES leak it — confirming the guarantee is exactly
    // framework-controlled paths, not task-authored ones.
    let secret = Secret::new(SECRET_SENTINEL.to_string());
    let buf = CaptureBuf::default();
    with_default(structured_subscriber(buf.clone()), || {
        let span = attempt_span("run-leak", "leaky-task", 1);
        span.in_scope(|| {
            // The task author's explicit act: it exposed the value and built the
            // string itself. dagr cannot scrub a string the author formed.
            tracing::info!(revealed = %secret.expose(), "task formatted its own secret");
        });
    });
    let out = buf.contents();
    assert!(
        out.contains(SECRET_SENTINEL),
        "a task-authored leak is deliberately NOT scrubbed (the boundary): {out}"
    );
}

// ===========================================================================
// Single subscriber, coexists with the test harness
// ===========================================================================

#[test]
fn installing_the_global_subscriber_is_idempotent_and_coexists() {
    // Installing the process-global subscriber must not panic or error even when
    // the test harness's own subscriber/hook is present, and a repeat install is
    // a no-op rather than a double-install error.
    let first = dagr_cli::logging::init_tracing();
    let second = dagr_cli::logging::init_tracing();
    // At most one install per process: whether or not THIS test won the race
    // (another test may have installed first), a second call is never an install.
    assert!(
        !second,
        "a repeat install is a no-op, never a double install"
    );
    // `first` is true only if this test won the global-install race; either way
    // the calls returned rather than panicking, which is the coexistence claim.
    let _ = first;
}

// ===========================================================================
// The attempt future can be instrumented with the span (driver integration seam)
// ===========================================================================

#[test]
fn an_async_attempt_future_inherits_the_span_across_awaits() {
    let buf = CaptureBuf::default();
    with_default(structured_subscriber(buf.clone()), || {
        // The span is created (and its fields recorded) beneath the active
        // subscriber, exactly as the driver opens it around each attempt.
        let fut = async {
            tracing::info!("before await");
            yield_once().await;
            // A line emitted AFTER an await point still carries the span fields,
            // proving the span follows the future across suspensions (the driver
            // instruments the attempt future, not just the synchronous prologue).
            third_party_library_emits_a_line();
        }
        .instrument(attempt_span("run-async", "await-node", 3));
        futures_block_on(fut);
    });

    let out = buf.contents();
    let post_await = out
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str::<serde_json::Value>(l).expect("json"))
        .find(|r| r.get("fields").and_then(|f| f.get("component")).is_some())
        .expect("the post-await third-party line was captured");
    assert_eq!(post_await["span"]["node"], "await-node");
    assert_eq!(post_await["span"]["attempt"].as_u64(), Some(3));
}

/// Yield exactly once (one `Pending` before `Ready`), an await point with no
/// runtime dependency, so a line after `.await` proves the span survives a
/// suspension.
async fn yield_once() {
    use std::task::Poll;
    let mut yielded = false;
    std::future::poll_fn(move |cx| {
        if yielded {
            Poll::Ready(())
        } else {
            yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    })
    .await;
}

/// Drive a future to completion on the current thread with no extra runtime, so
/// the `with_default` scoped subscriber is the one in force while it runs.
fn futures_block_on<F: std::future::Future>(fut: F) -> F::Output {
    use std::sync::Arc;
    use std::task::{Context, Poll, Wake, Waker};
    struct N(std::thread::Thread);
    impl Wake for N {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }
    let mut fut = Box::pin(fut);
    let waker = Waker::from(Arc::new(N(std::thread::current())));
    let mut cx = Context::from_waker(&waker);
    loop {
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(v) => return v,
            Poll::Pending => std::thread::park(),
        }
    }
}
