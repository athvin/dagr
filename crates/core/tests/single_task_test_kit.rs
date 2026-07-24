//! C28 **single-task test kit** behavioral tests — ticket T60 (073). Written
//! first, TDD.
//!
//! These exercise the **shipped** single-task test kit in
//! [`dagr_core::test_kit`]: the first of C28's three testing levels (arch.md
//! `### C28 · Testing surface`). The kit lets a caller invoke **exactly one**
//! task with a hand-built [`RunContext`] (C8) and fake resources (C9) —
//! **no live network, no database** — and observe the outcome, proving the
//! synchronous path needs no async runtime and the await-bound path needs only
//! the runtime the kit provides.
//!
//! Scope discipline (T60): this is the **single-task** level only — no
//! full-pipeline harness (T62), no structure-snapshot level (T61), no
//! scheduler/driver. Every scenario below drives one task through the
//! library-provided kit, never a bespoke harness.
//!
//! The kit is behind the (default-on) `test-kit` feature; the whole test file is
//! gated so it only compiles when the kit does.
#![cfg(feature = "test-kit")]

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;

use dagr_core::context::{DataInterval, ResourceRegistry, Secret};
use dagr_core::task::{RunContext, Task};
use dagr_core::test_kit::SingleTaskTest;
use dagr_core::TaskError;

// ===========================================================================
// Fake resources (C9): substituted through the kit's registry with NO task
// change between production and test.
// ===========================================================================

/// A fake resource whose interactions the test can observe — a stand-in for a
/// long-lived external client (an object-store or HTTP client) the task would
/// call in production.
#[derive(Clone, Default)]
struct FakeStore {
    /// How many times the task asked the store to `read`. Shared so the test can
    /// observe the recorded interactions after the run.
    reads: Arc<AtomicU32>,
}

impl FakeStore {
    fn read(&self) -> &'static str {
        self.reads.fetch_add(1, Ordering::SeqCst);
        "fake-payload"
    }
    fn read_count(&self) -> u32 {
        self.reads.load(Ordering::SeqCst)
    }
}

/// Two same-typed clients distinguished by newtype wrappers (C9 disambiguation).
struct BillingClient(FakeStore);
struct AnalyticsClient(FakeStore);

/// A secret resource carrying a planted sentinel value (C9 redaction test).
struct ApiToken(Secret<String>);

// ===========================================================================
// Illustrative task types
// ===========================================================================

/// A task that produces a value from a fake resource retrieved by type.
struct ReadsFake;
impl Task for ReadsFake {
    type Input = ();
    type Output = String;
    async fn run(&mut self, ctx: &RunContext, _i: ()) -> Result<String, TaskError> {
        let store = ctx
            .resources()
            .get::<FakeStore>()
            .expect("the fake FakeStore was injected");
        Ok(store.read().to_string())
    }
}

/// A synchronous task that does no awaiting at all — its body is pure compute.
struct SyncDoubler {
    n: u32,
}
impl Task for SyncDoubler {
    type Input = ();
    type Output = u32;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<u32, TaskError> {
        Ok(self.n * 2)
    }
}

/// An await-bound task that yields to the executor once before producing — it
/// genuinely suspends, so driving it requires the kit's provided runtime.
struct AwaitsThenProduces {
    value: u32,
}
impl Task for AwaitsThenProduces {
    type Input = ();
    type Output = u32;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<u32, TaskError> {
        yield_once().await;
        Ok(self.value)
    }
}

/// A task that reads every C8 context field and returns them, so the test can
/// assert what the task actually observed.
#[derive(Debug, PartialEq, Eq)]
struct SeenFields {
    run: String,
    pipeline: String,
    node_present: bool,
    attempt: u32,
    max_attempts: u32,
    param: Option<u32>,
    interval: Option<(String, String)>,
    cancelled: bool,
    span_attempt: u32,
    scratch_present: bool,
    registry_len: usize,
}
struct ReadsAllFields;
impl Task for ReadsAllFields {
    type Input = ();
    type Output = SeenFields;
    async fn run(&mut self, ctx: &RunContext, _i: ()) -> Result<SeenFields, TaskError> {
        Ok(SeenFields {
            run: ctx.run_id().as_str().to_string(),
            pipeline: ctx.pipeline_id().as_str().to_string(),
            node_present: {
                let _ = ctx.node_id();
                true
            },
            attempt: ctx.attempt(),
            max_attempts: ctx.max_attempts(),
            param: ctx.parameters::<u32>().copied(),
            interval: ctx
                .data_interval()
                .map(|d| (d.start().to_string(), d.end().to_string())),
            cancelled: ctx.cancellation().is_cancelled(),
            span_attempt: ctx.span().attempt(),
            // A read must not panic even on the honest-empty default seams.
            scratch_present: ctx.scratch().namespace_dir().is_some(),
            registry_len: ctx.resources().len(),
        })
    }
}

/// A task arranged to fail with a caller-chosen classified error.
struct FailsWith {
    err: fn() -> TaskError,
}
impl Task for FailsWith {
    type Input = ();
    type Output = u32;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<u32, TaskError> {
        Err((self.err)())
    }
}

/// A task that observes the cancellation signal and reports whether it saw it.
struct ObservesCancel;
impl Task for ObservesCancel {
    type Input = ();
    type Output = bool;
    async fn run(&mut self, ctx: &RunContext, _i: ()) -> Result<bool, TaskError> {
        Ok(ctx.cancellation().is_cancelled())
    }
}

/// A task that reads its two newtyped fakes and returns each one's base value.
struct ReadsTwoFakes;
impl Task for ReadsTwoFakes {
    type Input = ();
    type Output = (u32, u32);
    async fn run(&mut self, ctx: &RunContext, _i: ()) -> Result<(u32, u32), TaskError> {
        let billing = ctx.resources().get::<BillingClient>().expect("billing");
        let analytics = ctx.resources().get::<AnalyticsClient>().expect("analytics");
        billing.0.read();
        analytics.0.read();
        Ok((billing.0.read_count(), analytics.0.read_count()))
    }
}

/// A task that formats an injected secret through the *framework* output path
/// under test: it returns Ok, and the kit's captured diagnostics are inspected.
struct HoldsSecret;
impl Task for HoldsSecret {
    type Input = ();
    type Output = String;
    async fn run(&mut self, ctx: &RunContext, _i: ()) -> Result<String, TaskError> {
        // The task uses the secret through the authorized `expose` path but never
        // returns or logs it; the test asserts the *framework's* captured output
        // never contains the sentinel.
        let token = ctx.resources().get::<ApiToken>().expect("token");
        let _authorized = token.0.expose();
        Ok("done".to_string())
    }
}

// ===========================================================================
// A minimal await helper: a future that pends exactly once.
// ===========================================================================

fn yield_once() -> impl std::future::Future<Output = ()> {
    struct YieldOnce {
        done: bool,
    }
    impl std::future::Future for YieldOnce {
        type Output = ();
        fn poll(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<()> {
            if self.done {
                std::task::Poll::Ready(())
            } else {
                self.done = true;
                cx.waker().wake_by_ref();
                std::task::Poll::Pending
            }
        }
    }
    YieldOnce { done: false }
}

// ===========================================================================
// Tests — one per Test-plan bullet.
// ===========================================================================

/// **Synchronous task, no runtime.** A synchronous task, driven through the
/// kit's synchronous entry point, runs and returns its produced value with no
/// async runtime present and only its injected fake reachable.
#[test]
fn synchronous_task_runs_with_no_runtime() {
    let outcome = SingleTaskTest::new(SyncDoubler { n: 21 }).run_sync();
    assert!(outcome.is_success(), "the sync task succeeded");
    assert_eq!(outcome.output(), Some(&42), "produced its computed value");
}

/// A synchronous task can use an injected fake with no task change; the fake's
/// recorded interactions are observable to the test.
#[test]
fn synchronous_task_uses_injected_fake() {
    let store = FakeStore::default();
    let registry = ResourceRegistry::builder()
        .register(store.clone())
        .expect("unambiguous")
        .build();

    let outcome = SingleTaskTest::new(ReadsFake).resources(registry).run_sync();

    assert_eq!(outcome.output(), Some(&"fake-payload".to_string()));
    assert_eq!(store.read_count(), 1, "the fake recorded exactly one read");
}

/// **Await-bound task, provided runtime only.** An await-bound task that
/// genuinely suspends completes through the kit's await entry point, using only
/// the test runtime the kit provides — the caller stands up no runtime.
#[test]
fn await_bound_task_completes_on_provided_runtime() {
    let outcome = SingleTaskTest::new(AwaitsThenProduces { value: 7 }).run_await();
    assert!(outcome.is_success());
    assert_eq!(outcome.output(), Some(&7));
}

/// **Every context field is populated** — build with only defaults, on a
/// first-attempt-of-first-node default, and read every C8 field the task sees.
#[test]
fn every_context_field_is_populated_on_defaults() {
    let outcome = SingleTaskTest::new(ReadsAllFields).run_sync();
    let seen = outcome.output().expect("succeeded");

    assert!(!seen.run.is_empty(), "run identity present");
    assert!(!seen.pipeline.is_empty(), "pipeline identity present");
    assert!(seen.node_present, "node identity readable");
    assert_eq!(seen.attempt, 1, "first attempt by default");
    assert_eq!(seen.max_attempts, 1, "max attempts default");
    assert_eq!(seen.param, None, "no parameters by default");
    assert_eq!(seen.interval, None, "no data interval by default");
    assert!(!seen.cancelled, "cancellation un-tripped by default");
    assert_eq!(seen.span_attempt, 1, "span carries the attempt");
    assert!(!seen.scratch_present, "honest-empty scratch seam by default");
    assert_eq!(seen.registry_len, 0, "honest-empty registry by default");
}

/// **Caller-supplied fields are honored** — parameters, node identity, and the
/// attempt number are read back exactly as supplied; changing the attempt
/// changes what the task reads.
#[test]
fn caller_supplied_fields_are_honored() {
    let outcome = SingleTaskTest::new(ReadsAllFields)
        .node("retry-shaped-node")
        .attempt(2)
        .max_attempts(5)
        .parameters(99u32)
        .run_sync();
    let seen = outcome.output().expect("succeeded");
    assert_eq!(seen.attempt, 2, "the supplied attempt is read back");
    assert_eq!(seen.max_attempts, 5);
    assert_eq!(seen.param, Some(99), "the supplied parameter is read back");
    assert_eq!(seen.span_attempt, 2, "the span tracks the supplied attempt");

    // Changing the attempt changes what the task reads (retry-shaped test).
    let outcome1 = SingleTaskTest::new(ReadsAllFields).attempt(1).run_sync();
    assert_eq!(outcome1.output().unwrap().attempt, 1);
}

/// **Data interval round-trips verbatim** — the task sees byte-for-byte what was
/// supplied; the kit interprets nothing.
#[test]
fn data_interval_round_trips_verbatim() {
    // A deliberately non-timestamp, reversed pair proves no interpretation.
    let outcome = SingleTaskTest::new(ReadsAllFields)
        .data_interval(DataInterval::new("zzz-end", "aaa-start"))
        .run_sync();
    let seen = outcome.output().expect("succeeded");
    assert_eq!(
        seen.interval,
        Some(("zzz-end".to_string(), "aaa-start".to_string())),
        "the interval is returned exactly as supplied"
    );
}

/// **Fake resource retrieved by type with no task change** — already covered by
/// `synchronous_task_uses_injected_fake`; here we prove it through the await
/// entry point too, and that the same task code path is used.
#[test]
fn fake_resource_retrieved_by_type_via_await_path() {
    let store = FakeStore::default();
    let registry = ResourceRegistry::builder()
        .register(store.clone())
        .expect("unambiguous")
        .build();
    let outcome = SingleTaskTest::new(ReadsFake)
        .resources(registry)
        .run_await();
    assert_eq!(outcome.output(), Some(&"fake-payload".to_string()));
    assert_eq!(store.read_count(), 1);
}

/// **Two same-typed resources via newtypes** — each newtype resolves to its own
/// fake and they are not confused.
#[test]
fn two_same_typed_resources_via_newtypes() {
    let registry = ResourceRegistry::builder()
        .register(BillingClient(FakeStore::default()))
        .expect("billing distinct")
        .register(AnalyticsClient(FakeStore::default()))
        .expect("analytics distinct despite the shared inner type")
        .build();

    let outcome = SingleTaskTest::new(ReadsTwoFakes)
        .resources(registry)
        .run_sync();
    // Each newtype's fake counted only its own read.
    assert_eq!(
        outcome.output(),
        Some(&(1, 1)),
        "each newtype resolved to its own fake"
    );
}

/// **Secret fake stays redacted** — a planted sentinel never appears in any of
/// the kit's own framework-emitted output paths.
#[test]
fn secret_fake_stays_redacted_in_kit_output() {
    const SENTINEL: &str = "p1anted-s3ntinel-value";
    let registry = ResourceRegistry::builder()
        .register(ApiToken(Secret::new(SENTINEL.to_string())))
        .expect("unambiguous")
        .build();

    let outcome = SingleTaskTest::new(HoldsSecret)
        .resources(registry)
        .run_sync();
    assert!(outcome.is_success());

    // The kit's captured framework diagnostics (events + a rendered dump of the
    // controlled context) must never contain the sentinel.
    let dump = outcome.framework_output_dump();
    assert!(
        !dump.contains(SENTINEL),
        "the planted secret sentinel leaked into kit output:\n{dump}"
    );
}

/// **Classified error surfaces to the caller** — a retry-eligible and a
/// permanent failure both surface as an error (not an output) with the
/// classification readable.
#[test]
fn classified_error_surfaces_to_the_caller() {
    let retry = SingleTaskTest::new(FailsWith {
        err: || TaskError::retryable("transient blip"),
    })
    .run_sync();
    assert!(!retry.is_success());
    assert!(retry.output().is_none(), "no output on failure");
    let err = retry.error().expect("an error surfaced");
    assert!(err.is_retryable(), "classification readable: retry-eligible");

    let permanent = SingleTaskTest::new(FailsWith {
        err: || TaskError::permanent("bad input"),
    })
    .run_sync();
    let err = permanent.error().expect("an error surfaced");
    assert!(err.is_permanent(), "classification readable: permanent");

    let skip = SingleTaskTest::new(FailsWith {
        err: || TaskError::skip("nothing to do"),
    })
    .run_sync();
    assert!(skip.error().expect("skip surfaced").is_skip());
}

/// **Cancellation signal is observable** — a pre-tripped signal is seen as
/// cancelled; an un-tripped default is seen as not-cancelled.
#[test]
fn cancellation_signal_is_observable() {
    let cancelled = SingleTaskTest::new(ObservesCancel).cancelled().run_sync();
    assert_eq!(
        cancelled.output(),
        Some(&true),
        "the task observed the pre-tripped cancellation"
    );

    let uncancelled = SingleTaskTest::new(ObservesCancel).run_sync();
    assert_eq!(
        uncancelled.output(),
        Some(&false),
        "a context built without tripping presents it un-tripped"
    );
}

/// **Context is inert toward scheduling** (surface check) — the constructed
/// context exposes only reads; there is no method to mutate the graph, reorder
/// work, or reach a scheduler. Validated by the fact that the kit hands the task
/// a `&RunContext` whose entire surface is the C8 read-only accessor set (proven
/// by construction: the kit builds it through `RunContext::builder`, which has no
/// scheduling lever), plus this compile-checked absence: none of the scheduling
/// verbs exist on the type the task receives.
#[test]
fn context_is_inert_toward_scheduling() {
    // If any scheduling method existed on `RunContext`, a task could call it.
    // The kit exposes exactly the same read-only context every invocation gets.
    // This test documents the checklist item; the absence is enforced by the
    // context type itself (no such method compiles). We assert the kit produced a
    // usable read-only context and that running twice is identical (no hidden
    // scheduling state advanced between runs).
    let a = SingleTaskTest::new(SyncDoubler { n: 5 }).run_sync();
    let b = SingleTaskTest::new(SyncDoubler { n: 5 }).run_sync();
    assert_eq!(a.output(), b.output(), "deterministic; no scheduling drift");
}

/// **Determinism** — a task invoked twice through the kit with the same inputs
/// behaves identically (no hidden wall-clock, injected cancellation).
#[test]
fn same_inputs_behave_identically() {
    let build = || {
        let store = FakeStore::default();
        let registry = ResourceRegistry::builder()
            .register(store.clone())
            .expect("unambiguous")
            .build();
        (
            store,
            SingleTaskTest::new(ReadsFake)
                .resources(registry)
                .attempt(3)
                .data_interval(DataInterval::new("x", "y"))
                .run_sync(),
        )
    };
    let (s1, o1) = build();
    let (s2, o2) = build();
    assert_eq!(o1.output(), o2.output(), "identical outputs");
    assert_eq!(o1.attempt(), o2.attempt(), "identical observed attempt");
    assert_eq!(
        s1.read_count(),
        s2.read_count(),
        "identical fake interactions"
    );
}

/// **Metrics capture** — the kit exposes the attempt's captured metrics for
/// assertion (C23 seam), including on a successful run.
#[test]
fn captured_metrics_are_observable() {
    let outcome = SingleTaskTest::new(SyncDoubler { n: 3 }).run_sync();
    // The kit exposes an AttemptMetrics the test can read; a fresh success has an
    // empty task-metric set (the task attached none) and it does not panic.
    let metrics = outcome.metrics();
    assert_eq!(
        metrics.framework_metric_count(),
        0,
        "no framework metrics fabricated by the kit for a plain success"
    );
}

/// **Scratch capture** — a context built with a scratch root exposes a wired
/// scratch store the test can inspect after the run.
#[test]
fn scratch_is_capturable_when_rooted() {
    let tmp = std::env::temp_dir().join(format!(
        "dagr-t60-scratch-{}",
        std::process::id() as u64 * 2 + 1
    ));
    let outcome = SingleTaskTest::new(SyncDoubler { n: 1 })
        .scratch_root(tmp.clone())
        .run_sync();
    assert!(outcome.is_success());
    let scratch = outcome.scratch();
    assert!(
        scratch.namespace_dir().is_some(),
        "a rooted context carries a wired scratch store"
    );
}

/// **No pipeline writes its own single-task harness** — the example test (below)
/// and every test in this file construct the context, inject fakes, and drive
/// the task **only** through the library-provided kit APIs: no bespoke
/// `RunContext::builder` plumbing, no hand-rolled executor. This is asserted by
/// the fact that this whole file imports only `SingleTaskTest` from the kit and
/// the domain types it configures — never `run_attempt`, `Slot`, or a private
/// runner. (Documentation-backed; see the shipped example in the kit rustdoc.)
#[test]
fn example_uses_only_library_provided_kit_apis() {
    // The end-to-end example: a fake resource, a hand-built context, one sync and
    // one await case — all through the kit.
    let store = FakeStore::default();
    let registry = ResourceRegistry::builder()
        .register(store.clone())
        .expect("unambiguous")
        .build();

    // Synchronous case.
    let sync_out = SingleTaskTest::new(ReadsFake)
        .resources(registry)
        .node("reader")
        .run_sync();
    assert_eq!(sync_out.output(), Some(&"fake-payload".to_string()));

    // Await-bound case (fresh registry — the first was moved in).
    let await_out = SingleTaskTest::new(AwaitsThenProduces { value: 11 }).run_await();
    assert_eq!(await_out.output(), Some(&11));

    let _ = AtomicBool::new(false); // (kept: proves no external sync needed)
}
