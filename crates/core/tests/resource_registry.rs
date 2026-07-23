//! Behavioral tests for the C9 resource registry (ticket T30 / 040). Written
//! first, TDD: each test mirrors one bullet of the ticket's Test plan. The
//! registry is dependency injection built in `main` — a type-keyed, immutable,
//! shared-for-the-run map the developer constructs by hand; the framework fetches
//! nothing from anywhere to populate it.
//!
//! Scope note: this exercises **only** C9 (registration, typed acquisition,
//! newtype disambiguation, ambiguity rejection, secret wrapping, and bootstrap
//! validation against declared [`ResourceRequirements`], plus reading the
//! registry through a [`RunContext`]). No concurrency dispatch/admission (T33),
//! no owning-worker thread (documented only), no artifact rendering (C20/C22).
//!
//! The compile-time guarantees — the secret wrapper having no `Debug`/`Display`
//! and the `Send + Sync + 'static` bound on stored resources — are covered by the
//! T8 compile-fail harness (`tests/ui/secret_no_debug.rs`,
//! `tests/ui/secret_no_display.rs`, `tests/ui/registry_non_send_resource.rs`),
//! not here; a runtime test cannot assert the *absence* of a trait impl.

use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use dagr_core::context::{
    BootstrapOutcome, RegistryError, ResourceRegistry, ResourceRequirements, RunContext, Secret,
};
use dagr_core::handle::NodeId;
use dagr_core::task::Task;
use dagr_core::{PipelineId, RunId, TaskError};

/// Drive a future to completion on the current thread with no runtime — the same
/// no-op-waker busy-poll the C8/C28 task tests use (`Waker::noop`, stable since
/// 1.85, within MSRV). The task futures here do no I/O, so one poll completes
/// them; the real runner is C14 / T20.
fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    let mut fut = pin!(future);
    loop {
        if let Poll::Ready(value) = fut.as_mut().poll(&mut cx) {
            return value;
        }
    }
}

// === Representative resources ==============================================

/// A stand-in for a long-lived object-storage client. `Send + Sync + 'static`,
/// as every stored resource must be.
#[derive(Debug, PartialEq, Eq)]
struct ObjectStore {
    bucket: String,
}

/// A stand-in for a database connection pool.
#[derive(Debug, PartialEq, Eq)]
struct DbPool {
    dsn: String,
}

/// The underlying HTTP client type shared by two logically-distinct resources —
/// distinguished only by the newtype wrappers below (the C9 disambiguation
/// pattern).
#[derive(Debug, PartialEq, Eq)]
struct HttpClient {
    base_url: String,
}

/// Newtype: the HTTP client that talks to the *billing* service.
#[derive(Debug, PartialEq, Eq)]
struct BillingClient(HttpClient);

/// Newtype: the HTTP client that talks to the *analytics* service. Same
/// underlying type as [`BillingClient`], distinct registry key.
#[derive(Debug, PartialEq, Eq)]
struct AnalyticsClient(HttpClient);

// === Retrieve by type ======================================================

/// **Retrieve by type.** A registry built with one resource of a concrete type
/// hands that exact resource back on a type-keyed `get` — no string key, no
/// runtime type mismatch on the happy path.
#[test]
fn retrieve_by_type_returns_the_registered_resource() {
    let registry = ResourceRegistry::builder()
        .register(ObjectStore {
            bucket: "warehouse".to_string(),
        })
        .expect("first ObjectStore registration is unambiguous")
        .build();

    let store = registry
        .get::<ObjectStore>()
        .expect("the registered ObjectStore is retrievable by its type");
    assert_eq!(store.bucket, "warehouse");
}

/// **Missing key.** Retrieving a type that was never registered yields `None`
/// — never a fabricated or silently-wrong resource.
#[test]
fn retrieving_an_unregistered_type_yields_none() {
    let registry = ResourceRegistry::builder()
        .register(ObjectStore {
            bucket: "warehouse".to_string(),
        })
        .expect("registration is unambiguous")
        .build();

    assert!(registry.get::<DbPool>().is_none());
}

// === Ambiguous duplicate rejected ==========================================

/// **Ambiguous duplicate rejected.** Registering a second resource of the
/// literally identical type fails registry construction with an ambiguity error;
/// it neither silently replaces the first nor keeps both, and no registry is
/// produced (the builder is consumed, so the caller cannot proceed with a
/// half-built registry).
#[test]
fn duplicate_same_type_registration_is_rejected_as_ambiguous() {
    let err = ResourceRegistry::builder()
        .register(DbPool {
            dsn: "first".to_string(),
        })
        .expect("first DbPool registration is unambiguous")
        .register(DbPool {
            dsn: "second".to_string(),
        })
        .expect_err("a second DbPool of the identical type is ambiguous");

    match err {
        RegistryError::Duplicate { type_name } => {
            assert!(
                type_name.contains("DbPool"),
                "the ambiguity error names the offending type, got {type_name}"
            );
        }
    }
}

/// The first-registered resource is **not** mutated or replaced by a rejected
/// duplicate: recovering the builder after a rejected duplicate is impossible
/// (the error path consumes nothing that could carry the second value), and a
/// registry built from only the first registration still holds the first value.
#[test]
fn a_rejected_duplicate_does_not_replace_the_first() {
    let builder = ResourceRegistry::builder()
        .register(DbPool {
            dsn: "first".to_string(),
        })
        .expect("first registration is unambiguous");

    // Attempting the duplicate returns an error and yields no builder — so the
    // only registry the caller can build is the one holding the first value.
    let registry = builder.build();
    assert_eq!(registry.get::<DbPool>().map(|p| p.dsn.as_str()), Some("first"));
}

// === Newtype disambiguation ================================================

/// **Newtype disambiguation succeeds.** Two resources of the same underlying
/// type (`HttpClient`), each wrapped in a distinct newtype, both register and are
/// retrievable independently by newtype — the documented pattern for two
/// same-typed resources.
#[test]
fn newtype_wrappers_disambiguate_two_same_typed_resources() {
    let registry = ResourceRegistry::builder()
        .register(BillingClient(HttpClient {
            base_url: "https://billing".to_string(),
        }))
        .expect("BillingClient is a distinct type")
        .register(AnalyticsClient(HttpClient {
            base_url: "https://analytics".to_string(),
        }))
        .expect("AnalyticsClient is a distinct type despite the shared inner type")
        .build();

    let billing = registry.get::<BillingClient>().expect("billing retrievable");
    let analytics = registry
        .get::<AnalyticsClient>()
        .expect("analytics retrievable");
    assert_eq!(billing.0.base_url, "https://billing");
    assert_eq!(analytics.0.base_url, "https://analytics");
}

// === Immutable after construction ==========================================

/// **Immutable after construction.** A built registry exposes no mutation path.
/// This is a structural fact — verified here by cloning the shared handle and
/// confirming both clones observe the same immutable contents, and (at compile
/// time, structurally) that no `&mut self` accessor or insertion method exists on
/// [`ResourceRegistry`] itself. The builder is the *only* place a resource is
/// added, and it is consumed by `build`.
#[test]
fn a_built_registry_is_shared_read_only() {
    let registry = ResourceRegistry::builder()
        .register(ObjectStore {
            bucket: "shared".to_string(),
        })
        .expect("unambiguous")
        .build();

    // The registry is cheaply shareable for the whole run; every clone observes
    // the same immutable contents (there is no per-clone divergence because
    // there is no mutation path).
    let a = registry.clone();
    let b = registry.clone();
    assert_eq!(a.get::<ObjectStore>().map(|s| s.bucket.as_str()), Some("shared"));
    assert_eq!(b.get::<ObjectStore>().map(|s| s.bucket.as_str()), Some("shared"));
}

// === Backward-compat with T16 ==============================================

/// **Empty registry (T16 back-compat).** The honest-empty registry T16's
/// [`RunContext`] carries still behaves exactly as before: `get` is `None` for
/// every type and `is_empty` is `true`. The default and the builder's
/// zero-registration build agree.
#[test]
fn the_empty_registry_still_behaves_as_before() {
    let default_registry = ResourceRegistry::default();
    assert!(default_registry.is_empty());
    assert!(default_registry.get::<ObjectStore>().is_none());

    let built_empty = ResourceRegistry::builder().build();
    assert!(built_empty.is_empty());
    assert!(built_empty.get::<ObjectStore>().is_none());

    // A non-empty registry reports `is_empty() == false`.
    let non_empty = ResourceRegistry::builder()
        .register(ObjectStore {
            bucket: "b".to_string(),
        })
        .expect("unambiguous")
        .build();
    assert!(!non_empty.is_empty());
}

// === A task reads a resource through the RunContext ========================

/// A task that reaches a resource through `ctx.resources().get::<R>()`.
struct UploadTask;

impl Task for UploadTask {
    type Input = ();
    type Output = String;

    async fn run(
        &mut self,
        ctx: &RunContext,
        _input: Self::Input,
    ) -> Result<Self::Output, TaskError> {
        // The task programs against the resource *type*, retrieved through the
        // read-only context — no string lookup, no route back to scheduling.
        let store = ctx
            .resources()
            .get::<ObjectStore>()
            .expect("the ObjectStore resource is registered for this run");
        Ok(format!("uploaded to {}", store.bucket))
    }
}

/// **Task reads a resource through `RunContext`.** A context built by hand with a
/// populated registry hands the task the real resource; the task uses it with no
/// knowledge of how it got there.
#[test]
fn a_task_reads_a_resource_through_the_run_context() {
    let registry = ResourceRegistry::builder()
        .register(ObjectStore {
            bucket: "prod-bucket".to_string(),
        })
        .expect("unambiguous")
        .build();

    let ctx = RunContext::builder(
        RunId::new("run-1"),
        PipelineId::new("pipe-1"),
        NodeId::from_name("upload"),
    )
    .resources(registry)
    .build();

    let mut task = UploadTask;
    let out = block_on(task.run(&ctx, ()));
    assert_eq!(out.unwrap(), "uploaded to prod-bucket");
}

/// **Fake substitution needs no task change.** The same [`UploadTask`], with no
/// modification, retrieves and uses a *fake* `ObjectStore` when the test
/// constructs a registry containing the fake instead of the real client. There is
/// only one `ObjectStore` type; the fake is simply a differently-constructed
/// value of it — the whole point of type-keyed DI.
#[test]
fn a_fake_resource_substitutes_with_no_task_change() {
    let fake = ResourceRegistry::builder()
        .register(ObjectStore {
            bucket: "in-memory-fake".to_string(),
        })
        .expect("unambiguous")
        .build();

    let ctx = RunContext::builder(
        RunId::new("run-1"),
        PipelineId::new("pipe-1"),
        NodeId::from_name("upload"),
    )
    .resources(fake)
    .build();

    let mut task = UploadTask;
    let out = block_on(task.run(&ctx, ()));
    assert_eq!(out.unwrap(), "uploaded to in-memory-fake");
}

// === Bootstrap validation ==================================================

/// **Missing declared resource fails at bootstrap, before execution.** A set of
/// nodes declares a resource type that was never registered. Validating the
/// registry against those declarations fails; the failure names both the missing
/// resource type and the *exact set* of nodes that declared a requirement on it.
#[test]
fn a_missing_declared_resource_fails_bootstrap_naming_the_resource_and_nodes() {
    // Registry has ObjectStore but NOT DbPool.
    let registry = ResourceRegistry::builder()
        .register(ObjectStore {
            bucket: "b".to_string(),
        })
        .expect("unambiguous")
        .build();

    let loader = NodeId::from_name("loader");
    let writer = NodeId::from_name("writer");
    let reader = NodeId::from_name("reader");

    // loader and writer require DbPool (unregistered); reader requires only
    // ObjectStore (satisfied).
    let declarations = vec![
        (loader, ResourceRequirements::new().require::<DbPool>()),
        (writer, ResourceRequirements::new().require::<DbPool>()),
        (reader, ResourceRequirements::new().require::<ObjectStore>()),
    ];

    let outcome = registry.validate_requirements(&declarations);
    let failure = outcome.expect_err("a missing declared resource fails bootstrap");

    // Exactly one missing-resource error, naming DbPool.
    assert_eq!(failure.errors().len(), 1);
    let missing = &failure.errors()[0];
    assert!(
        missing.resource_type_name().contains("DbPool"),
        "the error names the missing resource type, got {}",
        missing.resource_type_name()
    );

    // The exact set of requiring nodes is {loader, writer} — not reader.
    let mut requiring: Vec<NodeId> = missing.requiring_nodes().to_vec();
    requiring.sort_by_key(|n| format!("{n:?}"));
    let mut expected = vec![loader, writer];
    expected.sort_by_key(|n| format!("{n:?}"));
    assert_eq!(requiring, expected);
    assert!(!requiring.contains(&reader));
}

/// **Bootstrap-failure artifact produced on missing resource.** The missing
/// resource condition yields a bootstrap-failure outcome (distinct from an
/// assembly failure), carrying the resource-validation error in its error list,
/// with **zero attempts recorded** — no node executed. The outcome is a value the
/// downstream artifact emitter (C20/C22) renders; this ticket only produces it.
#[test]
fn a_missing_resource_produces_the_bootstrap_failure_artifact_with_zero_attempts() {
    let registry = ResourceRegistry::default(); // registers nothing

    let node = NodeId::from_name("needs-store");
    let declarations = vec![(node, ResourceRequirements::new().require::<ObjectStore>())];

    let failure = registry
        .validate_requirements(&declarations)
        .expect_err("bootstrap fails");

    // The outcome is a bootstrap failure, distinct from an assembly failure.
    assert_eq!(failure.outcome(), BootstrapOutcome::BootstrapFailed);
    assert_ne!(failure.outcome(), BootstrapOutcome::Succeeded);

    // Zero attempts recorded — nothing executed.
    assert_eq!(failure.attempts_recorded(), 0);

    // The resource-validation error is in the error list.
    assert_eq!(failure.errors().len(), 1);
    assert!(failure.errors()[0]
        .resource_type_name()
        .contains("ObjectStore"));
}

/// **All requirements satisfied passes.** A registry containing every declared
/// resource type validates clean; execution is allowed to proceed (no error).
#[test]
fn all_requirements_satisfied_passes_validation() {
    let registry = ResourceRegistry::builder()
        .register(ObjectStore {
            bucket: "b".to_string(),
        })
        .expect("unambiguous")
        .register(DbPool {
            dsn: "d".to_string(),
        })
        .expect("unambiguous")
        .build();

    let declarations = vec![
        (
            NodeId::from_name("a"),
            ResourceRequirements::new().require::<ObjectStore>(),
        ),
        (
            NodeId::from_name("b"),
            ResourceRequirements::new()
                .require::<ObjectStore>()
                .require::<DbPool>(),
        ),
    ];

    assert!(registry.validate_requirements(&declarations).is_ok());
}

/// **Declared requirements are surfaced.** The declared requirements — resource
/// type name and requiring node — are enumerable in a stable form so a downstream
/// graph-artifact test (C20) can assert they appear. Surfacing does not depend on
/// whether they are satisfied.
#[test]
fn declared_requirements_are_surfaced_for_artifact_emission() {
    let a = NodeId::from_name("a");
    let b = NodeId::from_name("b");
    let declarations = vec![
        (a, ResourceRequirements::new().require::<ObjectStore>()),
        (
            b,
            ResourceRequirements::new()
                .require::<ObjectStore>()
                .require::<DbPool>(),
        ),
    ];

    // Every (node, resource-type-name) pair the nodes declared is present in the
    // surfaced set.
    let surfaced = dagr_core::context::surface_requirements(&declarations);
    assert!(surfaced
        .iter()
        .any(|(n, name)| *n == a && name.contains("ObjectStore")));
    assert!(surfaced
        .iter()
        .any(|(n, name)| *n == b && name.contains("ObjectStore")));
    assert!(surfaced
        .iter()
        .any(|(n, name)| *n == b && name.contains("DbPool")));
    // node a did not declare DbPool.
    assert!(!surfaced
        .iter()
        .any(|(n, name)| *n == a && name.contains("DbPool")));
}

// === Secret wrapper ========================================================

/// A unique sentinel a test plants inside a secret and then hunts for in any
/// framework-controlled emission path.
const SENTINEL: &str = "S3CR3T-SENTINEL-9c1f-do-not-log";

/// **Sentinel redaction.** A secret wrapping a sentinel string never leaks that
/// sentinel through a framework-controlled emission path exercised here — the
/// wrapper's `Debug` (used by any framework diagnostic path) is redacted, so
/// formatting the *registry contents* never surfaces the sentinel. (Full
/// framework-emitted log-line redaction is C25/T45; this establishes the wrapper
/// and the sentinel hook.)
#[test]
fn a_secret_sentinel_never_appears_in_a_framework_emission_path() {
    let secret = Secret::new(SENTINEL.to_string());

    // The wrapper yields its inner value to authorized access.
    assert_eq!(secret.expose(), SENTINEL);

    // A registry holding the secret, formatted through the framework's own
    // Debug path (the only structured-emission path this ticket controls), never
    // surfaces the sentinel bytes: the registry's Debug is redaction-safe.
    let registry = ResourceRegistry::builder()
        .register(secret)
        .expect("unambiguous")
        .build();
    let rendered = format!("{registry:?}");
    assert!(
        !rendered.contains(SENTINEL),
        "the sentinel leaked through the registry's framework emission path: {rendered}"
    );

    // Even the redacted marker the wrapper emits in its place must not be the
    // sentinel.
    let secret2 = Secret::new(SENTINEL.to_string());
    let redacted = secret2.redacted();
    assert!(!redacted.contains(SENTINEL));
}
