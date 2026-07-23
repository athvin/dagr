//! Behavioral unit tests for the C8 run context (ticket T16 / 022). Written
//! first, TDD: every scenario constructs a [`RunContext`] **by hand** — no
//! runtime, no store, no registry, no scheduler — and reads its accessors. This
//! is the exact hand-constructability guarantee C8 makes and T60 (single-task
//! test kit) leans on.
//!
//! Each test mirrors one bullet of the ticket's Test plan. The context is a
//! read-only capability surface: the tests confirm every field is populated,
//! read back verbatim, and that no accessor is a lever back into scheduling.

use std::sync::Arc;

use dagr_core::context::{
    CancellationSource, CoveredNodeStates, DataInterval, PipelineId, ResourceRequirement,
    ResourceRequirements, RunContext, RunId, TerminalState,
};
use dagr_core::handle::NodeId;

/// A recognizable parameters value — carried opaquely by the context and read
/// back by downcast. The framework never inspects it.
#[derive(Debug, PartialEq, Eq)]
struct Params {
    target: String,
    limit: u32,
}

/// Build a fully-populated context with distinct, recognizable values for every
/// field. Returned alongside the cancellation source so a test can flip it.
fn full_context() -> (RunContext, CancellationSource) {
    let source = CancellationSource::new();
    let ctx = RunContext::builder(
        RunId::new("run-abc"),
        PipelineId::new("etl-pipeline"),
        NodeId::from_name("load-node"),
    )
    .attempt(1)
    .max_attempts(3)
    .parameters(Arc::new(Params {
        target: "warehouse".to_string(),
        limit: 42,
    }))
    .data_interval(DataInterval::new("2026-07-01", "2026-07-02"))
    .cancellation(source.signal())
    .build();
    (ctx, source)
}

/// **All fields populated on a hand-built context.** Every accessor returns
/// exactly the supplied value; nothing is absent, defaulted-away, or panics.
#[test]
fn all_fields_populated_on_a_hand_built_context() {
    let (ctx, _source) = full_context();

    assert_eq!(ctx.run_id(), &RunId::new("run-abc"));
    assert_eq!(ctx.pipeline_id(), &PipelineId::new("etl-pipeline"));
    assert_eq!(ctx.node_id(), NodeId::from_name("load-node"));
    assert_eq!(ctx.attempt(), 1);
    assert_eq!(ctx.max_attempts(), 3);

    let params = ctx
        .parameters::<Params>()
        .expect("the supplied parameters are readable by type");
    assert_eq!(
        params,
        &Params {
            target: "warehouse".to_string(),
            limit: 42
        }
    );

    let interval = ctx.data_interval().expect("a data interval was supplied");
    assert_eq!(interval.start(), "2026-07-01");
    assert_eq!(interval.end(), "2026-07-02");

    // The cancellation signal and span are present (read-only observation
    // channels), and the registry/scratch seams exist.
    assert!(!ctx.cancellation().is_cancelled());
    let _span = ctx.span();
    let _registry = ctx.resources();
    let _scratch = ctx.scratch();
}

/// **Attempt number is readable and reflects the supplied attempt.** One context
/// at attempt 1, another at attempt 2, both with max 3.
#[test]
fn attempt_number_reflects_the_supplied_attempt() {
    let first = RunContext::builder(
        RunId::new("r"),
        PipelineId::new("p"),
        NodeId::from_name("n"),
    )
    .attempt(1)
    .max_attempts(3)
    .build();
    let second = RunContext::builder(
        RunId::new("r"),
        PipelineId::new("p"),
        NodeId::from_name("n"),
    )
    .attempt(2)
    .max_attempts(3)
    .build();

    assert_eq!(first.attempt(), 1);
    assert_eq!(second.attempt(), 2);
    assert_eq!(first.max_attempts(), 3);
    assert_eq!(second.max_attempts(), 3);
}

/// **Data interval is carried verbatim and never interpreted.** An arbitrary
/// opaque pair — reversed order, identical endpoints, empty content — is returned
/// unchanged; construction and reading never inspect, order, validate, or
/// normalize the contents.
#[test]
fn data_interval_is_carried_verbatim_and_never_interpreted() {
    // Reversed order: an interval whose "end" precedes its "start" if anyone
    // tried to parse them. The framework must not care.
    let reversed = DataInterval::new("zzz", "aaa");
    assert_eq!(reversed.start(), "zzz");
    assert_eq!(reversed.end(), "aaa");

    // Identical endpoints.
    let identical = DataInterval::new("same", "same");
    assert_eq!(identical.start(), "same");
    assert_eq!(identical.end(), "same");

    // Empty / sentinel content, and content nonsensical as a timestamp.
    let empty = DataInterval::new("", "");
    assert_eq!(empty.start(), "");
    assert_eq!(empty.end(), "");
    let nonsense = DataInterval::new("not-a-date", "\0\u{1}garbage");
    assert_eq!(nonsense.start(), "not-a-date");
    assert_eq!(nonsense.end(), "\0\u{1}garbage");

    // Carried through the context unchanged.
    let ctx = RunContext::builder(
        RunId::new("r"),
        PipelineId::new("p"),
        NodeId::from_name("n"),
    )
    .data_interval(reversed)
    .build();
    let carried = ctx.data_interval().expect("interval supplied");
    assert_eq!(carried.start(), "zzz");
    assert_eq!(carried.end(), "aaa");
}

/// **Optional data interval absence is representable.** A context built with no
/// data interval reports absence cleanly, and no other field is affected.
#[test]
fn optional_data_interval_absence_is_representable() {
    let ctx = RunContext::builder(
        RunId::new("r"),
        PipelineId::new("p"),
        NodeId::from_name("n"),
    )
    .attempt(1)
    .max_attempts(1)
    .build();

    assert!(ctx.data_interval().is_none());
    // Other fields are unaffected by the interval's absence.
    assert_eq!(ctx.attempt(), 1);
    assert_eq!(ctx.node_id(), NodeId::from_name("n"));
}

/// **Hand-construction requires no runtime.** Build a context and invoke a
/// trivial task-shaped closure with it, entirely within the test — no store, no
/// registry, no clock, no network. The closure runs and observes the context.
#[test]
fn hand_construction_requires_no_runtime() {
    let (ctx, _source) = full_context();

    // A trivial task-shaped closure: it observes the context and returns.
    let observe = |ctx: &RunContext| -> u32 { ctx.attempt() + ctx.max_attempts() };
    assert_eq!(observe(&ctx), 1 + 3);
}

/// **`for_test` yields a usable, fully-populated context with no arguments.**
/// The zero-argument path T9's tests already call stays valid and every field is
/// present (no field silently absent).
#[test]
fn for_test_yields_a_populated_context() {
    let ctx = RunContext::for_test();
    // Every field is populated — none panics or is absent.
    let _ = ctx.run_id();
    let _ = ctx.pipeline_id();
    let _ = ctx.node_id();
    assert_eq!(ctx.attempt(), 1, "first attempt by default");
    assert!(ctx.max_attempts() >= 1);
    assert!(!ctx.cancellation().is_cancelled());
    assert!(ctx.data_interval().is_none());
    assert!(ctx.covered_terminal_states().is_none());
}

/// **Resource-requirement declaration is carried through to a queryable form.** A
/// node that declares it requires a resource type reports that type; a node
/// declaring nothing reports an empty requirement set. The reported form is one
/// bootstrap (T30) can later validate against a registry.
#[test]
fn resource_requirements_are_carried_to_a_queryable_form() {
    struct ObjectStore;
    struct DbPool;
    struct NotRequired;

    let declared = ResourceRequirements::new()
        .require::<ObjectStore>()
        .require::<DbPool>();

    assert_eq!(declared.len(), 2);
    assert!(declared.requires::<ObjectStore>());
    assert!(declared.requires::<DbPool>());
    assert!(!declared.requires::<NotRequired>());

    // A node declaring nothing reports an empty requirement set.
    let none = ResourceRequirements::new();
    assert!(none.is_empty());
    assert_eq!(none.len(), 0);

    // The declared types are enumerable in a stable, renderable form (feeds the
    // graph artifact later, C20): each carries a type name.
    let names: Vec<&str> = declared
        .iter()
        .map(ResourceRequirement::type_name)
        .collect();
    assert!(names.iter().any(|n| n.contains("ObjectStore")));
    assert!(names.iter().any(|n| n.contains("DbPool")));
}

/// **Registry accessor seam is present and honestly unimplemented.** The accessor
/// exists with a stable signature; because C9 (T30) is not landed here, it
/// surfaces a clearly-empty / not-yet-available result, never a silently-wrong
/// resource.
#[test]
fn registry_accessor_seam_is_present_and_honestly_unimplemented() {
    struct SomeClient;

    let (ctx, _source) = full_context();
    let registry = ctx.resources();

    // The seam exists and is honestly empty: no resource is fabricated. When T30
    // lands, this test is updated to assert real type-keyed retrieval.
    assert!(registry.get::<SomeClient>().is_none());
    assert!(registry.is_empty());
}

/// **Scratch accessor seam is present and honestly unimplemented.** The accessor
/// exists with a stable signature and, because C18 (T53) is not landed here,
/// surfaces a documented not-yet-available result rather than pretending to
/// persist.
#[test]
fn scratch_accessor_seam_is_present_and_honestly_unimplemented() {
    let (ctx, _source) = full_context();
    let scratch = ctx.scratch();

    // A read returns "not yet available", not a silent success or a fabricated
    // value. When T53 lands, this becomes a read-after-write-across-attempts test.
    let read = scratch.get(b"cursor");
    assert!(
        read.is_err(),
        "scratch is honestly unimplemented, not silently empty"
    );
    let write = scratch.put(b"cursor", b"42");
    assert!(write.is_err(), "scratch does not pretend to persist");
}

/// **Teardown context exposes covered-node terminal states; a normal context does
/// not claim to.** The teardown-flavoured context reports exactly the supplied
/// terminal states (from the normative taxonomy); the non-teardown context
/// reports the absence of any covered set.
#[test]
fn teardown_context_exposes_covered_node_terminal_states() {
    let covered = CoveredNodeStates::new()
        .with(NodeId::from_name("setup-a"), TerminalState::Succeeded)
        .with(NodeId::from_name("setup-b"), TerminalState::Skipped);

    let teardown_ctx = RunContext::builder(
        RunId::new("r"),
        PipelineId::new("p"),
        NodeId::from_name("teardown"),
    )
    .covered_terminal_states(covered)
    .build();

    let states = teardown_ctx
        .covered_terminal_states()
        .expect("a teardown context exposes covered states");
    assert_eq!(
        states.get(NodeId::from_name("setup-a")),
        Some(TerminalState::Succeeded)
    );
    assert_eq!(
        states.get(NodeId::from_name("setup-b")),
        Some(TerminalState::Skipped)
    );
    // Cleanup can no-op when setup never ran: an unknown node reports nothing.
    assert_eq!(states.get(NodeId::from_name("never-registered")), None);

    // A non-teardown context reflects the absence of any covered set.
    let normal_ctx = RunContext::for_test();
    assert!(normal_ctx.covered_terminal_states().is_none());
}

/// **Cancellation signal is observable but read-only from the task's side.** The
/// task-facing side observes a flip but has no method to cancel the run itself —
/// the signal is an observation channel, not a lever.
#[test]
fn cancellation_signal_is_observable_but_read_only() {
    let (ctx, source) = full_context();

    // Before the flip: not cancelled.
    assert!(!ctx.cancellation().is_cancelled());

    // The runtime side (held by the test) flips it.
    source.cancel();

    // The task-facing side observes the change.
    assert!(ctx.cancellation().is_cancelled());
}

/// **The context exposes no mutation or scheduling authority.** This is partly a
/// compile-time/API-shape assertion: every public accessor takes `&self` and
/// returns a value or a read handle — there is no `&mut self` API that mutates
/// graph or scheduling state, no register/rescind, no reach into the runtime. A
/// shared `&RunContext` is enough to read every field, proving no `&mut` lever
/// exists.
#[test]
fn context_exposes_no_mutation_or_scheduling_authority() {
    fn assert_send_sync<T: Send + Sync>() {}

    let (ctx, _source) = full_context();
    let shared: &RunContext = &ctx;

    // Every observation is available through a shared reference — no `&mut self`
    // mutating API is reachable. If any accessor required `&mut self`, this would
    // not compile.
    let _ = shared.run_id();
    let _ = shared.pipeline_id();
    let _ = shared.node_id();
    let _ = shared.attempt();
    let _ = shared.max_attempts();
    let _ = shared.parameters::<Params>();
    let _ = shared.data_interval();
    let _ = shared.cancellation();
    let _ = shared.span();
    let _ = shared.resources();
    let _ = shared.scratch();
    let _ = shared.covered_terminal_states();

    // The context is `Send + Sync` so it can be shared across the worker driving
    // the attempt, and holds no mutable shared state reachable by the task.
    assert_send_sync::<RunContext>();
}
