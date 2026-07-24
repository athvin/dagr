//! C27 · Resume **core** — the pure gate + seed/closure/demand plan algorithm
//! (ticket T58, 070). Written first, TDD.
//!
//! T41 landed the structural fingerprint that gates resume; T57 landed the
//! durable-output contract and its per-attempt reference recording; T54a landed
//! the retained-scratch foundation; T55 stubbed the `resume` verb. T58 lands the
//! **resume core**: given a prior run's per-node terminal states + recorded
//! durable references and this binary's assembled graph, compute a demand-driven
//! re-execution plan — refusing up front on a changed structural fingerprint, an
//! incomparable algorithm version, a cross-tool-version prior, or a dangling
//! durable reference; and, when it proceeds, deriving the must-run seed, closing
//! it downward, resolving demand upward (rehydrating durable producers, pulling
//! demanded in-memory producers into the must-run set), and marking every prior
//! success left outside the must-run set `satisfied-from-prior` with its
//! originating run identity.
//!
//! This suite exercises the **pure plan** in `dagr_core::resume` against the REAL
//! `dagr_core::flow` registration surface + `Pipeline::fingerprint`. The
//! invocation derivation (parameters/interval/force), the run-store-gone refusal,
//! and the resumed-artifact recording (which need serde + the artifact crate) are
//! the CLI's, exercised in `crates/cli/tests/resume_verb.rs`. The exhaustive
//! behavioural suite is T59.

use std::collections::BTreeMap;

use dagr_core::assembly::{DurableOutput, NodePolicy};
use dagr_core::flow::{Flow, Pipeline};
use dagr_core::resume::{
    plan_resume, PriorNode, PriorRun, ReferenceExistence, ResumePlan, ResumeRefusal,
};
use dagr_core::task::Task;
use dagr_core::{RehydrateError, RunContext, TaskError, TerminalState};

// === Value + task fixtures =================================================

#[derive(Debug, Clone, PartialEq, Eq)]
struct Blob(String);

// A durable output type: its reference is its string, rehydrated verbatim.
impl DurableOutput for Blob {
    fn serialize_reference(&self) -> String {
        self.0.clone()
    }
    fn rehydrate(reference: &str) -> Result<Self, RehydrateError> {
        Ok(Blob(reference.to_string()))
    }
}

struct MakeBlob;
impl Task for MakeBlob {
    type Input = ();
    type Output = Blob;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Blob, TaskError> {
        Ok(Blob("x".into()))
    }
}

struct Passthrough;
impl Task for Passthrough {
    type Input = Blob;
    type Output = Blob;
    async fn run(&mut self, _c: &RunContext, i: Blob) -> Result<Blob, TaskError> {
        Ok(i)
    }
}

struct Effect;
impl Task for Effect {
    type Input = ();
    type Output = ();
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<(), TaskError> {
        Ok(())
    }
}

struct OrderedEffect;
impl Task for OrderedEffect {
    type Input = ();
    type Output = ();
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<(), TaskError> {
        Ok(())
    }
}

// === Pipeline fixtures =====================================================

/// A durable producer → in-memory consumer chain: `produce` (durable Blob) →
/// `consume` (Blob → Blob). The canonical resume shape.
fn durable_chain() -> Pipeline {
    let mut flow = Flow::new();
    let produce = flow.register_source_durable("produce", &MakeBlob, NodePolicy::new());
    let _consume = flow.register("consume", &Passthrough, produce);
    flow.finish()
}

/// A non-durable producer → consumer chain: `produce` (in-memory Blob) →
/// `consume`. Producer's value cannot be rehydrated.
fn in_memory_chain() -> Pipeline {
    let mut flow = Flow::new();
    let produce = flow.register_source("produce", &MakeBlob); // non-durable
    let _consume = flow.register("consume", &Passthrough, produce);
    flow.finish()
}

/// The cleanup-after-publish shape: `publish` is ordering-only (nothing consumes
/// its value), `cleanup` runs after it via an ordering edge.
fn cleanup_after_publish() -> Pipeline {
    let mut flow = Flow::new();
    let publish = flow.register_source("publish", &Effect);
    let _cleanup =
        flow.register_source_ordered_after("cleanup", &OrderedEffect, &[publish.ordering()]);
    flow.finish()
}

// === Prior-run builders ====================================================

/// A prior run whose fingerprint MATCHES `pipeline`, tool version "dagr@1", one
/// node per name in `states` with the given terminal state, an optional durable
/// reference, and originating run "run-A".
fn prior_for(pipeline: &Pipeline, states: &[(&str, TerminalState, Option<&str>)]) -> PriorRun {
    let fp = pipeline.fingerprint();
    let mut nodes = BTreeMap::new();
    for (name, terminal, dref) in states {
        nodes.insert(
            (*name).to_string(),
            PriorNode {
                terminal: *terminal,
                durable_reference: dref.map(str::to_string),
                originating_run: "run-A".to_string(),
            },
        );
    }
    PriorRun {
        structural_fingerprint: fp.structural(),
        policy_hash: fp.policy(),
        algorithm_version: fp.algorithm_version(),
        tool_version: "dagr@1".to_string(),
        nodes,
    }
}

/// The default probe: every reference exists.
fn present(_node: &str, _reference: &str) -> ReferenceExistence {
    ReferenceExistence::Present
}

fn plan(pipeline: &Pipeline, prior: &PriorRun) -> Result<ResumePlan, ResumeRefusal> {
    plan_resume(pipeline, prior, "dagr@1", present)
}

// ===========================================================================
// The gate: structural / algorithm-version / tool-version refusals.
// ===========================================================================

/// **Structural-fingerprint refusal with diff.** A prior run whose recorded
/// structural fingerprint differs from this binary's refuses without planning any
/// node, and the refusal carries the differing fingerprints (the structural diff).
#[test]
fn structural_fingerprint_mismatch_refuses() {
    let pipeline = durable_chain();
    let mut prior = prior_for(&pipeline, &[("produce", TerminalState::Succeeded, Some("r"))]);
    // A node was rewired/renamed since the prior run: the structural fingerprint
    // no longer matches.
    prior.structural_fingerprint ^= 0xDEAD_BEEF;

    let refusal = plan(&pipeline, &prior).expect_err("a changed graph cannot be resumed");
    match refusal {
        ResumeRefusal::StructuralMismatch { prior: p, current: c } => {
            assert_ne!(p, c, "the diff shows the two differing structural fingerprints");
            assert_eq!(c, pipeline.fingerprint().structural());
        }
        other => panic!("expected a structural-mismatch refusal, got {other:?}"),
    }
}

/// **Algorithm-version refusal is distinct.** A prior run whose fingerprint
/// algorithm version is not comparable to this binary's refuses as a DISTINCT
/// "cannot compare" refusal, not a structural mismatch.
#[test]
fn algorithm_version_mismatch_is_a_distinct_refusal() {
    let pipeline = durable_chain();
    let mut prior = prior_for(&pipeline, &[("produce", TerminalState::Succeeded, Some("r"))]);
    prior.algorithm_version += 1; // a different, incomparable algorithm version

    let refusal = plan(&pipeline, &prior).expect_err("an incomparable algorithm cannot compare");
    assert!(
        matches!(refusal, ResumeRefusal::AlgorithmVersionMismatch { .. }),
        "expected the cannot-compare refusal, distinct from a structural mismatch, got {refusal:?}"
    );
}

/// **Tool-version refusal is distinct.** A prior run recorded by a different tool
/// version refuses with the cross-tool-version refusal (v1's no-cross-version
/// promise), distinct from both the structural and algorithm-version refusals.
#[test]
fn tool_version_mismatch_is_a_distinct_refusal() {
    let pipeline = durable_chain();
    let prior = prior_for(&pipeline, &[("produce", TerminalState::Succeeded, Some("r"))]);
    // This binary is a different tool version than the prior run recorded.
    let refusal = plan_resume(&pipeline, &prior, "dagr@2", present)
        .expect_err("v1 makes no cross-tool-version resume promise");
    assert!(
        matches!(refusal, ResumeRefusal::ToolVersionMismatch { .. }),
        "expected the cross-tool-version refusal, got {refusal:?}"
    );
}

/// **Policy-only change proceeds.** A prior run identical structurally but with a
/// changed policy hash does NOT refuse — the plan carries a policy diff and
/// proceeds. (The raised-timeout resume is the motivating case.)
#[test]
fn policy_only_change_proceeds_with_a_diff() {
    let pipeline = durable_chain();
    let mut prior = prior_for(&pipeline, &[("produce", TerminalState::Succeeded, Some("r"))]);
    prior.policy_hash ^= 0x1234; // a raised timeout, say — policy differs, structure does not

    let plan = plan(&pipeline, &prior).expect("a policy-only change proceeds, never refuses");
    assert!(
        plan.policy_diff().is_some(),
        "a policy divergence is surfaced as a per-node diff, not a refusal"
    );
}

/// A structurally-and-policy-identical prior surfaces NO policy diff.
#[test]
fn identical_policy_surfaces_no_diff() {
    let pipeline = durable_chain();
    let prior = prior_for(
        &pipeline,
        &[
            ("produce", TerminalState::Succeeded, Some("r")),
            ("consume", TerminalState::Succeeded, None),
        ],
    );
    let plan = plan(&pipeline, &prior).expect("proceeds");
    assert!(plan.policy_diff().is_none(), "identical policy → no diff");
}

// ===========================================================================
// The existence probe: a dangling durable reference fails the PLAN.
// ===========================================================================

/// **Dangling durable reference fails the plan.** A candidate durable node whose
/// referenced object has been deleted fails the resume *plan* up front (not the
/// eleventh executing node), naming the offending reference.
#[test]
fn dangling_durable_reference_fails_the_plan() {
    let pipeline = durable_chain();
    // produce succeeded durably; consume did NOT, so consume re-runs and demands
    // produce's durable value — which we make dangling.
    let prior = prior_for(
        &pipeline,
        &[
            ("produce", TerminalState::Succeeded, Some("gone/ref")),
            ("consume", TerminalState::Failed, None),
        ],
    );
    let probe = |_node: &str, reference: &str| {
        if reference == "gone/ref" {
            ReferenceExistence::Absent
        } else {
            ReferenceExistence::Present
        }
    };
    let refusal = plan_resume(&pipeline, &prior, "dagr@1", probe)
        .expect_err("a dangling reference fails the plan before any node executes");
    match refusal {
        ResumeRefusal::DanglingReference { node, reference } => {
            assert_eq!(node, "produce");
            assert_eq!(reference, "gone/ref", "the refusal names the offending reference");
        }
        other => panic!("expected a dangling-reference refusal, got {other:?}"),
    }
}

/// An undemanded durable success is NOT existence-checked (its value is never
/// rehydrated), so a dangling reference on an undemanded node does not fail the
/// plan — it is satisfied-from-prior on effect alone.
#[test]
fn undemanded_durable_success_with_a_dangling_ref_still_resumes() {
    let pipeline = durable_chain();
    // Both succeeded; a full-success resume re-runs nothing, so produce's durable
    // ref is never demanded — a dangling ref must not fail the plan.
    let prior = prior_for(
        &pipeline,
        &[
            ("produce", TerminalState::Succeeded, Some("gone/ref")),
            ("consume", TerminalState::Succeeded, None),
        ],
    );
    let probe = |_n: &str, _r: &str| ReferenceExistence::Absent;
    let plan = plan_resume(&pipeline, &prior, "dagr@1", probe)
        .expect("an undemanded durable success is not existence-checked");
    assert!(plan.must_run().is_empty(), "a full success is a no-op");
}

// ===========================================================================
// The seed / closure / demand algorithm.
// ===========================================================================

/// **Full-success resume is a no-op.** Every node ended `succeeded`: the seed is
/// empty, the must-run set is empty, and every node is satisfied-from-prior.
#[test]
fn full_success_resume_is_a_noop() {
    let pipeline = durable_chain();
    let prior = prior_for(
        &pipeline,
        &[
            ("produce", TerminalState::Succeeded, Some("r")),
            ("consume", TerminalState::Succeeded, None),
        ],
    );
    let plan = plan(&pipeline, &prior).expect("proceeds");
    assert!(plan.seed().is_empty(), "empty seed on a full success");
    assert!(plan.must_run().is_empty(), "nothing re-executes");
    assert_eq!(
        plan.satisfied_from_prior().len(),
        2,
        "both prior successes are satisfied-from-prior"
    );
}

/// **Seed = non-succeeded nodes.** A node whose prior terminal state was not
/// `succeeded` is in the seed and re-runs.
#[test]
fn non_succeeded_node_is_in_the_seed() {
    let pipeline = durable_chain();
    let prior = prior_for(
        &pipeline,
        &[
            ("produce", TerminalState::Succeeded, Some("r")),
            ("consume", TerminalState::Failed, None),
        ],
    );
    let plan = plan(&pipeline, &prior).expect("proceeds");
    assert!(plan.seed().contains("consume"), "the failed node seeds the plan");
    assert!(plan.must_run().contains("consume"), "and re-executes");
}

/// **Durable success is satisfied and rehydrated on demand.** A durable producer
/// succeeded, a downstream consumer must re-run: the producer is
/// satisfied-from-prior (carrying its originating run), is not in the must-run
/// set, and the plan rehydrates its reference to fill the consumer's slot.
#[test]
fn durable_success_is_satisfied_and_rehydrated_on_demand() {
    let pipeline = durable_chain();
    let prior = prior_for(
        &pipeline,
        &[
            ("produce", TerminalState::Succeeded, Some("produce/output")),
            ("consume", TerminalState::Failed, None),
        ],
    );
    let plan = plan(&pipeline, &prior).expect("proceeds");
    assert!(!plan.must_run().contains("produce"), "the durable producer does not re-run");
    assert_eq!(
        plan.satisfied_from_prior().get("produce").map(String::as_str),
        Some("run-A"),
        "it is satisfied-from-prior carrying its originating run identity"
    );
    assert_eq!(
        plan.rehydrate().get("produce").map(String::as_str),
        Some("produce/output"),
        "its durable reference is rehydrated to fill the re-running consumer's slot"
    );
}

/// **In-memory success re-runs only when demanded — (a) not demanded.** A
/// non-durable producer succeeded and nothing that re-runs demands its value: it
/// is satisfied-from-prior and does not re-execute.
#[test]
fn in_memory_success_not_demanded_is_satisfied() {
    let pipeline = in_memory_chain();
    // consume succeeded too, so nothing re-runs — produce's value is not demanded.
    let prior = prior_for(
        &pipeline,
        &[
            ("produce", TerminalState::Succeeded, None),
            ("consume", TerminalState::Succeeded, None),
        ],
    );
    let plan = plan(&pipeline, &prior).expect("proceeds");
    assert!(plan.must_run().is_empty(), "nothing re-runs");
    assert!(
        plan.satisfied_from_prior().contains_key("produce"),
        "the undemanded in-memory success is satisfied-from-prior"
    );
}

/// **In-memory success re-runs only when demanded — (b) demanded.** A non-durable
/// producer succeeded but a re-executing consumer demands its value: the producer
/// joins the must-run set and re-executes (it cannot be rehydrated), and its own
/// upstream demands would cascade the same way.
#[test]
fn in_memory_success_demanded_re_runs() {
    let pipeline = in_memory_chain();
    let prior = prior_for(
        &pipeline,
        &[
            ("produce", TerminalState::Succeeded, None), // in-memory, no reference
            ("consume", TerminalState::Failed, None),    // re-runs, demands produce
        ],
    );
    let plan = plan(&pipeline, &prior).expect("proceeds");
    assert!(
        plan.must_run().contains("produce"),
        "the demanded in-memory producer joins the must-run set and re-executes"
    );
    assert!(
        !plan.satisfied_from_prior().contains_key("produce"),
        "a re-executing node is not satisfied-from-prior"
    );
    assert!(
        plan.rehydrate().get("produce").is_none(),
        "an in-memory value is re-executed, never rehydrated"
    );
}

/// A demanded in-memory producer cascades its OWN upstream demands. A three-node
/// in-memory chain a→b→c where only c failed pulls b (demanded by c) and then a
/// (demanded by b) into the must-run set.
#[test]
fn demanded_in_memory_producer_cascades_upstream() {
    let mut flow = Flow::new();
    let a = flow.register_source("a", &MakeBlob); // in-memory
    let b = flow.register("b", &Passthrough, a); // in-memory
    let _c = flow.register("c", &Passthrough, b);
    let pipeline = flow.finish();

    let prior = prior_for(
        &pipeline,
        &[
            ("a", TerminalState::Succeeded, None),
            ("b", TerminalState::Succeeded, None),
            ("c", TerminalState::Failed, None),
        ],
    );
    let plan = plan(&pipeline, &prior).expect("proceeds");
    for n in ["a", "b", "c"] {
        assert!(plan.must_run().contains(n), "{n} re-runs (demand cascaded upward)");
    }
    assert!(
        plan.satisfied_from_prior().is_empty(),
        "every node re-runs; none is satisfied-from-prior"
    );
}

/// **Downward closure re-runs reachable nodes.** A seed node's successors
/// downstream of it re-run even when they themselves succeeded before.
#[test]
fn downward_closure_re_runs_reachable_successors() {
    let mut flow = Flow::new();
    let a = flow.register_source_durable("a", &MakeBlob, NodePolicy::new());
    let b = flow.register("b", &Passthrough, a);
    let _c = flow.register("c", &Passthrough, b);
    let pipeline = flow.finish();

    // a succeeded durably, b FAILED (seeds the plan), c succeeded — but c is
    // downstream of b, so the downward closure re-runs it.
    let prior = prior_for(
        &pipeline,
        &[
            ("a", TerminalState::Succeeded, Some("a/out")),
            ("b", TerminalState::Failed, None),
            ("c", TerminalState::Succeeded, None),
        ],
    );
    let plan = plan(&pipeline, &prior).expect("proceeds");
    assert!(plan.must_run().contains("b"), "the seed node re-runs");
    assert!(
        plan.must_run().contains("c"),
        "the successor downstream of the seed re-runs (downward closure)"
    );
    assert!(!plan.must_run().contains("a"), "the durable upstream is satisfied + rehydrated");
    assert_eq!(plan.rehydrate().get("a").map(String::as_str), Some("a/out"));
}

/// **Teardown-covered node is re-executed.** A node covered by a teardown that
/// executed in the prior run is in the seed and re-executes even though it
/// succeeded (its durable output may have been destroyed).
#[test]
fn teardown_covered_node_is_re_executed_even_when_it_succeeded() {
    // setup (durable) is covered by a teardown ordered after it; both succeeded.
    let mut flow = Flow::new();
    let setup = flow.register_source_durable("setup", &MakeBlob, NodePolicy::new());
    let _teardown: dagr_core::Handle<()> =
        flow.register_teardown("teardown", &Effect, &[setup.ordering()]);
    let pipeline = flow.finish();

    // Both succeeded. Without the teardown rule, setup would be satisfied-from-prior;
    // because a teardown covering it executed, it must re-run.
    let prior = prior_for(
        &pipeline,
        &[
            ("setup", TerminalState::Succeeded, Some("setup/out")),
            ("teardown", TerminalState::Succeeded, None),
        ],
    );
    let plan = plan(&pipeline, &prior).expect("proceeds");
    assert!(
        plan.seed().contains("setup"),
        "a teardown-covered node joins the seed even though it succeeded"
    );
    assert!(plan.must_run().contains("setup"), "and re-executes");
    assert!(
        !plan.satisfied_from_prior().contains_key("setup"),
        "a teardown-destroyed output is not resume-safe: never satisfied-from-prior"
    );
}

/// **Cleanup-after-publish shape.** An ordering-only `publish` (non-durable,
/// nothing demands its value) that succeeded is satisfied-from-prior even though
/// it is not durable; a re-running `cleanup` downstream sees a success-like
/// upstream. Here cleanup failed, so it re-runs; publish stays satisfied.
#[test]
fn cleanup_after_publish_resumes_correctly() {
    let pipeline = cleanup_after_publish();
    let prior = prior_for(
        &pipeline,
        &[
            ("publish", TerminalState::Succeeded, None), // ordering-only, non-durable
            ("cleanup", TerminalState::Failed, None),    // re-runs
        ],
    );
    let plan = plan(&pipeline, &prior).expect("proceeds");
    assert!(
        plan.satisfied_from_prior().contains_key("publish"),
        "the ordering-only, non-durable, undemanded success is satisfied-from-prior"
    );
    assert!(
        !plan.must_run().contains("publish"),
        "publish is not pulled into the must-run set (its value is never demanded)"
    );
    assert!(plan.must_run().contains("cleanup"), "cleanup re-runs");
}

/// Every prior success left outside the must-run set carries its ORIGINATING run
/// identity — even a multi-generation origin (a node satisfied-from-prior in the
/// prior run keeps its original origin).
#[test]
fn satisfied_nodes_carry_their_originating_run_identity() {
    let pipeline = durable_chain();
    let mut prior = prior_for(
        &pipeline,
        &[
            ("produce", TerminalState::Succeeded, Some("r")),
            ("consume", TerminalState::Failed, None),
        ],
    );
    // produce was itself satisfied-from-prior in run-A, originating in run-ROOT.
    prior.nodes.get_mut("produce").unwrap().originating_run = "run-ROOT".to_string();
    let plan = plan(&pipeline, &prior).expect("proceeds");
    assert_eq!(
        plan.satisfied_from_prior().get("produce").map(String::as_str),
        Some("run-ROOT"),
        "the originating run identity is carried forward across generations"
    );
}

/// A non-succeeded but *non-failure* prior state (e.g. cancelled/skipped) also
/// seeds re-execution — the seed is "not succeeded", not "failed".
#[test]
fn a_cancelled_prior_node_also_seeds_re_execution() {
    let pipeline = durable_chain();
    let prior = prior_for(
        &pipeline,
        &[
            ("produce", TerminalState::Succeeded, Some("r")),
            ("consume", TerminalState::Cancelled, None),
        ],
    );
    let plan = plan(&pipeline, &prior).expect("proceeds");
    assert!(
        plan.seed().contains("consume"),
        "a cancelled (non-succeeded) node seeds re-execution"
    );
}
