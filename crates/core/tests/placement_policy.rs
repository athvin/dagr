//! **Placement as node policy** — the declaration surface, the two hashes, and
//! resume compatibility. Written first, TDD.
//!
//! Placement is a [`NodePolicy`] field, never an execution class. The reason is
//! mechanical rather than aesthetic: the execution class feeds the **structural**
//! fingerprint, and a structural mismatch is a hard resume refusal — so a `Remote`
//! class would refuse resume for every pipeline in existence the moment it was
//! added. `NodePolicy` feeds the **policy** hash, where a divergence proceeds with
//! a printed diff. The payoff is what this suite pins: a pipeline can be run
//! locally and resumed remotely (or the reverse), and moving a node between the two
//! is a reviewable policy diff rather than a broken resume.
//!
//! What this file pins:
//!
//! - a declared placement is carried **verbatim as opaque strings** — `dagr-core`
//!   parses nothing, validates nothing, and learns no Kubernetes;
//! - placing a node moves the **policy hash** and leaves the **structural
//!   fingerprint** untouched;
//! - an **unplaced** pipeline hashes exactly as it did before placement existed
//!   (both digests pinned to literals recorded on the pre-change tree);
//! - a local→remote resume **proceeds** with a policy diff, and a placed pipeline
//!   resumed against itself produces **no** diff.

use std::collections::BTreeMap;

use dagr_core::assembly::{NodePolicy, Placement};
use dagr_core::resume::{PriorNode, PriorRun, ReferenceExistence, ResumeRefusal, plan_resume};
use dagr_core::stable_name::StableName;
use dagr_core::task::{RunContext, Task};
use dagr_core::{FINGERPRINT_ALGORITHM_VERSION, Flow, Pipeline, TaskError, TerminalState};

// ===========================================================================
// Fixtures — a two-node chain, registered through the stable-name-aware surface
// so both hashes have real author-declared inputs to run over.
// ===========================================================================

struct Rows;
impl StableName for Rows {
    const STABLE_NAME: &'static str = "Rows";
}
struct Report;
impl StableName for Report {
    const STABLE_NAME: &'static str = "Report";
}

struct Extract;
impl StableName for Extract {
    const STABLE_NAME: &'static str = "extract-rows";
}
impl Task for Extract {
    type Input = ();
    type Output = Rows;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Rows, TaskError> {
        Ok(Rows)
    }
}

struct Load;
impl StableName for Load {
    const STABLE_NAME: &'static str = "load-report";
}
impl Task for Load {
    type Input = Rows;
    type Output = Report;
    async fn run(&mut self, _c: &RunContext, _i: Rows) -> Result<Report, TaskError> {
        Ok(Report)
    }
}

/// The placement the `load` node carries in the placed variant: CPU, memory, one
/// node selector, and one toleration — every field the ticket names, all opaque.
fn gpu_placement() -> Placement {
    Placement::new()
        .cpu("500m")
        .memory("2Gi")
        .node_selectors(&[("nodepool", "gpu"), ("zone", "eu-west-1a")])
        .tolerations(&["nvidia.com/gpu=present:NoSchedule"])
}

/// The two-node chain. `placement` is applied to the **downstream** node only, so
/// the placed and unplaced pipelines differ in exactly one policy field.
fn chain(placement: Option<Placement>) -> Pipeline {
    let mut flow = Flow::new();
    let rows = flow.register_source_named::<Extract>(
        "extract",
        &Extract,
        None::<String>,
        NodePolicy::new(),
    );
    let policy = match placement {
        Some(p) => NodePolicy::new().placement(p),
        None => NodePolicy::new(),
    };
    let _ = flow.register_named::<Load, _>("load", &Load, rows, None::<String>, policy);
    flow.finish()
}

fn unplaced() -> Pipeline {
    chain(None)
}

fn placed() -> Pipeline {
    chain(Some(gpu_placement()))
}

// ===========================================================================
// The policy surface — opaque strings, carried verbatim
// ===========================================================================

/// A placement declared at registration reaches the assembled node's policy
/// **verbatim**: every string is the one the author wrote, in the order written.
/// Nothing is parsed, normalized, or validated — `dagr-core` never learns what any
/// of these strings mean.
#[test]
fn a_declared_placement_is_carried_verbatim_as_opaque_strings() {
    let pipeline = placed();
    let node = pipeline
        .nodes()
        .find(|n| n.name() == "load")
        .expect("the placed node is in the pipeline");

    let placement = node
        .policy()
        .placement_spec()
        .expect("the placed node carries its placement");
    assert_eq!(placement.cpu_request(), Some("500m"));
    assert_eq!(placement.memory_request(), Some("2Gi"));
    assert_eq!(
        placement.node_selector_pairs(),
        &[("nodepool", "gpu"), ("zone", "eu-west-1a")],
        "node selectors are opaque (key, value) string pairs in declared order"
    );
    assert_eq!(
        placement.toleration_strings(),
        &["nvidia.com/gpu=present:NoSchedule"],
        "a toleration is one opaque string; dagr-core does not parse its grammar"
    );

    // The same value surfaces on the FULL effective policy — which is what the
    // graph artifact serializes.
    assert_eq!(
        node.effective_policy().placement(),
        Some(gpu_placement()),
        "the effective policy carries the placement the author declared"
    );
}

/// The default is **absent** (= local), and writing the default out explicitly
/// (`placement_off`) is indistinguishable from leaving it unset — the same rule
/// every other policy field obeys.
#[test]
fn placement_defaults_to_absent_and_the_written_out_default_matches() {
    assert_eq!(
        NodePolicy::new().placement_spec(),
        None,
        "the conservative default is no placement (the node runs locally)"
    );
    assert_eq!(
        NodePolicy::new().placement_off(),
        NodePolicy::new(),
        "writing the no-placement default out is identical to leaving it unset"
    );
    assert_eq!(
        NodePolicy::new().placement(gpu_placement()).placement_off(),
        NodePolicy::new(),
        "placement_off clears a previously declared placement"
    );
    assert_eq!(
        unplaced()
            .nodes()
            .find(|n| n.name() == "load")
            .expect("node")
            .effective_policy()
            .placement(),
        None,
        "an unplaced node's effective policy carries no placement"
    );
}

// ===========================================================================
// The two hashes — policy moves, structure does not
// ===========================================================================

/// **The load-bearing property.** Two pipelines identical except that one node
/// carries a placement have **equal structural fingerprints** and **different
/// policy hashes**. This is what makes a local↔remote move a policy diff instead
/// of a resume refusal.
#[test]
fn placement_moves_the_policy_hash_and_never_the_structural_fingerprint() {
    let plain = unplaced().fingerprint();
    let remote = placed().fingerprint();

    assert_eq!(
        plain.structural(),
        remote.structural(),
        "placement is out of the structural fingerprint — the graph's shape did not change"
    );
    assert_ne!(
        plain.policy(),
        remote.policy(),
        "placement is in the policy hash — a placement change is review-visible"
    );
    assert_eq!(
        plain.algorithm_version(),
        remote.algorithm_version(),
        "placement does not move the fingerprint algorithm version"
    );
}

/// Changing *any* opaque field of a placement moves the policy hash — the whole
/// declaration is hashed, not just its presence.
#[test]
fn every_placement_field_feeds_the_policy_hash() {
    let base = chain(Some(Placement::new().cpu("500m"))).fingerprint();
    let variants = [
        Placement::new().cpu("1"),
        Placement::new().cpu("500m").memory("2Gi"),
        Placement::new()
            .cpu("500m")
            .node_selectors(&[("nodepool", "gpu")]),
        Placement::new()
            .cpu("500m")
            .tolerations(&["nvidia.com/gpu=present:NoSchedule"]),
    ];
    for variant in variants {
        let fp = chain(Some(variant)).fingerprint();
        assert_eq!(
            fp.structural(),
            base.structural(),
            "no placement field may reach the structural fingerprint"
        );
        assert_ne!(
            fp.policy(),
            base.policy(),
            "changing a placement field must move the policy hash: {variant:?}"
        );
    }
}

/// **The no-churn guarantee.** A pipeline that declares no placement hashes to
/// exactly the digests the pre-placement tree computed for it. These two literals
/// were recorded on the tree immediately before placement existed; if either moves,
/// every existing pipeline's resume just broke.
#[test]
fn an_unplaced_pipeline_hashes_exactly_as_it_did_before_placement_existed() {
    // Recorded on the pre-change tree from this exact fixture.
    const BASELINE_STRUCTURAL: u64 = 0xfbf0_1c07_9d3e_27f4;
    const BASELINE_POLICY: u64 = 0xd246_a1e7_e043_6d63;

    let fp = unplaced().fingerprint();
    assert_eq!(
        fp.structural(),
        BASELINE_STRUCTURAL,
        "adding placement must not perturb an unplaced pipeline's structural fingerprint"
    );
    assert_eq!(
        fp.policy(),
        BASELINE_POLICY,
        "adding placement must not perturb an unplaced pipeline's policy hash — an \
         absent placement contributes zero bytes to the canonical encoding"
    );
    assert_eq!(fp.algorithm_version(), FINGERPRINT_ALGORITHM_VERSION);
}

// ===========================================================================
// Resume compatibility — the payoff
// ===========================================================================

/// Every reference exists (this fixture has no durable node, so the probe is never
/// consulted; it is required by the signature).
fn present(_node: &str, _reference: &str, _expected_hash: Option<&str>) -> ReferenceExistence {
    ReferenceExistence::Present
}

/// The prior-run facts for `pipeline`, with every node recorded succeeded.
fn prior_for(pipeline: &Pipeline) -> PriorRun {
    let fp = pipeline.fingerprint();
    let mut nodes = BTreeMap::new();
    for node in pipeline.nodes() {
        nodes.insert(
            node.name().to_string(),
            PriorNode {
                terminal: TerminalState::Succeeded,
                durable_reference: None,
                durable_reference_content_hash: None,
                originating_run: "prior-run".to_string(),
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

/// **A run made locally resumes remotely.** The prior run was the unplaced
/// pipeline; this binary places a node. Resume **proceeds** — it does not refuse —
/// and surfaces a policy diff carrying both hashes.
#[test]
fn a_local_run_resumed_against_a_placed_binary_proceeds_with_a_policy_diff() {
    let prior_pipeline = unplaced();
    let prior = prior_for(&prior_pipeline);
    let current = placed();

    let plan = plan_resume(&current, &prior, "dagr@1", present)
        .expect("a placement change is a POLICY divergence — resume must never refuse it");

    let diff = plan
        .policy_diff()
        .expect("the policy hashes diverged, so the plan carries the diff");
    assert_eq!(diff.prior, prior.policy_hash);
    assert_eq!(diff.current, current.fingerprint().policy());
    assert_ne!(diff.prior, diff.current);

    // The rendered diff is what an operator reads; it must name both hashes.
    let rendered = diff.to_string();
    assert!(
        rendered.contains(&format!("{:016x}", diff.prior))
            && rendered.contains(&format!("{:016x}", diff.current)),
        "the printed policy diff names both hashes: {rendered}"
    );
}

/// **And the reverse.** A run made against the placed binary resumes against the
/// unplaced one — the same proceed-with-diff, in the other direction.
#[test]
fn a_remote_run_resumed_against_a_local_binary_also_proceeds() {
    let prior = prior_for(&placed());
    let current = unplaced();
    let plan = plan_resume(&current, &prior, "dagr@1", present)
        .expect("moving a node back off remote compute is also only a policy divergence");
    assert!(plan.policy_diff().is_some());
}

/// A placement change is **never** a structural mismatch — the refusal that would
/// have made an `ExecutionClass::Remote` variant unusable.
#[test]
fn a_placement_change_is_never_a_structural_refusal() {
    let prior = prior_for(&unplaced());
    match plan_resume(&placed(), &prior, "dagr@1", present) {
        Ok(_) => {}
        Err(ResumeRefusal::StructuralMismatch { prior, current }) => panic!(
            "a placement change must not refuse as a structural mismatch \
             (prior fnv:{prior:016x}, current fnv:{current:016x})"
        ),
        Err(other) => panic!("resume refused for an unexpected reason: {other:?}"),
    }
}

/// A placed pipeline resumed **against itself** prints no policy diff — placement
/// contributes to the hash deterministically, so an unchanged declaration is an
/// unchanged hash.
#[test]
fn a_placed_pipeline_resumed_against_itself_has_no_policy_diff() {
    let pipeline = placed();
    let prior = prior_for(&pipeline);
    let plan = plan_resume(&pipeline, &prior, "dagr@1", present).expect("identical binary resumes");
    assert_eq!(
        plan.policy_diff(),
        None,
        "an unchanged placement is an unchanged policy hash — nothing to print"
    );
}
