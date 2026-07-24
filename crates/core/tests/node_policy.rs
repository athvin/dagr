//! C5 node-policy tests — ticket T29 (039). Written first, TDD.
//!
//! These exercise the **full** C5 node-policy surface (arch.md `### C5 · Node
//! policy`): the immutable per-node policy value, its conservative defaults, its
//! attach-at-registration API, the full **effective policy** (defaulted values
//! written out) that reaches the graph artifact, and the policy's participation
//! in the two graph hashes (C21 / T0.7) — the policy hash vs. the structural
//! fingerprint split.
//!
//! T29 **expands** the minimal `NodePolicy` seam T14 landed and T22's interim
//! retry knob into one home: retries + backoff shape, per-attempt timeout, the
//! declared per-pool cost vector (working memory / output residency split), the
//! trigger rule (closed T0.4 set, sourced from the binding typestate), the
//! constrained execution-class override, group, retention, and durability.
//!
//! Downstream *consumption* of these knobs — admission/capacity (T31/C12),
//! class dispatch (T33/C13), trigger-rule runtime (T34/C15), and the concrete
//! BLAKE3 fingerprint algorithm and artifact schema (T40/T41/C21) — is out of
//! scope; this ticket only defines the values they read.

use std::time::Duration;

use dagr_core::assembly::{DurableOutput, EffectivePolicy, NodePolicy, ProblemKind};
use dagr_core::binding::TriggerRule;
use dagr_core::execution::Backoff;
use dagr_core::flow::Flow;
use dagr_core::handle::NodeId;
use dagr_core::task::{ExecutionClass, RunContext, Task};
use dagr_core::TaskError;

// --- Illustrative value + task types ----------------------------------------
struct Rows;

/// A sourceless (await-bound, default class) task producing `Rows`.
struct MakeRows;
impl Task for MakeRows {
    type Input = ();
    type Output = Rows;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Rows, TaskError> {
        Ok(Rows)
    }
}

/// A sourceless task producing `u64` (await-bound — forbids a synchronous
/// override).
struct AwaitBoundTask;
impl Task for AwaitBoundTask {
    type Input = ();
    type Output = u64;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<u64, TaskError> {
        Ok(0)
    }
}

/// A synchronous (blocking) sourceless task — its work shape permits moving
/// between the two synchronous classes.
struct BlockingTask;
impl Task for BlockingTask {
    type Input = ();
    type Output = u64;
    const EXECUTION_CLASS: ExecutionClass = ExecutionClass::Blocking;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<u64, TaskError> {
        Ok(0)
    }
}

/// A durable-contract-satisfying output type (T57 full contract).
struct DurableBlob;
impl DurableOutput for DurableBlob {
    fn serialize_reference(&self) -> String {
        "durable-blob/ref".to_string()
    }
    fn rehydrate(_reference: &str) -> Result<Self, dagr_core::RehydrateError> {
        Ok(DurableBlob)
    }
}

/// A sourceless task whose output satisfies the durable-output contract.
struct MakeDurable;
impl Task for MakeDurable {
    type Input = ();
    type Output = DurableBlob;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<DurableBlob, TaskError> {
        Ok(DurableBlob)
    }
}

/// Fetch a node's effective policy by name from an assembled pipeline.
fn effective(flow_build: impl FnOnce(&mut Flow), name: &str) -> EffectivePolicy {
    let mut flow = Flow::new();
    flow_build(&mut flow);
    let pipeline = flow.finish();
    pipeline
        .node(NodeId::from_name(name))
        .expect("node present")
        .effective_policy()
}

// ---------------------------------------------------------------------------
// Every field has a documented default, applied uniformly (no-policy node).
// ---------------------------------------------------------------------------

#[test]
fn every_field_has_its_documented_default() {
    let policy = effective(
        |flow| {
            let _ = flow.register_source("plain", &MakeRows);
        },
        "plain",
    );

    // no retries
    assert_eq!(policy.retry_count(), 0, "default: no retries");
    // no timeout
    assert_eq!(policy.timeout(), None, "default: no per-attempt timeout");
    // zero declared cost on every pool entry (working + output residency both 0)
    assert!(policy.cost().is_zero(), "default: zero declared cost");
    assert_eq!(policy.cost().working_memory(), 0);
    assert_eq!(policy.cost().output_residency(), 0);
    assert_eq!(policy.cost().blocking_threads(), 0);
    assert_eq!(policy.cost().compute_threads(), 0);
    // trigger rule all-succeeded
    assert_eq!(
        policy.trigger_rule(),
        TriggerRule::AllSucceeded,
        "default: all-succeeded"
    );
    // execution class equals the task's declared class (await-bound here)
    assert_eq!(
        policy.execution_class(),
        ExecutionClass::AwaitBound,
        "default: class as declared by the task"
    );
    // group absent
    assert_eq!(policy.group(), None, "default: no group");
    // release once consumed (not retained)
    assert!(!policy.is_retained(), "default: release once consumed");
    // not durable
    assert!(!policy.is_durable(), "default: not durable");
}

// ---------------------------------------------------------------------------
// All-defaults node equals no-policy node, field-for-field.
// ---------------------------------------------------------------------------

#[test]
fn all_defaults_node_equals_no_policy_node() {
    // No policy stated.
    let none = effective(
        |flow| {
            let _ = flow.register_source("n", &MakeRows);
        },
        "n",
    );
    // Every default written out explicitly.
    let all = effective(
        |flow| {
            let _ = flow.register_source_with(
                "n",
                &MakeRows,
                NodePolicy::new()
                    .retries(0)
                    .timeout_off()
                    .working_memory(0)
                    .output_residency(0)
                    .blocking_threads(0)
                    .compute_threads(0)
                    .retained(false)
                    .durable(false),
            );
        },
        "n",
    );
    assert_eq!(none, all, "defaulted policy equals written-out defaults");
}

// ---------------------------------------------------------------------------
// All-defaults node equals no-policy node under the policy hash + structural fp.
// ---------------------------------------------------------------------------

#[test]
fn all_defaults_node_equals_no_policy_under_the_policy_hash() {
    let mut a = Flow::new();
    let _ = a.register_source("n", &MakeRows);
    let fp_a = a.finish().assemble().expect("assembles").fingerprint();

    let mut b = Flow::new();
    let _ = b.register_source_with(
        "n",
        &MakeRows,
        NodePolicy::new()
            .retries(0)
            .timeout_off()
            .retained(false)
            .durable(false),
    );
    let fp_b = b.finish().assemble().expect("assembles").fingerprint();

    assert_eq!(
        fp_a.policy(),
        fp_b.policy(),
        "defaulted and written-out-default policies hash identically"
    );
    assert_eq!(
        fp_a.structural(),
        fp_b.structural(),
        "structural fingerprints match too"
    );
}

// ---------------------------------------------------------------------------
// A single changed policy value changes only the policy hash.
// ---------------------------------------------------------------------------

#[test]
fn a_single_changed_policy_value_changes_only_the_policy_hash() {
    let mut a = Flow::new();
    let _ = a.register_source("n", &MakeRows);
    let fp_a = a.finish().assemble().expect("assembles").fingerprint();

    let mut b = Flow::new();
    let _ = b.register_source_with("n", &MakeRows, NodePolicy::new().retries(5));
    let fp_b = b.finish().assemble().expect("assembles").fingerprint();

    assert_eq!(
        fp_a.structural(),
        fp_b.structural(),
        "a retry-count change is not structural"
    );
    assert_ne!(
        fp_a.policy(),
        fp_b.policy(),
        "a retry-count change moves the policy hash"
    );
}

// ---------------------------------------------------------------------------
// Timeout is a policy value: it feeds the policy hash, not the structural fp.
// ---------------------------------------------------------------------------

#[test]
fn timeout_feeds_the_policy_hash_not_the_structural_fingerprint() {
    let mut a = Flow::new();
    let _ = a.register_source("n", &MakeRows);
    let fp_a = a.finish().assemble().expect("assembles").fingerprint();

    let mut b = Flow::new();
    let _ = b.register_source_with(
        "n",
        &MakeRows,
        NodePolicy::new().timeout(Duration::from_secs(30)),
    );
    let fp_b = b.finish().assemble().expect("assembles").fingerprint();

    assert_eq!(
        fp_a.structural(),
        fp_b.structural(),
        "timeout is not structural"
    );
    assert_ne!(
        fp_a.policy(),
        fp_b.policy(),
        "timeout moves the policy hash"
    );
}

// ---------------------------------------------------------------------------
// Trigger rule feeds the structural fingerprint, not the policy hash.
// ---------------------------------------------------------------------------

#[test]
fn trigger_rule_feeds_the_structural_fingerprint_not_the_policy_hash() {
    // Two consume-nothing nodes differing only in trigger rule.
    let mut a = Flow::new();
    let _ = a.register_source_with_trigger(
        "n",
        &MakeRows,
        NodePolicy::new(),
        TriggerRule::AllSucceeded,
    );
    let fp_a = a.finish().assemble().expect("assembles").fingerprint();

    let mut b = Flow::new();
    let _ =
        b.register_source_with_trigger("n", &MakeRows, NodePolicy::new(), TriggerRule::AllTerminal);
    let fp_b = b.finish().assemble().expect("assembles").fingerprint();

    assert_ne!(
        fp_a.structural(),
        fp_b.structural(),
        "trigger rule is a structural (resume-gating) input"
    );
    assert_eq!(
        fp_a.policy(),
        fp_b.policy(),
        "the trigger rule contributes no policy-hash divergence"
    );
}

// ---------------------------------------------------------------------------
// Group is in neither hash.
// ---------------------------------------------------------------------------

#[test]
fn group_is_in_neither_hash() {
    let mut a = Flow::new();
    let _ = a.register_source("n", &MakeRows);
    let fp_a = a.finish().assemble().expect("assembles").fingerprint();

    let mut b = Flow::new();
    let _ = b.register_source_in_group("n", &MakeRows, Some("etl"));
    let fp_b = b.finish().assemble().expect("assembles").fingerprint();

    let mut c = Flow::new();
    let _ = c.register_source_in_group("n", &MakeRows, Some("renamed"));
    let fp_c = c.finish().assemble().expect("assembles").fingerprint();

    assert_eq!(fp_a.structural(), fp_b.structural(), "group not structural");
    assert_eq!(fp_a.policy(), fp_b.policy(), "group not in policy hash");
    assert_eq!(
        fp_b.structural(),
        fp_c.structural(),
        "group rename not structural"
    );
    assert_eq!(
        fp_b.policy(),
        fp_c.policy(),
        "group rename not in policy hash"
    );
    // But the group is visible in the effective policy (artifact organization).
    let ep = effective(
        |flow| {
            let _ = flow.register_source_in_group("n", &MakeRows, Some("etl"));
        },
        "n",
    );
    assert_eq!(ep.group(), Some("etl"));
}

// ---------------------------------------------------------------------------
// Valid execution-class override on synchronous work assembles.
// ---------------------------------------------------------------------------

#[test]
fn valid_execution_class_override_on_synchronous_work_assembles() {
    // Blocking task moved to compute.
    let ep = effective(
        |flow| {
            let _ = flow.register_source_with(
                "sync",
                &BlockingTask,
                NodePolicy::new().execution_class(ExecutionClass::Compute),
            );
        },
        "sync",
    );
    assert_eq!(ep.execution_class(), ExecutionClass::Compute);

    // The reverse direction (compute-shaped moved to blocking) also assembles:
    // model it as a blocking task overridden back to blocking then to compute is
    // covered above; here assert a blocking->blocking (redundant) override.
    let mut flow = Flow::new();
    let _ = flow.register_source_with(
        "sync2",
        &BlockingTask,
        NodePolicy::new().execution_class(ExecutionClass::Blocking),
    );
    assert!(
        flow.finish().assemble().is_ok(),
        "synchronous override assembles"
    );
}

// ---------------------------------------------------------------------------
// Invalid execution-class override on await-bound work fails assembly, naming
// the node, and does not short-circuit T14's all-problems reporting.
// ---------------------------------------------------------------------------

#[test]
fn invalid_execution_class_override_fails_and_does_not_short_circuit() {
    let mut flow = Flow::new();
    // An await-bound task overridden to a synchronous class — invalid.
    let _ = flow.register_source_with(
        "awaitish",
        &AwaitBoundTask,
        NodePolicy::new().execution_class(ExecutionClass::Blocking),
    );
    // Plus an unrelated duplicate-name defect so we can assert BOTH surface.
    let _ = flow.register_source("dup", &MakeRows);
    let _ = flow.register_source("dup", &MakeRows);

    let err = flow
        .finish()
        .assemble()
        .expect_err("invalid override must fail assembly");

    let override_problems: Vec<_> = err
        .problems()
        .iter()
        .filter(|p| p.kind() == ProblemKind::InvalidExecutionClassOverride)
        .collect();
    assert_eq!(override_problems.len(), 1);
    assert!(
        override_problems[0].message().contains("awaitish"),
        "names the offending node"
    );
    // The all-problems path still reports the unrelated duplicate name.
    assert!(
        err.problems()
            .iter()
            .any(|p| p.kind() == ProblemKind::DuplicateNodeName),
        "the invalid override does not short-circuit all-problems reporting"
    );
}

// ---------------------------------------------------------------------------
// Declared cost vector carries per-pool native units with the memory split.
// ---------------------------------------------------------------------------

#[test]
fn declared_cost_vector_carries_native_units_with_the_memory_split() {
    let ep = effective(
        |flow| {
            let _ = flow.register_source_with(
                "heavy",
                &MakeRows,
                NodePolicy::new()
                    .working_memory(4096)
                    .output_residency(1024)
                    .blocking_threads(2),
            );
        },
        "heavy",
    );
    let cost = ep.cost();
    // memory pool: distinct working + output-residency values.
    assert_eq!(cost.working_memory(), 4096);
    assert_eq!(cost.output_residency(), 1024);
    // thread pool: the declared thread count.
    assert_eq!(cost.blocking_threads(), 2);
    // unspecified pool entries are zero.
    assert_eq!(cost.compute_threads(), 0);
}

// ---------------------------------------------------------------------------
// Setting any policy value requires no task-code change.
// ---------------------------------------------------------------------------

#[test]
fn setting_a_policy_value_requires_no_task_code_change() {
    // The SAME task value, registered twice with distinct policies.
    let mut flow = Flow::new();
    let _ = flow.register_source_with(
        "a",
        &MakeRows,
        NodePolicy::new().retries(2).timeout(Duration::from_secs(5)),
    );
    let _ = flow.register_source_with(
        "b",
        &MakeRows,
        NodePolicy::new()
            .retries(7)
            .timeout(Duration::from_secs(11)),
    );
    let pipeline = flow.finish();

    let a = pipeline
        .node(NodeId::from_name("a"))
        .unwrap()
        .effective_policy();
    let b = pipeline
        .node(NodeId::from_name("b"))
        .unwrap()
        .effective_policy();

    assert_eq!(a.retry_count(), 2);
    assert_eq!(a.timeout(), Some(Duration::from_secs(5)));
    assert_eq!(b.retry_count(), 7);
    assert_eq!(b.timeout(), Some(Duration::from_secs(11)));
    // Both nodes exist with distinct effective policies; the task code (MakeRows)
    // is byte-identical between the two registrations by construction.
    assert_ne!(a, b);
}

// ---------------------------------------------------------------------------
// Full effective policy, including defaults, is available for every node.
// ---------------------------------------------------------------------------

#[test]
fn full_effective_policy_including_defaults_is_present() {
    let mut flow = Flow::new();
    // One node with only a timeout set; one with no policy at all.
    let _ = flow.register_source_with(
        "partial",
        &MakeRows,
        NodePolicy::new().timeout(Duration::from_secs(3)),
    );
    let _ = flow.register_source("none", &MakeRows);
    let pipeline = flow.finish();

    let partial = pipeline
        .node(NodeId::from_name("partial"))
        .unwrap()
        .effective_policy();
    // The partial node carries the author-set timeout AND every defaulted field.
    assert_eq!(partial.timeout(), Some(Duration::from_secs(3)));
    assert_eq!(partial.retry_count(), 0);
    assert!(partial.cost().is_zero());
    assert_eq!(partial.trigger_rule(), TriggerRule::AllSucceeded);
    assert_eq!(partial.execution_class(), ExecutionClass::AwaitBound);
    assert_eq!(partial.group(), None);
    assert!(!partial.is_retained());
    assert!(!partial.is_durable());

    let none = pipeline
        .node(NodeId::from_name("none"))
        .unwrap()
        .effective_policy();
    // The no-policy node's effective policy is fully written out too.
    assert_eq!(none.timeout(), None);
    assert_eq!(none.retry_count(), 0);
    assert!(none.cost().is_zero());
}

// ---------------------------------------------------------------------------
// Retention flag is a policy value that feeds the policy hash.
// ---------------------------------------------------------------------------

#[test]
fn retention_flag_is_policy_and_feeds_the_policy_hash() {
    let mut a = Flow::new();
    let _ = a.register_source_with("kept", &MakeRows, NodePolicy::new().retained(true));
    let ep_a = a
        .finish()
        .node(NodeId::from_name("kept"))
        .unwrap()
        .effective_policy();
    assert!(ep_a.is_retained(), "retained present in effective policy");

    // Two pipelines identical except retention; the policy hashes differ.
    let mut p = Flow::new();
    let _ = p.register_source_with("kept", &MakeRows, NodePolicy::new().retained(true));
    let fp_p = p.finish().assemble().expect("assembles").fingerprint();

    let mut q = Flow::new();
    let _ = q.register_source_with("kept", &MakeRows, NodePolicy::new().retained(false));
    let fp_q = q.finish().assemble().expect("assembles").fingerprint();

    assert_ne!(
        fp_p.policy(),
        fp_q.policy(),
        "retention contributes to the policy hash"
    );
}

// ---------------------------------------------------------------------------
// Durability flag lives in policy and arms the assembly durable-without-contract
// check (this ticket supplies the flag; it does not re-implement the check).
// ---------------------------------------------------------------------------

#[test]
fn durability_flag_arms_the_assembly_contract_check() {
    // A durable-marked node whose output DOES implement the contract assembles,
    // and carries `durable` in its effective policy.
    let mut ok = Flow::new();
    let _ = ok.register_source_durable("snap", &MakeDurable, NodePolicy::new());
    let ok_pipeline = ok.finish();
    assert!(
        ok_pipeline
            .node(NodeId::from_name("snap"))
            .unwrap()
            .effective_policy()
            .is_durable(),
        "durable present in effective policy"
    );
    assert!(
        ok_pipeline.assemble().is_ok(),
        "durable-with-contract assembles"
    );

    // A durable-marked node whose output does NOT implement the contract fails
    // assembly through the existing check.
    let mut bad = Flow::new();
    let _ = bad.register_source_with("snap", &MakeRows, NodePolicy::new().durable(true));
    let err = bad
        .finish()
        .assemble()
        .expect_err("durable without contract must fail");
    assert!(err
        .problems()
        .iter()
        .any(|p| p.kind() == ProblemKind::DurableWithoutContract));
}

// ---------------------------------------------------------------------------
// The migrated retry knob: policy owns the full retry+backoff shape and produces
// the RetryConfig the attempt runner reads (retries/backoff have ONE home).
// ---------------------------------------------------------------------------

#[test]
fn policy_owns_retries_and_backoff_and_produces_the_runner_config() {
    let backoff = Backoff::new(Duration::from_millis(50), 2.0, Duration::from_secs(10));
    let policy = NodePolicy::new().retries(4).backoff(backoff);

    // The policy carries the full retry + backoff shape.
    assert_eq!(policy.retry_count(), 4);
    assert_eq!(
        policy.backoff_shape().nominal_delay(0),
        Duration::from_millis(50)
    );
    assert_eq!(
        policy.backoff_shape().nominal_delay(1),
        Duration::from_millis(100)
    );

    // Policy produces the RetryConfig the attempt runner consumes — retries live
    // in exactly one home (the policy), and the runner reads it from there.
    let cfg = policy.retry_config();
    // retries(4) means 4 retries beyond the first attempt: 5 total attempts.
    assert_eq!(cfg.max_attempts(), 5, "retries(n) => n+1 total attempts");
    assert_eq!(cfg.backoff().nominal_delay(1), Duration::from_millis(100));

    // The default policy yields the no-retry (single-attempt) config.
    let default_cfg = NodePolicy::new().retry_config();
    assert_eq!(default_cfg.max_attempts(), 1, "default: no retries");
}
