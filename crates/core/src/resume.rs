//! C27 · **Resume core** — the pure gate + seed/closure/demand plan algorithm
//! (arch.md `### C27 · Resume`; ticket T58).
//!
//! # What this module owns
//!
//! Given a prior run's per-node terminal states and recorded durable references,
//! and this binary's assembled [`Pipeline`](crate::flow::Pipeline), [`plan_resume`]
//! computes a **demand-driven re-execution plan** — or refuses. It is the heart
//! of resume: the machinery that lets a killed or half-finished run continue
//! instead of repeating expensive work.
//!
//! It is **pure and dependency-free** (dagr-core carries no serde, no network, no
//! clock): the caller supplies the prior run's decoded per-node facts
//! ([`PriorRun`]) and an [existence probe](ReferenceExistence) closure, and gets
//! back a [`ResumePlan`] or a [`ResumeRefusal`]. Reading the prior run artifact,
//! deriving parameters/interval, the run-store-gone refusal, and producing the
//! resumed artifact recording — everything needing serde or the run store — is the
//! **CLI**'s (`dagr_cli::contract`), which wires this plan behind the T55 `resume`
//! verb.
//!
//! # The gate (arch.md C27, "first verify")
//!
//! Before any planning, [`plan_resume`] verifies the prior run against this
//! binary, each failure a **distinct** refusal:
//!
//! - **Algorithm-version comparability** — the two fingerprint algorithm versions
//!   must match, or the hashes cannot even be compared
//!   ([`ResumeRefusal::AlgorithmVersionMismatch`], the "cannot compare" refusal).
//!   Checked first: it gates the structural comparison.
//! - **Tool version** — v1 makes no cross-tool-version resume promise
//!   ([`ResumeRefusal::ToolVersionMismatch`]).
//! - **Structural fingerprint** — a mismatch means the graph changed since the
//!   prior run; resume never re-plans a *different* graph
//!   ([`ResumeRefusal::StructuralMismatch`], carrying both fingerprints — the
//!   structural diff).
//!
//! A **policy-hash** divergence is deliberately **not** a refusal (raising a
//! timeout and resuming the expensive half-finished run is the motivating case):
//! it is surfaced as [`ResumePlan::policy_diff`] and the plan proceeds.
//!
//! # The demand-driven algorithm (arch.md C27, three steps)
//!
//! 1. **Seed** — every node whose prior terminal state was not `succeeded`, plus
//!    every node covered by a teardown that executed in the prior run (C17: a
//!    teardown may have destroyed the node's durable output, so it is not
//!    resume-safe), plus any pipeline node the prior run has no record for.
//! 2. **Close downward** — everything reachable from the seed re-runs (a node
//!    re-runs when any of its data or ordering upstreams re-runs).
//! 3. **Resolve demand upward** — a re-running node demands the values of its
//!    **data inputs**; a demanded producer that is durable with an intact
//!    reference has its slot filled by [rehydration](ResumePlan::rehydrate); a
//!    demanded producer that is **not** durable (an in-memory value that cannot be
//!    rehydrated) joins the must-run set and cascades its own demands upward.
//!
//! Every prior success left outside the must-run set is
//! [`satisfied-from-prior`](ResumePlan::satisfied_from_prior) — durable or not,
//! because an undemanded value never needs rehydrating and the node's *effect*
//! stands (the cleanup-after-publish shape). Resuming a fully successful run has an
//! empty seed and is a no-op.
//!
//! # The in-memory-producer pressure (arch.md C27, stated plainly to developers)
//!
//! Nodes whose outputs were **in-memory** values cannot be rehydrated: the moment
//! a re-running consumer demands their value, they re-execute, and their demands
//! cascade upward. This is a genuine property of the design, not a bug — it
//! creates useful pressure to make expensive stage boundaries produce **durable,
//! addressable** outputs (C10 authoring guidance). If your expensive producer's
//! output is in-memory, a downstream re-run forces the producer to re-run too;
//! mark it durable to be satisfied-from-prior and rehydrated instead.
//!
//! # Out of scope (T54b / T59, permanent non-goals)
//!
//! Scratch **carry-forward** for re-executing nodes is T54b — this plan only
//! surfaces [which nodes re-execute](ResumePlan::must_run) so T54b can copy their
//! retained scratch forward. The exhaustive behavioural suite is T59. Resume never
//! re-plans a *different* graph, never backfills, never schedules — those are
//! permanent scope boundaries.

use std::collections::{BTreeMap, BTreeSet};

use crate::context::TerminalState;
use crate::flow::Pipeline;

/// The prior run's decoded facts about **one** node, as the resume plan needs
/// them (the CLI reads these out of the prior run artifact; core stays serde-free).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PriorNode {
    /// The node's terminal state in the prior run. `succeeded` is the only state
    /// that can be carried forward; anything else seeds re-execution.
    pub terminal: TerminalState,
    /// The durable reference the node's succeeded attempt recorded, if any
    /// (C27/T57). `None` for a non-durable node — an in-memory value that cannot
    /// be rehydrated.
    pub durable_reference: Option<String>,
    /// The run identity this node's success **originated** in (C22/C27). For a
    /// node that ran in the prior run this is the prior run's own id; for a node
    /// the prior run itself carried `satisfied-from-prior`, it is the earlier
    /// originating run, so the identity is carried forward across generations.
    pub originating_run: String,
}

/// The prior run's decoded facts the resume plan is computed against — the
/// serde-free input the CLI assembles from the prior run artifact.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PriorRun {
    /// The prior run's recorded **structural fingerprint** (C21). Compared against
    /// this binary's; a mismatch refuses ([`ResumeRefusal::StructuralMismatch`]).
    pub structural_fingerprint: u64,
    /// The prior run's recorded **policy hash** (C21). A divergence is a
    /// proceed-with-diff, never a refusal.
    pub policy_hash: u64,
    /// The fingerprint **algorithm version** the prior hashes were computed under
    /// (C21). Incomparable to this binary's is the "cannot compare" refusal.
    pub algorithm_version: u64,
    /// The **tool version** that recorded the prior run. v1 makes no
    /// cross-tool-version resume promise (a mismatch refuses).
    pub tool_version: String,
    /// The prior run's per-node facts, keyed by node identity name.
    pub nodes: BTreeMap<String, PriorNode>,
}

/// The outcome of a cheap **existence probe** of a durable reference (arch.md
/// C27; T0.8 ADR §7): is the prior durable output still there?
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReferenceExistence {
    /// The referent is present — the value can be rehydrated.
    Present,
    /// The referent is **gone** (a deleted object). A demanded durable reference
    /// that probes absent is a **dangling** reference: it fails the resume *plan*
    /// up front ([`ResumeRefusal::DanglingReference`]), not the eleventh executing
    /// node.
    Absent,
    /// The probe could not determine presence (a transient store error). The plan
    /// proceeds — only a definite `Absent` fails it — leaving a genuine dangling
    /// reference to surface at rehydration if it truly is gone.
    CannotDetermine,
}

/// A per-node **policy diff** entry: a node whose effective policy hash contribution
/// differs between the prior run and this binary. Surfaced informationally when the
/// two runs' policy hashes diverge — resume proceeds regardless (arch.md C27).
///
/// The resume core has only the two aggregate policy hashes to compare (it is
/// pure over the fingerprint slot, not the full per-node policy), so the diff it
/// produces is the single aggregate fact "the policy hashes differ". The
/// per-node presentation (which node's timeout was raised) is the CLI's to render
/// from the graph + prior artifact if it chooses; the plan records that a
/// divergence exists so the caller prints it rather than refusing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyDiff {
    /// The prior run's policy hash.
    pub prior: u64,
    /// This binary's policy hash.
    pub current: u64,
}

/// A **refusal** — resume verified the prior run against this binary and would not
/// proceed (arch.md C27). Each variant is a **distinct**, testable cause; the CLI
/// maps every one to the C26 resume-refusal exit code and prints the carried
/// detail.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResumeRefusal {
    /// The prior run's **structural fingerprint** differs from this binary's — the
    /// graph changed (a node renamed/rewired). Resume cannot re-plan a different
    /// graph; it refuses and the two fingerprints are the structural diff.
    StructuralMismatch {
        /// The prior run's structural fingerprint.
        prior: u64,
        /// This binary's structural fingerprint.
        current: u64,
    },
    /// The prior run's fingerprint **algorithm version** is not comparable to this
    /// binary's — the digests cannot be compared at all (the "cannot compare"
    /// refusal, distinct from a structural mismatch).
    AlgorithmVersionMismatch {
        /// The prior run's algorithm version.
        prior: u64,
        /// This binary's algorithm version.
        current: u64,
    },
    /// The prior run was recorded by a **different tool version** — v1 makes no
    /// cross-tool-version resume promise (C27 / Stability), and this refusal is
    /// its documentation.
    ToolVersionMismatch {
        /// The prior run's tool version.
        prior: String,
        /// This binary's tool version.
        current: String,
    },
    /// A candidate durable node's referenced object is **gone** — a dangling
    /// reference. It fails the resume *plan* up front (before any node executes),
    /// naming the offending node and reference.
    DanglingReference {
        /// The node whose durable reference is dangling.
        node: String,
        /// The dangling reference (the offending reference, named).
        reference: String,
    },
}

impl std::fmt::Display for ResumeRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResumeRefusal::StructuralMismatch { prior, current } => write!(
                f,
                "resume refused: the prior run's structural fingerprint (fnv:{prior:016x}) differs \
                 from this binary's (fnv:{current:016x}) — the graph changed since the prior run \
                 (a node was renamed, rewired, or added/removed). Resume cannot re-plan a \
                 different graph.",
            ),
            ResumeRefusal::AlgorithmVersionMismatch { prior, current } => write!(
                f,
                "resume refused: cannot compare — the prior run's fingerprint algorithm version \
                 ({prior}) is not comparable to this binary's ({current}). A fingerprint from a \
                 different algorithm version says nothing about topology.",
            ),
            ResumeRefusal::ToolVersionMismatch { prior, current } => write!(
                f,
                "resume refused: the prior run was recorded by tool version `{prior}`, this binary \
                 is `{current}` — v1 makes no cross-tool-version resume promise.",
            ),
            ResumeRefusal::DanglingReference { node, reference } => write!(
                f,
                "resume refused: node `{node}`'s durable output is gone — its recorded reference \
                 `{reference}` no longer exists in storage (a dangling reference). The resume plan \
                 fails before any node executes.",
            ),
        }
    }
}

impl std::error::Error for ResumeRefusal {}

/// A computed **resume plan** (arch.md C27): what must re-execute, what is carried
/// forward satisfied-from-prior (with its originating run), and which durable
/// references are rehydrated to fill re-running consumers' slots.
///
/// The plan is the hand-off the resume verb executes: it re-runs exactly
/// [`must_run`](Self::must_run) (T54b copies their retained scratch forward),
/// fills re-running consumers' input slots by rehydrating the producers in
/// [`rehydrate`](Self::rehydrate), and records every node in
/// [`satisfied_from_prior`](Self::satisfied_from_prior) as `satisfied-from-prior`
/// carrying its originating run identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResumePlan {
    seed: BTreeSet<String>,
    must_run: BTreeSet<String>,
    satisfied_from_prior: BTreeMap<String, String>,
    rehydrate: BTreeMap<String, String>,
    policy_diff: Option<PolicyDiff>,
}

impl ResumePlan {
    /// The **must-run seed** (step 1): every node whose prior terminal state was
    /// not `succeeded`, plus every node covered by a teardown that executed in the
    /// prior run, plus any pipeline node the prior run has no record for.
    #[must_use]
    pub fn seed(&self) -> &BTreeSet<String> {
        &self.seed
    }

    /// The **must-run set**: the seed, closed downward (everything reachable
    /// re-runs), plus every demanded non-durable producer pulled in upward. These
    /// nodes re-execute; T54b copies their retained scratch forward.
    #[must_use]
    pub fn must_run(&self) -> &BTreeSet<String> {
        &self.must_run
    }

    /// The **satisfied-from-prior** nodes: every prior success left outside the
    /// must-run set, mapped to its **originating run identity** (C22/C27). Not
    /// re-executed; its prior success is carried forward — durable or not.
    #[must_use]
    pub fn satisfied_from_prior(&self) -> &BTreeMap<String, String> {
        &self.satisfied_from_prior
    }

    /// The **rehydration** map: a durable, satisfied-from-prior producer demanded
    /// by a re-running consumer, mapped to the durable reference whose value fills
    /// that consumer's input slot. A node here is never in
    /// [`must_run`](Self::must_run) — its value is rehydrated, not recomputed.
    #[must_use]
    pub fn rehydrate(&self) -> &BTreeMap<String, String> {
        &self.rehydrate
    }

    /// The **policy diff**, present when the prior run's policy hash diverges from
    /// this binary's (arch.md C27). A policy divergence is a *proceed-with-diff*,
    /// never a refusal — the CLI prints this and plans regardless.
    #[must_use]
    pub fn policy_diff(&self) -> Option<&PolicyDiff> {
        self.policy_diff.as_ref()
    }
}

/// Compute the C27 **resume plan** for `pipeline` against a `prior` run, or refuse
/// (arch.md `### C27 · Resume`).
///
/// The gate runs first (algorithm-version comparability, then tool version, then
/// structural fingerprint — each a distinct [`ResumeRefusal`]); a policy-hash
/// divergence proceeds with a [diff](ResumePlan::policy_diff). Then the
/// demand-driven algorithm computes the seed, closes it downward, and resolves
/// demand upward — existence-probing every **demanded** durable reference (a
/// definite absence is a [`ResumeRefusal::DanglingReference`]).
///
/// `probe` is the cheap existence probe: given a `(node, reference)` it reports
/// whether the durable referent is [present, absent, or cannot-determine](ReferenceExistence).
/// It is called **only** for a durable producer whose value a re-running consumer
/// demands — an undemanded durable success is never probed (its value is never
/// rehydrated), so a dangling reference on an undemanded node does not fail the
/// plan.
///
/// # Errors
///
/// Returns a [`ResumeRefusal`] when the gate rejects the prior run (structural /
/// algorithm-version / tool-version mismatch) or a demanded durable reference is
/// dangling.
pub fn plan_resume<P>(
    pipeline: &Pipeline,
    prior: &PriorRun,
    this_tool_version: &str,
    probe: P,
) -> Result<ResumePlan, ResumeRefusal>
where
    P: Fn(&str, &str) -> ReferenceExistence,
{
    let fingerprint = pipeline.fingerprint();

    // --- The gate (arch.md C27, "first verify") ------------------------------
    // Algorithm-version comparability gates everything: hashes from different
    // algorithm versions cannot be compared at all.
    if prior.algorithm_version != fingerprint.algorithm_version() {
        return Err(ResumeRefusal::AlgorithmVersionMismatch {
            prior: prior.algorithm_version,
            current: fingerprint.algorithm_version(),
        });
    }
    // v1 makes no cross-tool-version resume promise.
    if prior.tool_version != this_tool_version {
        return Err(ResumeRefusal::ToolVersionMismatch {
            prior: prior.tool_version.clone(),
            current: this_tool_version.to_string(),
        });
    }
    // The structural fingerprint gates resume; a mismatch is the graph changing.
    if prior.structural_fingerprint != fingerprint.structural() {
        return Err(ResumeRefusal::StructuralMismatch {
            prior: prior.structural_fingerprint,
            current: fingerprint.structural(),
        });
    }
    // A policy-hash divergence proceeds with a diff (the raised-timeout case).
    let policy_diff = (prior.policy_hash != fingerprint.policy()).then(|| PolicyDiff {
        prior: prior.policy_hash,
        current: fingerprint.policy(),
    });

    // --- Build the graph adjacency the algorithm reads -----------------------
    // For each node: its data-input producer names (the values it DEMANDS) and its
    // full upstream set (data + ordering — what a downward closure follows).
    let mut data_inputs: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut all_upstreams: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut is_durable: BTreeMap<String, bool> = BTreeMap::new();
    let mut node_names: BTreeSet<String> = BTreeSet::new();
    for node in pipeline.nodes() {
        let name = node.name().to_string();
        node_names.insert(name.clone());
        is_durable.insert(name.clone(), node.policy().is_durable());
        let mut inputs = Vec::new();
        for edge in node.data_edges() {
            if let Some(producer) = pipeline.node(edge.upstream()) {
                inputs.push(producer.name().to_string());
            }
        }
        let mut ups = inputs.clone();
        for edge in node.ordering_edges() {
            if let Some(producer) = pipeline.node(edge.upstream()) {
                ups.push(producer.name().to_string());
            }
        }
        data_inputs.insert(name.clone(), inputs);
        all_upstreams.insert(name, ups);
    }

    // --- Step 1: the must-run seed ------------------------------------------
    // A node whose prior terminal was not `succeeded`, a teardown-covered node, or
    // a pipeline node the prior run never recorded.
    let teardown_covered: BTreeSet<String> = pipeline
        .teardown_covered_nodes()
        .into_values()
        .flatten()
        .collect();
    let mut seed: BTreeSet<String> = BTreeSet::new();
    for name in &node_names {
        let prior_state = prior.nodes.get(name).map(|n| n.terminal);
        let not_succeeded = prior_state != Some(TerminalState::Succeeded);
        if not_succeeded || teardown_covered.contains(name) {
            seed.insert(name.clone());
        }
    }

    // --- Steps 2 + 3 to a joint fixpoint ------------------------------------
    // must_run starts at the seed. Repeatedly: close downward (any node whose
    // upstream re-runs, re-runs) and resolve demand upward (a re-running node
    // demands its data inputs; a non-durable demanded producer joins must_run).
    // Both grow must_run monotonically, so iterating to a fixpoint terminates.
    let mut must_run = seed.clone();
    let mut rehydrate: BTreeMap<String, String> = BTreeMap::new();
    loop {
        let before = must_run.len();

        // Downward closure: a node re-runs if any upstream (data or ordering) is
        // in must_run.
        for (name, ups) in &all_upstreams {
            if must_run.contains(name) {
                continue;
            }
            if ups.iter().any(|u| must_run.contains(u)) {
                must_run.insert(name.clone());
            }
        }

        // Upward demand: every re-running node demands its data-input producers.
        let demanders: Vec<String> = must_run.iter().cloned().collect();
        for consumer in demanders {
            let Some(inputs) = data_inputs.get(&consumer) else {
                continue;
            };
            for producer in inputs.clone() {
                if must_run.contains(&producer) {
                    continue; // already re-running; nothing to rehydrate
                }
                // Only a PRIOR SUCCESS can be carried forward / rehydrated. A
                // non-succeeded producer is already in the seed (hence must_run),
                // so reaching here means the producer succeeded before.
                let durable_ref = prior
                    .nodes
                    .get(&producer)
                    .filter(|p| p.terminal == TerminalState::Succeeded)
                    .and_then(|p| p.durable_reference.clone())
                    .filter(|_| is_durable.get(&producer).copied().unwrap_or(false));

                match durable_ref {
                    Some(reference) => {
                        // A demanded durable producer: existence-probe it. A
                        // definite absence fails the plan up front (dangling).
                        match probe(&producer, &reference) {
                            ReferenceExistence::Absent => {
                                return Err(ResumeRefusal::DanglingReference {
                                    node: producer,
                                    reference,
                                });
                            }
                            ReferenceExistence::Present
                            | ReferenceExistence::CannotDetermine => {
                                rehydrate.insert(producer, reference);
                            }
                        }
                    }
                    None => {
                        // A demanded NON-durable (in-memory) producer cannot be
                        // rehydrated: it joins the must-run set and cascades its own
                        // demands on the next iteration. If it was previously slated
                        // for rehydration, that no longer applies.
                        rehydrate.remove(&producer);
                        must_run.insert(producer);
                    }
                }
            }
        }

        if must_run.len() == before {
            break; // fixpoint: nothing new joined must_run
        }
    }

    // --- Satisfied-from-prior marking ---------------------------------------
    // Every prior success left outside the must-run set is satisfied-from-prior —
    // durable or not — carrying its originating run identity.
    let mut satisfied_from_prior: BTreeMap<String, String> = BTreeMap::new();
    for name in &node_names {
        if must_run.contains(name) {
            continue;
        }
        if let Some(prior_node) = prior.nodes.get(name) {
            if prior_node.terminal == TerminalState::Succeeded {
                satisfied_from_prior.insert(name.clone(), prior_node.originating_run.clone());
            }
        }
    }

    Ok(ResumePlan {
        seed,
        must_run,
        satisfied_from_prior,
        rehydrate,
        policy_diff,
    })
}
