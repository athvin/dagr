//! C11 · Readiness tracker — the pure decision engine that decides **what is
//! eligible to run, and when** (arch.md `### C11 · Readiness tracker`).
//!
//! # The countdown model
//!
//! The tracker maintains a **per-node remaining-dependency countdown**, seeded
//! from T14's precomputed [remaining-dependency
//! counts](crate::assembly::AssemblyArtifact::remaining_dependency_count). When
//! any node reaches a terminal state, the driver calls
//! [`notify_terminal`](ReadinessTracker::notify_terminal); each **dependent** of
//! the notified node is decremented, and a dependent whose countdown reaches zero
//! (every upstream now terminal) has its trigger rule evaluated. A node with zero
//! dependencies starts at countdown zero and is surfaced in the
//! [initial-ready frontier](ReadinessTracker::initial_ready), so the driver has a
//! starting frontier without any notification.
//!
//! # The all-upstreams-terminal evaluation gate
//!
//! A node's trigger rule is evaluated **only once its countdown reaches zero** —
//! never on a partial result (arch.md Vocabulary: *"a rule never fires early on a
//! partial result"*). Until the last upstream reaches a terminal state, the node
//! is neither ready nor propagated: it simply waits. This is what un-batches
//! readiness — a node becomes ready the instant its own dependencies allow, never
//! stalled behind an unrelated slow branch that happens to share a level.
//!
//! # Fires / can-never-fire → propagated state (the normative T0.4 table)
//!
//! Once every upstream is terminal, the node's rule is evaluated against the
//! multiset of upstream terminal states by [`evaluate_rule`], **exactly** per the
//! T0.4 decision record
//! (`docs/implementation/010-T0.4-trigger-rule-and-state-tables.md`, §5):
//!
//! - A node whose rule **fires** becomes **ready** ([`Decision::Ready`]).
//! - A node whose rule **can never fire** is **immediately assigned its
//!   propagated terminal state without executing**
//!   ([`Decision::PropagatedTerminal`]): `upstream-failed`, `upstream-skipped`, or
//!   (for an `any-failed` contingency that never arose) `skipped`. The
//!   `upstream-skipped` / `upstream-failed` assignments carry the **originating
//!   node's identity** (Vocabulary; T0.4).
//!
//! For `all-succeeded` (the only rule M1 wires onto runtime nodes; §5a): fires
//! when every upstream is success-like; otherwise `upstream-skipped` when every
//! non-success upstream is skip-like, `cancelled` when every non-success upstream
//! is stop-like, and `upstream-failed` otherwise (any failure-like upstream, or a
//! cross-class mix). `satisfied-from-prior` counts **success-like**, so a resumed
//! prior success satisfies a downstream `all-succeeded` (C11 "covered explicitly").
//!
//! A **propagated-terminal assignment is itself a terminal notification** that
//! cascades to that node's dependents, without any intervening execution — a
//! failure that deadens a chain of `all-succeeded` nodes reaches the far end in a
//! single [`notify_terminal`](ReadinessTracker::notify_terminal) call.
//!
//! # The full rule interface, `all-succeeded` behaviour in M1
//!
//! [`evaluate_rule`] accepts **all three** rules from T0.4's closed set
//! ([`AllSucceeded`](TriggerRule::AllSucceeded),
//! [`AllTerminal`](TriggerRule::AllTerminal),
//! [`AnyFailed`](TriggerRule::AnyFailed)), so T34 can enable `all-terminal` and
//! `any-failed` **runtime firing** without reshaping the tracker. M1 exercises the
//! `all-succeeded` fires / can-never-fire branches on runtime nodes; the other two
//! table entries stay reachable through the same seam (their runtime firing —
//! stop-mode contingency admission and the like — is T34, C15).
//!
//! # Boundaries (what this is NOT)
//!
//! The tracker is a **pure decision engine**: no spawning, no scheduling, no
//! timers, no event writing, no I/O. It consumes terminal-state notifications and
//! emits ready-node and propagated-terminal decisions — nothing else. The run-loop
//! driver (T24) admits and spawns work and feeds outcomes back; admission control
//! (C12), failure-policy runtime (C15/T34), teardown (C17), and the event stream
//! (C19) all belong to other tickets. The tracker's
//! [`pending_count`](ReadinessTracker::pending_count) gives T24 the *"nothing
//! pending"* half of the run-end condition; the *"in flight"* half is the driver's.

use std::collections::BTreeMap;

use crate::assembly::AssemblyArtifact;
use crate::binding::{DataEdge, TriggerRule};
use crate::context::TerminalState;
use crate::flow::Pipeline;
use crate::handle::NodeId;

/// The **state class** a terminal state belongs to — the total four-class
/// partition trigger rules are defined over (arch.md Vocabulary; T0.4 §3).
///
/// Every one of the nine terminal states belongs to **exactly one** class, which
/// is what makes [`evaluate_rule`] total over the taxonomy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum StateClass {
    /// `succeeded`, `satisfied-from-prior`.
    Success,
    /// `skipped`, `upstream-skipped`.
    Skip,
    /// `failed`, `timed-out`, `abandoned`, `upstream-failed`.
    Failure,
    /// `cancelled`.
    Stop,
}

/// The state class of a terminal state (T0.4 §3 — the total partition).
const fn class_of(state: TerminalState) -> StateClass {
    match state {
        TerminalState::Succeeded | TerminalState::SatisfiedFromPrior => StateClass::Success,
        TerminalState::Skipped | TerminalState::UpstreamSkipped => StateClass::Skip,
        TerminalState::Failed
        | TerminalState::TimedOut
        | TerminalState::Abandoned
        | TerminalState::UpstreamFailed => StateClass::Failure,
        TerminalState::Cancelled => StateClass::Stop,
    }
}

/// The outcome of evaluating a node's trigger rule against its upstreams' terminal
/// states, once **every** upstream is terminal (T0.4 §5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuleOutcome {
    /// The rule **fires**: the node becomes ready and executes.
    Fires,
    /// The rule **can never fire**: the node is immediately assigned this
    /// propagated terminal state without executing. Never [`Succeeded`] — a
    /// propagated state is always one of `upstream-failed`, `upstream-skipped`,
    /// `cancelled`, or (for `any-failed`) `skipped`.
    ///
    /// [`Succeeded`]: TerminalState::Succeeded
    Propagate(TerminalState),
}

/// Evaluate a node's trigger `rule` against its `upstreams`' terminal states,
/// **exactly** per T0.4's fires / can-never-fire decision table (§5).
///
/// The caller must invoke this **only once every upstream is terminal** (the
/// all-upstreams-terminal gate — arch.md Vocabulary); passing a partial picture
/// would violate the invariant the table is total over. `upstreams` is the
/// multiset of the node's upstreams' terminal states, in any order — the outcome
/// depends only on the *classes* present (T0.4 §5), never on order.
///
/// The seam accepts **all three** rules from T0.4's closed set so `all-terminal`
/// and `any-failed` are reachable without reshaping the tracker (their *runtime
/// firing* is T34); M1 wires only `all-succeeded` onto runtime nodes.
///
/// # Panics
///
/// Debug-asserts that `upstreams` is non-empty. A node with zero upstreams never
/// reaches the gate through decrement (it starts ready), so this is only ever
/// called on a node with at least one upstream.
#[must_use]
pub fn evaluate_rule(rule: TriggerRule, upstreams: &[TerminalState]) -> RuleOutcome {
    debug_assert!(
        !upstreams.is_empty(),
        "evaluate_rule is called only once every upstream is terminal; a zero-upstream node \
         starts ready and never reaches the gate",
    );
    match rule {
        TriggerRule::AllSucceeded => eval_all_succeeded(upstreams),
        TriggerRule::AllTerminal => {
            // §5b: fires whenever every upstream is terminal, regardless of class;
            // no can-never-fire case, never propagates failure. The gate already
            // guarantees every upstream is terminal.
            RuleOutcome::Fires
        }
        TriggerRule::AnyFailed => eval_any_failed(upstreams),
    }
}

/// T0.4 §5a — `all-succeeded`. Fires when every upstream is success-like;
/// otherwise `upstream-skipped` (all non-success skip-like), `cancelled` (all
/// non-success stop-like), or `upstream-failed` (any failure-like, or a
/// cross-class mix).
fn eval_all_succeeded(upstreams: &[TerminalState]) -> RuleOutcome {
    let mut any_non_success = false;
    let mut all_non_success_skip = true;
    let mut all_non_success_stop = true;
    for &state in upstreams {
        match class_of(state) {
            StateClass::Success => {}
            StateClass::Skip => {
                any_non_success = true;
                all_non_success_stop = false;
            }
            StateClass::Stop => {
                any_non_success = true;
                all_non_success_skip = false;
            }
            StateClass::Failure => {
                any_non_success = true;
                all_non_success_skip = false;
                all_non_success_stop = false;
            }
        }
    }
    if !any_non_success {
        return RuleOutcome::Fires;
    }
    if all_non_success_skip {
        RuleOutcome::Propagate(TerminalState::UpstreamSkipped)
    } else if all_non_success_stop {
        RuleOutcome::Propagate(TerminalState::Cancelled)
    } else {
        RuleOutcome::Propagate(TerminalState::UpstreamFailed)
    }
}

/// T0.4 §5c — `any-failed`. Fires when at least one upstream is failure-like (a
/// transitively `upstream-failed` upstream counts); otherwise the contingency
/// never arose → `skipped`.
fn eval_any_failed(upstreams: &[TerminalState]) -> RuleOutcome {
    if upstreams
        .iter()
        .any(|&s| class_of(s) == StateClass::Failure)
    {
        RuleOutcome::Fires
    } else {
        RuleOutcome::Propagate(TerminalState::Skipped)
    }
}

/// One downstream decision a [`notify_terminal`](ReadinessTracker::notify_terminal)
/// unlocked (C11 hand-off, T0.4 §8).
///
/// The driver (T24) acts on these: it admits and spawns a [`Ready`](Decision::Ready)
/// node, and records a [`PropagatedTerminal`](Decision::PropagatedTerminal) node's
/// state directly (that node never executes). A propagated-terminal decision has
/// already been folded back into the tracker's own state before it is returned, so
/// its own dependents are decremented in the same call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    /// The node's trigger rule fired: it is **ready to run**. The driver admits
    /// and spawns it, then reports its executed-terminal outcome back through
    /// [`notify_terminal`](ReadinessTracker::notify_terminal).
    Ready(NodeId),
    /// The node's rule can never fire: it is **assigned a propagated terminal
    /// state without executing** (C11; T0.4 §5). The driver records the state; it
    /// must **not** run or re-notify the node.
    PropagatedTerminal {
        /// The node assigned the propagated state.
        node: NodeId,
        /// The propagated terminal state — `upstream-failed`, `upstream-skipped`,
        /// `cancelled`, or (for `any-failed`) `skipped`.
        state: TerminalState,
        /// The **originating node's identity** the propagation carries: the
        /// upstream whose failure or skip made the rule unsatisfiable (Vocabulary;
        /// T0.4). For a cascade, this is the immediate upstream that carried the
        /// deciding class into this node.
        origin: NodeId,
    },
}

/// Per-node bookkeeping the tracker maintains, keyed by node name for
/// determinism (node identity is name-derived — T0.7).
#[derive(Debug, Clone)]
struct NodeState {
    /// The node's opaque identity (name-derived).
    id: NodeId,
    /// The node's trigger rule (from the pipeline; T0.4 / T11).
    rule: TriggerRule,
    /// Remaining upstreams not yet terminal — the C11 countdown. Reaches zero when
    /// every upstream is terminal, gating rule evaluation.
    remaining: u32,
    /// The terminal states of upstreams that have already terminated, paired with
    /// the upstream's identity — the picture the rule is evaluated against, and the
    /// source of the propagated-state origin.
    upstream_states: Vec<(NodeId, TerminalState)>,
    /// This node's recorded terminal state once decided (executed-terminal from the
    /// driver, or propagated-terminal by the tracker), else `None` (pending).
    decided: Option<TerminalState>,
    /// The names of this node's direct dependents — decremented when this node
    /// terminates. Sorted by name for a deterministic decision order.
    dependents: Vec<String>,
}

/// The C11 **readiness tracker**: a pure state machine that, given upstream
/// terminal-state notifications, decides the next ready nodes and the immediate
/// propagated-terminal assignments (arch.md `### C11 · Readiness tracker`).
///
/// Build one from an immutable [`Pipeline`] and its [`AssemblyArtifact`] with
/// [`new`](ReadinessTracker::new); read the starting frontier from
/// [`initial_ready`](ReadinessTracker::initial_ready); drive it with
/// [`notify_terminal`](ReadinessTracker::notify_terminal) as each node reaches a
/// terminal state; and query
/// [`pending_count`](ReadinessTracker::pending_count) for the *"nothing pending"*
/// run-end signal. See the [module docs](self) for the countdown model, the
/// all-upstreams-terminal gate, and the fires / can-never-fire → propagated-state
/// mapping (T0.4 is the normative table).
///
/// The tracker **spawns nothing, schedules nothing, times nothing, and writes no
/// events** — it is the pure readiness half of the run loop; the driver (T24) owns
/// the rest.
#[derive(Debug, Clone)]
pub struct ReadinessTracker {
    /// Per-node bookkeeping, keyed by identity name (order-insensitive,
    /// deterministic — the T0.7 canonical key).
    nodes: BTreeMap<String, NodeState>,
    /// The source frontier: nodes whose countdown was zero at construction (no
    /// dependencies), ready without any notification. In name order.
    initial_ready: Vec<NodeId>,
    /// How many nodes remain pending (not yet decided). The run-end *"nothing
    /// pending"* signal reaches zero when this does.
    pending: usize,
}

impl ReadinessTracker {
    /// Build a tracker over an immutable `pipeline` and its precomputed
    /// `artifact`.
    ///
    /// The per-node countdown is seeded from T14's
    /// [remaining-dependency counts](AssemblyArtifact::remaining_dependency_count),
    /// the dependents map and trigger rules are read from the pipeline's recorded
    /// edges, and the [initial-ready frontier](Self::initial_ready) collects every
    /// zero-dependency (source) node — ready from the start. The `pipeline` and
    /// `artifact` must describe the same assembled graph (pass the artifact
    /// [`assemble`](Pipeline::assemble) returned for this pipeline); the tracker
    /// copies what it needs and borrows neither afterward.
    #[must_use]
    pub fn new(pipeline: &Pipeline, artifact: &AssemblyArtifact) -> Self {
        Self::new_with_ordering(pipeline, artifact, &BTreeMap::new())
    }

    /// Build a tracker that additionally honours **run-level ordering upstreams**
    /// (C15 / T34): `ordering` maps a node's name to the names of nodes it must
    /// run *after* even though it consumes no value from them.
    ///
    /// This is the seam by which a **consume-nothing node with a non-default
    /// trigger rule** (`all-terminal` / `any-failed`) acquires the upstreams its
    /// rule is evaluated against — the runtime firing of the non-default rules
    /// (arch.md C15) that T18 left to T34. An ordering upstream counts toward the
    /// node's countdown and contributes its terminal state to the picture
    /// [`evaluate_rule`] sees, exactly like a data upstream, but delivers **no
    /// value** — so it is the only kind of upstream a non-default-rule node may
    /// have (data upstreams force `all-succeeded`, C3/C4). The graph-authoring
    /// ordering-edge API, its compile-time attach rules, and its fingerprint /
    /// render treatment are **T50**; this run-level seam seeds only the tracker's
    /// dependency structure and touches neither the graph artifact nor the
    /// fingerprint.
    ///
    /// An empty `ordering` map yields exactly the same tracker as
    /// [`new`](Self::new) — the seam is purely additive, so the M1 `all-succeeded`
    /// data-edge behaviour is unchanged. An ordering entry naming an unknown
    /// upstream (not in the pipeline) is ignored, mirroring how [`new`](Self::new)
    /// ignores a data edge whose upstream is absent.
    #[must_use]
    pub fn new_with_ordering(
        pipeline: &Pipeline,
        artifact: &AssemblyArtifact,
        ordering: &BTreeMap<String, Vec<String>>,
    ) -> Self {
        // Resolve each node's distinct, in-pipeline ordering upstreams (name-keyed,
        // deduped) — the extra upstreams the seam contributes beyond data edges.
        let ordering_upstreams = |name: &str| -> Vec<String> {
            let mut ups: Vec<String> = ordering
                .get(name)
                .into_iter()
                .flatten()
                .filter(|up| pipeline.node(NodeId::from_name(up)).is_some())
                .cloned()
                .collect();
            ups.sort();
            ups.dedup();
            ups
        };

        // First pass: build each node's state with its seeded countdown and rule.
        // The countdown is the precomputed data-dependency count PLUS the count of
        // distinct ordering upstreams (both must be terminal before the rule fires).
        let mut nodes: BTreeMap<String, NodeState> = BTreeMap::new();
        for node in pipeline.nodes() {
            let data_remaining = artifact
                .remaining_dependency_count(node.id())
                // A node always has a precomputed count; fall back to its edge
                // count if (impossibly) absent, so construction is total.
                .unwrap_or_else(|| distinct_upstream_count(pipeline, node.data_edges()));
            let ordering_remaining =
                u32::try_from(ordering_upstreams(node.name()).len()).unwrap_or(u32::MAX);
            nodes.insert(
                node.name().to_string(),
                NodeState {
                    id: node.id(),
                    rule: node.trigger_rule(),
                    remaining: data_remaining.saturating_add(ordering_remaining),
                    upstream_states: Vec::new(),
                    decided: None,
                    dependents: Vec::new(),
                },
            );
        }

        // Second pass: wire the dependents map (upstream name -> dependent names),
        // over both data upstreams and the run-level ordering upstreams.
        for node in pipeline.nodes() {
            let mut upstream_names: Vec<String> = node
                .data_edges()
                .iter()
                .filter_map(|e| pipeline.node(e.upstream()).map(|u| u.name().to_string()))
                .collect();
            upstream_names.extend(ordering_upstreams(node.name()));
            upstream_names.sort();
            upstream_names.dedup();
            for up in upstream_names {
                if let Some(up_state) = nodes.get_mut(&up) {
                    up_state.dependents.push(node.name().to_string());
                }
            }
        }
        // Deterministic decision order: dependents in name order.
        for state in nodes.values_mut() {
            state.dependents.sort();
            state.dependents.dedup();
        }

        // The source frontier: every zero-dependency node, in name order.
        let initial_ready: Vec<NodeId> = nodes
            .values()
            .filter(|s| s.remaining == 0)
            .map(|s| s.id)
            .collect();
        let pending = nodes.len();

        Self {
            nodes,
            initial_ready,
            pending,
        }
    }

    /// The **initial-ready frontier**: every node with zero dependencies, ready
    /// from the start without any notification (C11). In deterministic name order.
    /// This is the starting frontier the driver (T24) admits first.
    #[must_use]
    pub fn initial_ready(&self) -> &[NodeId] {
        &self.initial_ready
    }

    /// The current **remaining-dependency countdown** for `node` — how many of its
    /// upstreams have not yet reached a terminal state (C11) — or `None` if no node
    /// carries that identity. Reaches zero when every upstream is terminal, gating
    /// the node's trigger-rule evaluation.
    #[must_use]
    pub fn remaining_dependencies(&self, node: NodeId) -> Option<u32> {
        self.node_state(node).map(|s| s.remaining)
    }

    /// Whether `node` has been **decided** — assigned a terminal state, either an
    /// executed-terminal from the driver or a propagated-terminal by the tracker
    /// (C11). A decided node is no longer pending.
    #[must_use]
    pub fn is_decided(&self, node: NodeId) -> bool {
        self.node_state(node).is_some_and(|s| s.decided.is_some())
    }

    /// The **terminal state** `node` was decided with, or `None` if it is still
    /// pending (or no node carries that identity). Every node ends in exactly one
    /// terminal state, assigned exactly once (Vocabulary).
    #[must_use]
    pub fn terminal_state(&self, node: NodeId) -> Option<TerminalState> {
        self.node_state(node).and_then(|s| s.decided)
    }

    /// The number of nodes still **pending** (not yet decided) — the driver's
    /// *"nothing pending"* half of the run-end condition (C11; the *"in flight"*
    /// half is the driver's, T24). Reaches zero exactly when every node has a
    /// terminal state.
    #[must_use]
    pub fn pending_count(&self) -> usize {
        self.pending
    }

    /// Notify the tracker that `node` reached terminal `state` — the operation the
    /// driver (T24) calls when a node reaches **any** terminal state (an
    /// executed-terminal outcome the driver observed).
    ///
    /// This records the node as decided, decrements every dependent's countdown,
    /// and — for each dependent whose countdown thereby reaches zero — evaluates
    /// its trigger rule against the now-complete upstream picture (arch.md
    /// Vocabulary: the all-upstreams-terminal gate). A dependent whose rule
    /// **fires** is returned as [`Decision::Ready`]; a dependent whose rule **can
    /// never fire** is assigned its propagated terminal state
    /// ([`Decision::PropagatedTerminal`]) *without executing*, and that assignment
    /// is itself treated as a terminal notification that **cascades** to its own
    /// dependents in the same call. The returned decisions are the complete set
    /// this notification unlocked, in deterministic name order.
    ///
    /// Notifying an **already-decided** node is a **no-op** returning no decisions
    /// — a node's terminal state is decided exactly once (Vocabulary), so a
    /// propagated node the tracker already assigned must not be re-notified into a
    /// second state. An unknown node id is likewise a no-op.
    pub fn notify_terminal(&mut self, node: NodeId, state: TerminalState) -> Vec<Decision> {
        let mut decisions = Vec::new();
        // A work queue of (just-terminated node id, its terminal state). A
        // propagated terminal is pushed back onto this queue so its dependents
        // decrement in the same cascade — no intervening execution.
        let mut queue: Vec<(NodeId, TerminalState)> = vec![(node, state)];
        while let Some((terminated, term_state)) = queue.pop() {
            // Resolve the terminated node; an unknown id is a no-op.
            let Some(name) = self.name_of(terminated) else {
                continue;
            };
            // Mark it decided (exactly once — re-notifying or a second cascade
            // reaching it changes nothing) and read its dependents in one borrow.
            let dependents = {
                let Some(n) = self.nodes.get_mut(&name) else {
                    continue;
                };
                if n.decided.is_some() {
                    continue;
                }
                n.decided = Some(term_state);
                n.dependents.clone()
            };
            // Transitioning from pending → decided; `pending` is > 0 here, but
            // saturate defensively so the count can never wrap.
            self.pending = self.pending.saturating_sub(1);

            // Decrement each dependent, recording this node's terminal state in the
            // dependent's upstream picture; evaluate the rule when it reaches zero.
            for dep_name in dependents {
                let ready_to_eval = {
                    let Some(dep) = self.nodes.get_mut(&dep_name) else {
                        continue;
                    };
                    // A dependent already decided (e.g. reached in this cascade)
                    // takes no further upstream input.
                    if dep.decided.is_some() {
                        continue;
                    }
                    dep.upstream_states.push((terminated, term_state));
                    dep.remaining = dep.remaining.saturating_sub(1);
                    dep.remaining == 0
                };
                if !ready_to_eval {
                    continue;
                }
                // Every upstream of `dep` is now terminal — evaluate its rule.
                let Some((dep_id, rule, states)) = self.nodes.get(&dep_name).map(|dep| {
                    let states: Vec<TerminalState> =
                        dep.upstream_states.iter().map(|(_, s)| *s).collect();
                    (dep.id, dep.rule, states)
                }) else {
                    continue;
                };
                match evaluate_rule(rule, &states) {
                    RuleOutcome::Fires => decisions.push(Decision::Ready(dep_id)),
                    RuleOutcome::Propagate(propagated) => {
                        let origin = self.origin_for(&dep_name, propagated, dep_id);
                        decisions.push(Decision::PropagatedTerminal {
                            node: dep_id,
                            state: propagated,
                            origin,
                        });
                        // The propagated assignment is itself a terminal
                        // notification: cascade it to `dep`'s dependents.
                        queue.push((dep_id, propagated));
                    }
                }
            }
        }
        decisions
    }

    /// The originating-node identity a propagated `state` carries for `dep_name`:
    /// the immediate upstream whose class decided the propagation (Vocabulary;
    /// T0.4). For `upstream-failed`, the first failure-like upstream; for
    /// `upstream-skipped`, the first skip-like; for `cancelled`, the first
    /// stop-like; otherwise the deciding upstream best matching the propagated
    /// state's class. Deterministic: the earliest-recorded matching upstream.
    ///
    /// `fallback` is `dep`'s own id, returned only in the impossible case that a
    /// propagated node recorded no upstream states (a propagated node always had
    /// ≥1 terminal upstream) — so this never fabricates a foreign identity.
    fn origin_for(&self, dep_name: &str, state: TerminalState, fallback: NodeId) -> NodeId {
        let Some(dep) = self.nodes.get(dep_name) else {
            return fallback;
        };
        let wanted = class_of(state);
        dep.upstream_states
            .iter()
            .find(|(_, s)| class_of(*s) == wanted)
            // Fall back to the first non-success upstream (the deciding one for the
            // "otherwise" branch), then to the first upstream — always defined
            // because a propagated node had ≥1 upstream.
            .or_else(|| {
                dep.upstream_states
                    .iter()
                    .find(|(_, s)| class_of(*s) != StateClass::Success)
            })
            .or_else(|| dep.upstream_states.first())
            .map_or(fallback, |(origin, _)| *origin)
    }

    fn node_state(&self, node: NodeId) -> Option<&NodeState> {
        self.nodes.values().find(|s| s.id == node)
    }

    fn name_of(&self, node: NodeId) -> Option<String> {
        self.nodes
            .iter()
            .find(|(_, s)| s.id == node)
            .map(|(name, _)| name.clone())
    }
}

/// The number of distinct in-pipeline upstreams the `edges` reference — the
/// fallback countdown seed when the artifact lacks a precomputed count (it never
/// should, for a node that assembled). Mirrors T14's precomputation.
fn distinct_upstream_count(pipeline: &Pipeline, edges: &[DataEdge]) -> u32 {
    let mut seen: Vec<NodeId> = Vec::new();
    for id in edges.iter().map(DataEdge::upstream) {
        if pipeline.node(id).is_some() && !seen.contains(&id) {
            seen.push(id);
        }
    }
    u32::try_from(seen.len()).unwrap_or(u32::MAX)
}
