# 035 · T25 — C11: termination property test

> **Milestone:** M1 · **Size:** M · **Type:** feature (tests) · **Components:** C11
> **Branch:** `feat/t25-termination-property-test` · **Depends on:** T24 · **Blocks:** T28

## Why / context
The readiness tracker (C11) decides what is eligible to run and when, and its most load-bearing guarantee is negative: it cannot deadlock. C11's acceptance criteria demand that *every node ends in exactly one terminal state from the normative taxonomy* and that *the run ends precisely when nothing is pending or in flight*, and they call out explicitly that "a test over randomly generated graphs with randomized outcomes confirms every run terminates." This ticket builds that property test on top of the M1 run-loop driver delivered in T24, exercising the real tracker-plus-driver against random DAG shapes and random per-node outcomes. It is the safety net that lets the M1 demo (T28) trust the scheduler under shapes no hand-written test would think to try.

## Objective
Build a property-based test suite that generates random DAGs with randomized node outcomes, runs each through the real M1 driver + readiness tracker (C11, via T24), and asserts the two termination invariants hold for every generated case, with failing cases shrunk to a minimal reproducer and reproducible from a recorded seed.

Concrete pieces of work:
- A **random DAG generator** producing arbitrary valid acyclic graphs: random node count within a bounded range, random edges that preserve acyclicity (e.g. edges only from lower to higher topological index), a mix of data edges and ordering-only edges, and — for nodes that consume nothing — a random choice among the three trigger rules `all-succeeded`, `all-terminal`, `any-failed`; data-consuming nodes are always `all-succeeded` (the compile-time restriction from C3, so the generator simply never assigns another rule to them).
- A **random outcome assignment** per node drawn from the outcomes a task can actually produce at M1: success, permanent failure, deliberate skip, timeout, and (where reachable) retry-then-succeed. Nodes that never run take their propagated state from the tracker, not the generator.
- A **harness** that executes each generated graph through the real C11 tracker driven by the T24 run loop against fakes (the C28 direction), scripting each executed node to its assigned outcome, with capacity pinned deterministically so admission never depends on host resources.
- **Property assertions** encoding: the run terminates within a bounded wall-clock/step budget; every node has exactly one recorded terminal state drawn only from the normative taxonomy; and the run-finished signal appears exactly when no node is pending or in flight.
- **Seed capture and shrinking**: every case runs from a recorded seed printed on failure, and the property framework shrinks a counterexample to a minimal DAG + outcome assignment.
- **Determinism**: the suite is seeded so a green run and a red run are both reproducible in CI from the printed seed.

## Test plan (write these first — TDD)
These are the property scenarios themselves — this ticket *is* a test deliverable, so the "tests" are the property definitions plus targeted regression cases that pin known-hard shapes.

**Property 1 — every run terminates.**
- Setup: the generator emits a random valid DAG (bounded node count, acyclic edges, mixed edge kinds, per-node random trigger rule among the closed set for consume-nothing nodes) with a random outcome assigned to each node, seeded from the property framework.
- Action: run the graph through the real tracker + T24 driver against fakes under a bounded step/time budget with capacity pinned.
- Expected outcome: the driver reaches run-finished within the budget for every generated case; no case exhausts the budget (a budget exhaustion is a deadlock and fails the property). The number of cases is large enough to be meaningful (a configurable case count, higher in CI than in a local quick run).

**Property 2 — exactly one terminal state per node, from the taxonomy.**
- Setup: same generated cases as Property 1.
- Action: after each run finishes, collect the terminal state recorded for every node in the graph.
- Expected outcome: every node in the generated graph has a recorded terminal state; no node has zero and none has two; and every recorded value is one of the nine normative terminal states (`succeeded`, `failed`, `timed-out`, `skipped`, `upstream-skipped`, `upstream-failed`, `cancelled`, `abandoned`, `satisfied-from-prior`) — never an off-taxonomy value and never the artifact-only `not-requested`.

**Property 3 — run ends exactly when nothing is pending or in flight.**
- Setup: same generated cases; the harness observes tracker state (pending/ready/in-flight counts) at the moment the driver declares the run finished.
- Action: run each case and capture the pending-and-in-flight tally at run-finished.
- Expected outcome: at run-finished the pending count and the in-flight count are both zero; and no node reaches a terminal state after run-finished is declared (the run does not close early over live work, nor linger after all work is decided).

**Property 4 — propagation is consistent with assigned outcomes (no node executes without a fired rule).**
- Setup: same generated cases, tracking for each node whether the harness was ever asked to execute it.
- Action: for every node, compare "was executed" against its recorded terminal state.
- Expected outcome: a node whose terminal state is a propagated/never-ran class (`upstream-skipped`, `upstream-failed`, or `skipped` arising from an `any-failed` contingency that never arose) was never handed to the executor; a node with an executed-outcome state (`succeeded`, `failed`, `timed-out`, or an originated `skipped`) was executed exactly once; and a node marked `upstream-failed` has at least one upstream in a failure-like class under a rule that could no longer fire, while an `all-terminal` node downstream of a failure still ran. This encodes C11's "rule fires vs. can-never-fire" behaviour across random shapes rather than one hand-built diamond.

**Regression case A — fan-out / fan-in diamond with mixed rules.**
- Setup: a pinned diamond where one branch fails, one succeeds, and the join carries `all-terminal`.
- Action: run it.
- Expected outcome: the join executes (its rule can still fire), the run terminates, and every node has exactly one terminal state — a fixed reproduction of the general properties for a shape reviewers can read.

**Regression case B — all-skips graph.**
- Setup: a pinned graph in which every executed node returns a deliberate skip and skips propagate downstream.
- Action: run it.
- Expected outcome: the run terminates, downstream nodes are `upstream-skipped` carrying an originating identity, originated skips are `skipped`, and the run as a whole is a success (a run of only skips succeeds).

**Regression case C — recorded-seed replay.**
- Setup: take a seed printed by any property run.
- Action: re-run the suite pinned to that seed.
- Expected outcome: byte-for-byte the same generated cases and the same pass/fail result — proving the suite is reproducible and any future counterexample can be re-driven in CI.

**Regression case D — shrink produces a minimal counterexample (meta-test / documented check).**
- Setup: temporarily inject a deliberately broken tracker (or a fault toggle) that leaves one node non-terminal.
- Action: run the property suite and observe the reported counterexample.
- Expected outcome: the framework reports a small DAG + outcome assignment rather than a hundred-node one, and prints its seed. This validates that the generator shrinks; it is documented (and, where cheap, kept behind an ignored/opt-in test) rather than left to chance.

## Definition of done
Derived from C11's acceptance criteria (arch.md §C11 · Readiness tracker) plus this ticket's deliverables. Only the two invariants named in T25's scope are load-bearing here; the per-rule fires/can-never-fire *unit* coverage belongs to T18 and the failure-policy runtime to T34 — this ticket asserts those behaviours hold as *emergent properties* over random shapes, not as bespoke unit cases.

- [ ] A random DAG generator produces valid acyclic graphs with a bounded random node count, acyclic edges, a mix of data and ordering-only edges, and per-node random trigger rules drawn only from the closed set (`all-succeeded`, `all-terminal`, `any-failed`), never assigning a non-default rule to a data-consuming node.
- [ ] A random per-node outcome assignment covers the M1 outcome space (success, permanent failure, deliberate skip, timeout, and retry-then-succeed where reachable); never-run nodes take their state from the tracker, not the generator.
- [ ] The suite drives each case through the real C11 tracker and the T24 run loop against fakes, with admission capacity pinned so results do not depend on host resources.
- [ ] **Termination property:** every generated run reaches run-finished within a bounded step/time budget; budget exhaustion is treated as a deadlock and fails the property — this is the anti-deadlock guarantee (C11).
- [ ] **Single-terminal-state property:** every node in every generated graph ends with exactly one recorded terminal state, and every recorded state is one of the nine normative taxonomy states — never off-taxonomy, never `not-requested`.
- [ ] **Run-boundary property:** at the moment run-finished is declared, pending and in-flight counts are both zero, and no node transitions to terminal after run-finished.
- [ ] **Propagation-consistency property:** never-ran nodes (`upstream-skipped`, `upstream-failed`, or an `any-failed` contingency that never arose → `skipped`) are never executed; executed nodes run exactly once; an `all-terminal` node downstream of a failure still runs, and an `all-succeeded` node whose rule can no longer fire is marked with the correct propagated class.
- [ ] Pinned regression cases exist for the mixed-rule diamond, the all-skips-succeeds graph, and recorded-seed replay, each independently checkable.
- [ ] Every case runs from a recorded seed that is printed on failure; counterexamples shrink to a minimal DAG + outcome assignment; the shrinking behaviour is demonstrated by a documented (opt-in) meta-check.
- [ ] The suite is deterministic and reproducible in CI from a printed seed, with a case count that is meaningfully large in CI and quick locally.
- [ ] The property-test dependency (proptest or equivalent) is added to the workspace as a dev-dependency only and recorded per the project's dependency-review convention.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
The ticket recorded "None", and `docs/tasks.md`'s T25 entry carries no `Q:`
items. The following implementation decisions were genuinely open at the seam
between what the ticket asked for and what the merged M1 surface can express;
each is resolved and recorded here per the open-questions duty.

- **Property framework — proptest vs. a hand-rolled seeded generator.** The DoD
  names "proptest or equivalent". Resolved: a **hand-rolled, dependency-free,
  deterministic seeded generator** (a `SplitMix64` PRNG + a bounded random-DAG
  shape + an explicit shrinker), the ticket's own permitted "equivalent". This
  keeps `dagr-core`'s and `dagr-cli`'s review-gated dependency trees **unchanged**
  (arch.md "Stability": additions to the core dependency set are a reviewed policy
  decision) — so `cargo audit`/`cargo deny` are untouched and stay green with no
  new dev-dependency and no deny.toml license addition. It is trivially
  deterministic in CI (no wall-clock, no network), captures every case's seed,
  prints it on failure, replays byte-for-byte from a recorded seed, and shrinks a
  failing case to a minimal reproducer — i.e. it delivers every capability the DoD
  requires of "proptest or equivalent" without the transitive tree.

- **System under test — the T24 driver vs. the C11 tracker directly.** The DoD
  says "drives each case through the real C11 tracker and the T24 run loop against
  fakes." Resolved: **both**, split by cost. The deep, high-case-count property
  (thousands of cases) drives the *real* `ReadinessTracker` directly
  (`crates/core/tests/termination_property.rs`) — it is the pure state machine
  whose termination is the crux, and the driver's `run_loop` is a thin
  admit→feed-back→admit shell around exactly its `initial_ready()` /
  `notify_terminal` API, so stepping it directly is faithful, deterministic, and
  fast (no per-case tokio runtimes). A companion suite
  (`crates/cli/tests/termination_property_driver.rs`) drives a small fixed set of
  the *same* generated DAGs through the *real* `dagr_cli::driver::drive` against
  fake scripted runners, proving the full two-runtime T24 loop terminates too. The
  driver never changes behaviour (T24/T18 are untouched).

- **Non-default trigger rules on generated runtime nodes.** The DoD asks the
  generator to assign per-node random rules from the closed set on consume-nothing
  nodes. Resolved: the merged M1 surface makes a non-default rule **inexpressible**
  on any runtime node reachable by the generator — a non-default rule is settable
  only on a consume-nothing node (C4), such a node's only upstreams are *ordering
  edges*, and ordering-edge registration is **T50 (not yet landed)**; the typed
  `Flow` builder pins `all-succeeded` on every data node (C3, compile-time). This
  is exactly why M1 "ships `all-succeeded` execution against the final rule
  interface" (this ticket's Out of scope) and why the `all-terminal`/`any-failed`
  *runtime firing* is T34. The generated runtime nodes therefore carry
  `all-succeeded` (honouring "never assigning a non-default rule to a
  data-consuming node" vacuously), and Property 4's `all-terminal`-fires-downstream
  -of-a-failure assertion is exercised at the pure `evaluate_rule` seam in
  regression case A, where it *is* reachable in M1 — the same seam T18 unit-tests
  and T34 will wire onto runtime nodes. No tracker/driver behaviour is changed to
  accommodate this.

- **Capacity pinning.** The M1 driver admits every ready node with no admission
  pool (C12 is T31), so admission is capacity-independent by construction; the
  property therefore never depends on host resources without any explicit pin.

- **Retry-then-succeed outcome.** Retry orchestration (C14/T22) is upstream of the
  tracker, which only ever sees a node's *decided* terminal. A retry-then-succeed
  case is modelled as the terminal `succeeded` it resolves to — the tracker sees no
  intermediate retry, so the outcome space the generator feeds the tracker is the
  set of decided terminals a node originates (success, failed, skipped, timed-out).

## Out of scope
- Per-rule unit coverage of the fires/can-never-fire table and the resulting propagated states (that is T18's readiness-tracker criterion and the runtime evaluation in T34) — this ticket only asserts they hold as emergent properties over random graphs.
- Failure-policy modes (stop-on-first-failure vs. continue-independent), cancellation, timeout-class semantics, admission-pool behaviour, teardown, and resume — all belong to M2+ tickets (T31, T34, T35, later milestones) and must not be pulled in; M1 ships `all-succeeded` execution against the final rule interface, and the property suite only relies on M1 behaviour.
- Crash-safety / I/O fault injection and event-stream prefix validity (T27) — a separate ticket; this suite asserts termination and terminal-state invariants, not stream durability.
- Performance or scale benchmarking of the tracker (T69) — the budget here is a deadlock detector, not a latency target.
- Anything that would change graph shape at runtime, introduce scheduling across machines, or add a metadata store — these are permanent non-goals; the generator produces a fixed graph per case and executes it on a single machine.
