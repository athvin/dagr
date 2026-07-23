# 025 · T14 — C7: assembly validation and precomputation

> **Milestone:** M1 · **Size:** M · **Type:** feature · **Components:** C7
> **Branch:** `feat/t14-assembly-validation-and-precomputation` · **Depends on:** T11, T13, T0.5, T0.8 · **Blocks:** T15, T17, T18, T29, T41

## Why / context
The builder from T13 accumulates named registrations into an immutable pipeline, and T11 gives it typed data edges with an ownership mode. This ticket adds the checks the compiler cannot perform and the precomputation the runtime consumes, turning a raw registration set into a validated, runnable pipeline. It is governed by C7 (Flow assembly), and it enforces acceptance criteria that leak in from C10 (Output slot consumer counts), C27 (durable-output contract), C5 (invalid execution-class override), and C17 (nonzero teardown cost). The assembly/bootstrap seam and cost-vector types come from T0.5, and the durable-output contract predicate comes from T0.8; per T0.5 the capacity fit check does NOT live here — it moves to bootstrap. Assembly must stay pure: no network, no filesystem, no clock, no credentials, and no parameter values, so the graph is provably parameter-independent and emittable in an empty environment.

## Objective
Make assembly a total, pure validation-plus-precomputation pass that reports every problem it finds (never just the first) and produces an immutable, runtime-ready pipeline.

Concrete pieces of work:
- Assembly validation that collects and returns ALL problems together, each as a distinct, complete report:
  - Duplicate registration name — the report names both declarations.
  - Empty pipeline (no nodes registered).
  - Invalid execution-class override incompatible with the task's declared work shape (per C5).
  - Duplicate stable type/task name (per C20 stable-name identity).
  - Durable-marked node whose output type lacks the durable-output contract (per T0.8 / C27).
  - Ownership-mode conflicts: an owned (moved) demand on a value with more than one consumer; an owned edge into a retrying node that has no clone-on-read opt-in.
  - Nonzero declared cost on a teardown node (per C17).
- The zero-consumer non-unit-output condition emitted as a WARNING (not an error): a node whose non-`()` output has zero consumers and is neither retained nor durable.
- The environment-capture allowlist declaration API on the builder, empty by default — a pure declaration of which environment variable names bootstrap is permitted to capture later; assembly stores the allowlist and captures nothing itself.
- Precomputation the runtime needs, computed once at assembly and frozen into the immutable pipeline: per-node consumer count, per-node remaining-dependency count, a valid execution order (topological), and the fingerprint slot (structural fingerprint plus policy hash per T0.7).
- Purity enforcement: assembly reachable code touches no I/O, no clock, no credentials, and no parameter value; parameter values are unreachable during registration and assembly.

## Test plan (write these first — TDD)

**Duplicate node name names both declarations.** Setup: register two nodes under the identical name. Action: assemble. Expected: assembly fails; the returned error identifies a duplicate-name problem and names BOTH declaration sites, not merely one.

**Empty pipeline is rejected.** Setup: a builder with no nodes registered. Action: assemble. Expected: assembly fails with an empty-pipeline problem.

**Invalid execution-class override fails assembly.** Setup: register an await-bound task with an override to a synchronous class (the disallowed direction per C5). Action: assemble. Expected: assembly fails with an invalid-override problem naming the node; the same-shaped compatible override (synchronous moving between blocking and compute) assembles cleanly.

**Duplicate stable name fails assembly.** Setup: two distinct registrations whose stable type/task names collide (per C20/T0.7). Action: assemble. Expected: assembly fails with a duplicate-stable-name problem identifying the colliding names.

**Durable node without the contract fails assembly.** Setup: mark a node durable whose output type does NOT implement the durable-output reference contract (T0.8). Action: assemble. Expected: assembly fails with a durable-without-contract problem naming the node; the same node with a contract-satisfying output type assembles cleanly.

**Ownership: owned demand on a multi-consumer value fails.** Setup: one producer whose output is consumed by two consumers, one edge declared as an owned (moved) demand. Action: assemble. Expected: assembly fails with an ownership-mode-conflict problem identifying the producer, the offending edge, and the multiple consumers.

**Ownership: owned edge into a retrying node without clone-on-read fails.** Setup: a consumer with retries configured that takes an owned input edge and does not opt into clone-on-read. Action: assemble. Expected: assembly fails with an ownership-mode-conflict problem naming the node and the input edge; adding clone-on-read (or removing retries) assembles cleanly.

**Nonzero teardown cost fails.** Setup: a teardown node with a nonzero declared cost in any pool. Action: assemble. Expected: assembly fails with a nonzero-teardown-cost problem naming the teardown node; a teardown with zero cost assembles cleanly.

**Zero-consumer non-unit output is a warning, not an error.** Setup: a node whose output type is non-`()`, has zero consumers, and is neither retained nor durable. Action: assemble. Expected: assembly SUCCEEDS and returns a warning identifying the node; a genuinely effect-only node with a `()` output produces no such warning; a retained or durable zero-consumer node produces no such warning.

**All problems are reported, not just the first.** Setup: a pipeline containing several independent defects at once (for example a duplicate name AND a nonzero teardown cost AND a durable-without-contract node). Action: assemble. Expected: the failure carries a report for EVERY defect present, each distinct and complete; fixing one still surfaces the rest.

**Consumer counts are exact before execution.** Setup: a fan-out where one producer feeds three consumers and another feeds none. Action: assemble and inspect the precomputed counts. Expected: the first producer's consumer count is exactly 3, the second's is exactly 0, and every node's count is present before any execution.

**Remaining-dependency counts match the graph.** Setup: a diamond (one root, two middles depending on the root, one join depending on both middles). Action: assemble and inspect the precomputed remaining-dependency counts. Expected: the root's count is 0, each middle's is 1, the join's is 2.

**Execution order is a valid topological order.** Setup: any acyclic pipeline with a known dependency structure. Action: assemble and read the precomputed order. Expected: every node appears after all of its dependencies in the order.

**Environment-capture allowlist is empty by default and declarative.** Setup: a builder on which no environment names are declared. Action: assemble and inspect the stored allowlist. Expected: the allowlist is empty; declaring specific names records exactly those names and nothing else; assembly reads no actual environment values.

**Assembly is pure — runs in an empty environment.** Setup: a test process with resources, credentials, and configuration absent (no network reachable, no relevant files, no parameter values supplied). Action: assemble a valid pipeline. Expected: assembly succeeds without touching any of them, proving no external dependency; this is the empty-environment proof required by C7.

**No parameter value is reachable during registration or assembly.** Setup: attempt (in a test) to read a parameter value from within registration/assembly-reachable code. Expected: no assembly-time API exposes parameter values — demonstrated by there being no such path — and assembly succeeds with no parameters present.

**Assembling twice yields byte-identical graph artifacts.** Setup: the same pipeline definition. Action: assemble it twice in one process and emit the graph artifact from each (per C20). Expected: the two artifacts are byte-identical apart from the generation-time header field, confirming the fingerprint slot and precomputation are deterministic.

## Definition of done
- [ ] Assembly turns accumulated registrations into an immutable pipeline value.
- [ ] A duplicate node name fails assembly and the error names BOTH declarations.
- [ ] An empty pipeline fails assembly.
- [ ] An execution-class override incompatible with the task's declared work shape fails assembly (C5); a compatible override assembles.
- [ ] A duplicate stable type/task name fails assembly (C20).
- [ ] A durable-marked node whose output type lacks the durable-output contract fails assembly (C27 / T0.8); a contract-satisfying output assembles.
- [ ] An owned demand on a value with more than one consumer fails assembly, identifying producer, edge, and consumers.
- [ ] An owned edge into a retrying node without clone-on-read fails assembly, identifying the node and edge.
- [ ] A teardown node with nonzero declared cost fails assembly (C17); a zero-cost teardown assembles.
- [ ] A node whose non-`()` output has zero consumers and is neither retained nor durable is reported as a WARNING, and assembly still succeeds; `()`-output, retained, and durable zero-consumer nodes produce no warning.
- [ ] Assembly reports ALL problems it finds in one pass, not only the first; each report is distinct and complete.
- [ ] Consumer counts are exact for every node before any execution begins (C10).
- [ ] Remaining-dependency counts are computed for every node and match the graph.
- [ ] A valid execution order (topological) is precomputed and frozen into the pipeline.
- [ ] The fingerprint slot (structural fingerprint + policy hash per T0.7) is computed at assembly.
- [ ] The environment-capture allowlist declaration API exists on the builder, is empty by default, records exactly the declared names, and captures no values during assembly.
- [ ] Assembly performs NO capacity/cost-fit check — that check is deferred to bootstrap per T0.5.
- [ ] Assembly is pure: no network, filesystem, clock, credentials, or parameter values are reachable from registration/assembly code; proven by a test running assembly in an empty environment.
- [ ] No parameter value is reachable during registration or assembly.
- [ ] Assembling the same pipeline twice in one process produces byte-identical graph artifacts apart from the generation-time field (C20).
- [ ] Public assembly APIs carry rustdoc, including the documented meaning of each problem variant and the warning.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
None. (The ticket section and the `docs/tasks.md` T14 entry carry no `Q:` items.)

### Implementation-seam resolutions (recorded during T14)
Three non-obvious seam choices the ADRs sanction but leave to T14; recorded here
so T29/T41/T57 inherit them:

- **The `NodePolicy` seam vs the full C5 struct (T29).** T14 depends on T13/T0.5,
  and T29 (the full C5 node-policy struct) depends on *T14* — so the policy
  fields assembly must read do not exist yet. Resolution: `dagr_core::assembly`
  defines the **minimal policy seam** carrying exactly the fields assembly reads
  (durability, retention, retries, teardown, the per-pool `CostVector`, class
  override) with the conservative C5 defaults. T29 expands this seam into the
  full policy struct (backoff, timeout, trigger rule, group, emission, policy-hash
  participation). This honors the ticket's *"reads policy fields … but does not
  define them"* against the actual T29-after-T14 dependency order.
- **The durable-contract witness mechanism (T0.8 §5 left it to T14/T57).** Stable
  Rust has no specialization, so a generic registrar cannot ask "does `T::Output`
  implement `DurableOutput`?". Resolution: the witness is captured at the **typed
  registration site** — `Flow::register_source_durable` / `Flow::register_durable`
  are bounded on `T::Output: DurableOutput` and record a `DurableWitness::Present`;
  every other path records `Absent`. Marking a node durable via the ordinary
  policy path (witness `Absent`) is exactly the durable-without-contract case
  assembly rejects — an **assembly** failure, not a compile error, as T0.8 §5
  requires. T57 supersedes the `DurableOutput` marker with the full trait pair.
- **The fingerprint digest (T41 owns the algorithm).** T14 populates the
  fingerprint slot using the T0.7 field composition and a deterministic,
  registration-order-independent, unambiguously-framed canonical byte encoding —
  enough for "assemble twice → byte-identical". The concrete **BLAKE3-v1**
  algorithm and its versioning (C21 / T41) and the artifact wire schema (C20 /
  T40) are downstream; T14's digest is a dependency-free FNV-1a stand-in (the same
  family `NodeId::from_name` already uses), adding **no** core-crate dependency.

## Out of scope
- The bootstrap phase itself: capacity/cost-fit rejection, missing-declared-resource rejection, parameter parsing and validation, and the ACTUAL capture of allowlisted environment values all belong to bootstrap (T24/T29 and the bootstrap consumer of the T0.5 seam), not here — this ticket only declares the allowlist and defers the cost-fit check.
- Emitting or serializing the graph artifact and computing the fingerprint algorithm internals (C20/C21, T40/T41); this ticket populates the fingerprint slot using the T0.7 composition but does not own the artifact schema or renderers.
- Runtime consumption of the precomputed counts and order — output slots (T17/C10), the readiness tracker (T18/C11), and node policy defaults (T29/C5) are downstream tickets.
- The node-policy struct and its defaults (T29); this ticket reads policy fields (durability, retention, retries/clone-on-read, teardown cost, class override) to validate them but does not define them.
- Determinism and purity property tests beyond the smoke coverage here are T15's remit.
- Anything crossing the permanent scope boundary: no scheduling or admission decisions, no runtime graph mutation, no distributed execution, no metadata store, no DSL — the graph shape is fixed at assembly and never changes at runtime, which is exactly why this validation is total and one-shot.
