# 049 · T38 — M2 demo: overcommit and clean stop

> **Milestone:** M2 · **Size:** M · **Type:** feature (demo) · **Components:** M2 gate
> **Branch:** `feat/t38-m2-demo-overcommit-and-clean-stop` · **Depends on:** T30, T32, T33, T34, T36, T37, T67 · **Blocks:** T65

## Why / context
This ticket is the M2 milestone gate: the Build order names M2 ("It survives") done when a pipeline whose combined *declared* demand exceeds the configured memory capacity completes without exceeding it, and an induced mid-run failure stops the run cleanly with nothing orphaned. It exists to prove those two guarantees end-to-end as executable CI tests rather than as unit-level assertions on the individual components (C9/T30, C12/T31–T32, C13/T33, C15/T34, C16/T35–T36). It builds on the admission controller (C12), execution class dispatch (C13), failure policy and propagation (C15), and cancellation and shutdown (C16), and it uses the T32 capacity-pinning flag to make the memory ceiling deterministic across CI machines. It also depends on T37's permit-release outcome matrix (the ledger-never-exceeds-capacity proof) and T67 (concurrent runs), so the demo can lean on those invariants instead of re-deriving them. It blocks T65, the system acceptance gate.

## Objective
Assemble two reference pipelines and drive each through the real execution core as CI tests that demonstrate the two M2 done-when behaviours as directly observable outcomes.

- Build an **overcommit** demo pipeline: several nodes with honest declared memory costs whose *combined* declared demand strictly exceeds a pinned memory-pool capacity, yet where every individual node's declared cost fits under that capacity.
- Run the overcommit pipeline with the memory pool pinned via the T32 flag, and assert the run completes with every node `succeeded` while the concurrently-charged declared cost never exceeds the pinned capacity at any instant — proving admission turned the ceiling into a throughput limit, not a crash.
- Build a **clean-stop** demo pipeline containing an induced mid-run failure under stop-on-first-failure mode, with in-flight sibling work, a downstream default-rule node, and a consume-nothing non-default-rule contingency node.
- Run the clean-stop pipeline and assert the run stops cleanly: the failure propagates per trigger rules, the contingency fires, no default-rule work is admitted after the failure, every permit is released back to empty pools, and no thread, temp file, or in-flight closure is left orphaned.
- Wire both as first-class CI tests with the capacity pinned to fixed values so results are portable and deterministic on any runner.

## Test plan (write these first — TDD)

**Overcommit completes without exceeding capacity (the throughput-not-crash proof).**
Setup: assemble a pipeline of parallel-ready nodes, each declaring an honest memory cost, such that the sum of all declared costs is strictly greater than a pinned memory-pool capacity `M`, while each single node's declared cost is `≤ M`; pin the memory pool to `M` via the T32 flag and instrument the admission ledger so the sum of declared cost across currently-admitted attempts is sampled. Action: run the pipeline to completion. Expected outcome: every node ends `succeeded`; the run's overall outcome is success; and at no sampled instant does the sum of admitted declared cost exceed `M` — the ceiling became a serialization point, and the run neither crashed nor OOM'd.

**Capacity is genuinely binding, not incidentally sufficient.**
Setup: the same overcommit pipeline and pinned capacity. Action: run it and record admission ordering / peak concurrency. Expected outcome: at least one node observably waited for a permit (its recorded permit-wait time is nonzero) because at least two nodes could not be co-admitted — proving the overcommit was real and admission actually gated it, not that everything happened to fit.

**A single oversized node fails fast at bootstrap, not at admission.**
Setup: a pipeline containing one node whose declared cost exceeds the pinned pool's total capacity `M`. Action: bootstrap the run. Expected outcome: bootstrap fails before any node is admitted, with a message naming the offending node and pool, and the bootstrap-failure artifact is produced — the demo confirms the fail-fast path rather than a wedged admission queue.

**Clean stop under stop-on-first-failure: no further default-rule work is admitted.**
Setup: assemble a pipeline in stop-on-first-failure mode with a node induced to fail permanently mid-run, at least one unrelated in-flight sibling node still executing when the failure is observed, and a downstream default-rule (`all-succeeded`) node that has not yet been admitted. Action: run the pipeline. Expected outcome: after the first terminal failure is observed, the pending downstream default-rule node is never admitted and ends `cancelled` (or `upstream-failed` if it is a direct data dependent — see propagation test); the failing node ends `failed`; the run's overall outcome is failure.

**Clean stop still fires the contingency.**
Setup: the clean-stop pipeline additionally contains a consume-nothing node with a non-default trigger rule (`any-failed`) attached to the failing node. Action: run the pipeline. Expected outcome: the contingency node executes and ends `succeeded` even though the run is stopping — a failure-triggered notify/cleanup contingency is exactly the work a stop is supposed to run, and stop mode does not cancel it.

**Propagation is by rule, not by blast radius.**
Setup: the clean-stop pipeline includes (a) a direct data dependent of the failing node and (b) an `all-terminal` cleanup-style consume-nothing node downstream of it. Action: run the pipeline. Expected outcome: the direct data dependent ends `upstream-failed` without executing (its `all-succeeded` rule can no longer be satisfied); the `all-terminal` node still executes because its rule can still fire; every node in the run has exactly one terminal state.

**All permits released — nothing left charged.**
Setup: the clean-stop pipeline with the admission ledger instrumented. Action: run to its stopped conclusion and, after the run object reports terminal, read every pool's remaining capacity. Expected outcome: every pool is back to full capacity — declared cost charged against every pool is zero — proving no permit leaked on the success, permanent-failure, cancellation, or drained-sibling paths.

**Nothing orphaned: no live threads, no residual temp, complete stream.**
Setup: the clean-stop pipeline run to conclusion, with the per-run temp directory and worker pools observable. Action: after the run reports terminal, inspect the process for live task threads, inspect the run's temp directory, and read the event stream. Expected outcome: no task closure is still running; the per-run temp directory created by cooperative tasks is cleaned up (and would be removed by a subsequent invocation regardless); and the event stream is complete and gapless with exactly one terminal state per node and exactly one attempt-outcome record per attempt — no dangling in-flight events.

**Deterministic under a pinned ceiling on any runner.**
Setup: run both demos in CI with capacity pinned to the fixed demo values via the T32 flag. Action: execute the CI job. Expected outcome: both demos produce the same terminal-state picture and the same pass/fail verdict regardless of the runner's real cgroup/host memory, because the pinning flag overrides detection — the CI-portability concern is resolved by pinning.

## Definition of done
- [ ] The overcommit demo pipeline has combined declared cost strictly greater than the pinned memory capacity while each single node's declared cost fits, and it completes with all nodes `succeeded`.
- [ ] During the overcommit run, the combined declared cost of executing nodes never exceeds pool capacity at any sampled instant (C12 capacity invariant, including any abandoned-but-running cost).
- [ ] The overcommit run demonstrably serializes at least one admission (nonzero recorded permit-wait on at least one node), proving the ceiling is binding rather than incidentally sufficient.
- [ ] A node whose declared cost exceeds the pinned pool's total capacity fails at bootstrap (not at admission), with a message naming node and pool, and the bootstrap-failure artifact is produced.
- [ ] Memory capacity is pinned via the T32 flag for both demos, overriding cgroup/host detection so results are deterministic and portable across CI runners.
- [ ] Under stop-on-first-failure, no default-rule non-teardown node is admitted after the first terminal failure is observed; pending unrelated default-rule nodes end `cancelled` (C15).
- [ ] A consume-nothing contingency node whose non-default rule (`any-failed`) fires on the final picture still executes to `succeeded` under stop mode (C15).
- [ ] A node whose trigger rule can still be satisfied (`all-terminal` cleanup) executes; a direct data dependent whose `all-succeeded` rule cannot be satisfied is marked `upstream-failed` without executing (C15).
- [ ] No node executes if any of its data dependencies did not succeed (C15).
- [ ] Every node in each demo has exactly one terminal state in the artifact/stream, including nodes that never ran (C15).
- [ ] After each run reports terminal, every admission pool is back to full capacity — all permits released on the success, permanent-failure, retryable-failure, and cooperative-cancellation paths (C12, cross-checked against T37's matrix).
- [ ] After the clean-stop run, no task closure is still running, and the process holds no orphaned live task threads.
- [ ] The per-run temp directory created by cooperative tasks is cleaned up on the clean stop, and the per-run temp-dir convention removes it by the next invocation regardless (C16).
- [ ] The clean-stop run writes a complete, gapless event stream with exactly one attempt-outcome record per attempt and exactly one terminal state per node — no dangling in-flight events (C16, C14).
- [ ] The clean-stop run's overall outcome is failure; the overcommit run's overall outcome is success; both verdicts are asserted, not merely observed.
- [ ] Both demos are wired as CI tests that run within the M2 test budget and pin capacity to fixed values so they pass on any runner.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
None.

## Out of scope
- Any artifact-rendering or fingerprinting behaviour — graph/run artifacts, diagrams, and the "which node was slowest, waiting or working?" question belong to M3 (C20–C25) and later tickets; this demo asserts terminal states, ledger balance, and stream completeness, not human-reviewable renders.
- Resume, teardown attachment, setup/teardown lifecycle beyond what C15/C16 already guarantee, the CLI contract, and the durable scratch store — all M4 (C17, C18, C26, C27, C28); the demo drives pipelines at the builder/assembly level with policy set programmatically, not via a command-line surface.
- Re-proving the per-outcome permit-release matrix or the two-concurrent-runs guarantee — those are owned by T37 and T67 respectively; this ticket consumes their invariants and must not duplicate their unit-level coverage.
- Container-limit detection logic itself (T32) beyond exercising its pinning flag; the demo does not test cgroup v2/v1/host probing.
- The system acceptance gate consolidation — that is T65, which this ticket blocks; do not pull cross-milestone acceptance-matrix wiring into here.
- Scope-boundary temptations that must not creep in: adding a scheduler, distributed or multi-machine execution, a metadata store, a web/UI surface, a DSL, or any runtime graph-shape mutation to make the overcommit or stop scenarios more dramatic — the graph is fixed at assembly and the demo runs a single machine only.
