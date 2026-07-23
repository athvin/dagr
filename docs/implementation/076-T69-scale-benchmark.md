# 076 · T69 — Scale benchmark

> **Milestone:** M4 · **Size:** S · **Type:** feature (bench) · **Components:** Performance envelope
> **Branch:** `feat/t69-scale-benchmark` · **Depends on:** T24, T48 · **Blocks:** T65

## Why / context
The Performance envelope section of `arch.md` states a hard budget: framework overhead per node — scheduling, admission, event writing, everything but the task's own work — stays **under one millisecond**, and the spec commits to holding that budget with a CI benchmark that runs a thousand-node no-op graph and fails on regression. This ticket builds that benchmark. It rides on the M1 run-loop driver (T24), which admits ready nodes, spawns attempts, feeds outcomes back, and terminates exactly when nothing is pending or in flight, and it reuses the ten-thousand-attempt scale-artifact concern that T48 froze to keep tooling honest at scale. The benchmark is a required input to the system acceptance gate (T65), where the Performance-envelope criterion must map to a passing test in the criteria matrix.

## Objective
Build a CI-runnable benchmark that measures pure dagr framework overhead across a thousand-node no-op graph and fails the build when per-node overhead regresses past the budget.

Concrete pieces of work:
- A benchmark harness that constructs a graph of exactly one thousand nodes whose tasks do no real work (no-op bodies, no declared cost beyond what forces admission through its normal path), so that measured time is framework overhead and not task work.
- Measurement of total wall-clock overhead for one full run of that graph, divided by node count to yield per-node overhead, with the task's own (near-zero) work excluded from what the budget covers.
- A budget assertion that the CI benchmark fails on regression: a checked-in threshold expressing the under-1-ms-per-node budget with an explicit, documented headroom margin so normal CI-runner variance does not flap the build, and a hard ceiling at the 1 ms/node spec limit.
- A shape that exercises the real driver end to end — readiness tracking (C11), admission (C12), attempt running (C14), and event-stream writing (C19) — not a stubbed scheduler, so the number reflects the overhead the spec budgets.
- Wiring the benchmark into CI as a gating job on the ticket branch, with a documented way to pin/normalize the runner's capacity (via the C12 pinning flag) so the measurement is deterministic rather than dependent on the CI host's discovered limits.
- A short operator note (in the benchmark's own docs/comments-as-rustdoc where a public item exists) recording what the budget covers, what the threshold and margin are, and how to re-baseline when a legitimate change moves the number.

## Test plan (write these first — TDD)
- **Graph size is exactly a thousand nodes.** Setup: invoke the benchmark's graph builder. Action: count the nodes in the assembled graph. Expected: the count is exactly 1000, and every node is a no-op task, so the benchmark measures the shape the spec names.
- **No-op run completes with all nodes in a success terminal state.** Setup: build the thousand-node no-op graph and run it through the real driver once. Action: fold the resulting event stream / inspect the run artifact. Expected: every one of the 1000 nodes reaches exactly one terminal state and that state is success-like; the run ends precisely when nothing is pending or in flight. A benchmark over a graph that silently skipped or failed nodes would measure the wrong thing, so this guards the measurement's validity.
- **Overhead is attributed to the framework, not the task.** Setup: run the thousand-node no-op graph. Action: read the per-attempt phase breakdown from the run artifact and confirm the task-body phase is negligible relative to the scheduling/admission/event-writing phases. Expected: the phases sum exactly to each attempt's total (per C22), and the executing-work phase is near zero, confirming the measured budget number is framework overhead rather than task work.
- **Per-node overhead is computed and reported.** Setup: run the benchmark once. Action: divide total measured framework overhead by the node count. Expected: the harness emits a single per-node-overhead number in a stable, machine-readable form the CI job can threshold against.
- **The budget assertion passes under budget.** Setup: run the benchmark on a normally-provisioned CI runner with capacity pinned to the benchmark's fixed configuration. Action: compare the computed per-node overhead against the checked-in threshold. Expected: the value is under the 1 ms/node ceiling (with the documented margin) and the benchmark exits success.
- **The budget assertion fails on regression.** Setup: with the threshold logic under test, feed it a per-node-overhead value deliberately above the ceiling (a simulated or injected slow number), or temporarily lower the threshold. Action: run the budget check. Expected: the check reports failure with a message naming the measured value, the threshold, and the node count — so a CI failure is diagnosable without re-running locally. This proves the "fails on regression" clause is real and not vacuous.
- **Capacity is deterministic, not host-discovered.** Setup: run the benchmark twice on the same runner. Action: confirm the admission pool sizes used were the pinned benchmark values, not values discovered from the CI host's cgroup/host limits. Expected: both runs use identical pinned capacity, so the number is a property of dagr's overhead and not of the runner's core count.
- **Runs as a CI job on the ticket branch.** Setup: the CI configuration on `feat/t69-scale-benchmark`. Action: trigger CI. Expected: a dedicated benchmark job runs the thousand-node graph and gates the build on the budget assertion, distinct from (but alongside) the ordinary test job.

## Definition of done
- [ ] A benchmark constructs a graph of exactly 1000 no-op nodes and runs it through the real run-loop driver (C11 readiness, C12 admission, C14 attempt running, C19 event stream — no stubbed scheduler).
- [ ] The benchmark measures framework overhead per node — scheduling, admission, event writing; everything but the task's own work — consistent with the Performance-envelope definition, and excludes task-body time from what the budget covers.
- [ ] Per-node overhead is computed as total framework overhead over node count and emitted in a stable, machine-readable form.
- [ ] A checked-in threshold enforces the under-1-ms-per-node budget with a documented headroom margin and a hard ceiling at 1 ms/node; the benchmark fails the build when the measured value exceeds it.
- [ ] The failure path is proven: a per-node-overhead value above the ceiling makes the budget check fail, with a message naming the measured value, the threshold, and the node count.
- [ ] Admission capacity is pinned via the C12 pinning flag to a fixed benchmark configuration so the measurement is deterministic and independent of the CI host's discovered limits.
- [ ] The measured no-op run terminates cleanly with every node in exactly one success-like terminal state, so the benchmark measures a valid, fully-executed graph.
- [ ] The benchmark is wired into CI as a gating job on the ticket branch, with the runner-normalization approach documented.
- [ ] The Performance-envelope budget is expressed as an automated test/benchmark that the T65 criteria matrix can map to (this ticket's output is the passing test that criterion cites).
- [ ] Rustdoc on any public item introduced records what the budget covers, the threshold and margin values, and how to re-baseline when a legitimate change moves the number.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
None.

## Out of scope
- Optimizing the driver to *hit* the budget — this ticket measures and gates; any actual overhead reduction is separate work under the driver's own components (C11/C12/C14/C19).
- The ten-thousand-attempt artifact fixture and artifact-schema validation — that is T48's deliverable; this ticket only reuses the run through the driver, not the artifact-corpus machinery.
- Benchmarking anything above a thousand nodes or below ten — the spec fixes the envelope at ten-to-a-thousand and this benchmark targets the thousand-node ceiling only.
- Cross-platform performance parity or a platform matrix for the benchmark — platform coverage is T70; this benchmark runs on the tier-1 CI runner.
- Measuring task work, throughput under a memory ceiling as a product feature, or any distributed/multi-machine scaling — dagr is single-machine by permanent scope boundary; it is not and will never be a distributed execution system or a scheduler, and this benchmark must not grow into one.
- Assembling the full T65 criteria matrix or the acceptance gate itself — this ticket supplies one input to that gate, not the gate.
