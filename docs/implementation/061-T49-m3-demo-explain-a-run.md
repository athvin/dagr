# 061 · T49 — M3 demo: explain a run from artifacts

> **Milestone:** M3 · **Size:** M · **Type:** feature (demo) · **Components:** M3 gate
> **Branch:** `feat/t49-m3-demo-explain-a-run` · **Depends on:** T41, T43, T44, T45, T47, T48, T68 · **Blocks:** T64, T65

## Why / context
This ticket is the M3 done-when, executed in CI: the milestone "It explains itself" is proven when a run produces both artifacts, the rendered diagram is reviewable, and the question *"which node was slowest, and was it waiting or working?"* is answerable from the artifacts alone — without reading a single log line (arch.md, Build order, M3). It is the milestone-capping demo in the same family as T28 (M1) and T38 (M2): a single, self-contained CI example that exercises the M3 components end to end rather than a new component. It builds on fingerprints (C21/T41), the run summary and critical path (C22/T43), node metrics (C23/T44), logging integration (C25/T45), run-overlay rendering (C24/T47), artifact validation and the fixture corpus (T48), and the crashed-run fold path (T68). It governs by the acceptance criteria of C22 · Run artifact (arch.md §C22) and the graph-artifact pairing of C20, and it is the gate that T64 (docs/cookbook) and T65 (system acceptance) depend on.

## Objective
Build a CI-run example pipeline plus a test harness that, from artifacts alone, demonstrates the M3 claim. Concretely:

- A small, deterministic reference pipeline (a handful of nodes with at least one clear structural bottleneck and at least one node that is demonstrably resource/queue-limited rather than compute-limited) whose sole purpose is to make "slowest node" and "waiting vs working" unambiguous and stable across runs.
- Run the pipeline end to end in CI so it emits both a graph artifact (C20) and a run artifact (C22) into the run store, and confirm the two are joinable (matching structural fingerprint, C21).
- Render the run-overlaid diagram (C24/C47) in both DOT and Mermaid from the two artifacts, and assert the overlay is produced and structurally sound — no live pipeline access.
- A programmatic "explainer" step in the test that reads only the artifacts and answers, mechanically: (1) which node was slowest by attempt elapsed time, and (2) whether that node's time was dominated by waiting (ready-wait plus permit-wait phases) or by working (executing phase) — decided from the per-attempt monotonic-offset phase durations in the run artifact, cross-checked against the summary's total-elapsed vs critical-path numbers.
- Assert the explainer reached its answer with zero reads of the event-log/stdout stream and zero access to the producing binary — artifacts only.
- Register the demo test in the checked-in criteria matrix (T0.10) as the machine test that covers the M3 done-when and system criterion 5 ("a run's duration and resource profile can be answered entirely from artifacts").

## Test plan (write these first — TDD)

1. **Both artifacts are produced by one run.** Setup: the reference pipeline binary and a temp run-store base. Action: invoke the graph-emit verb and then the run verb against an empty environment. Expected: a graph artifact and a run artifact exist in the run store; both parse; both validate against their published schemas; the run outcome is the successful full-run outcome.

2. **Artifacts are joinable.** Setup: the graph and run artifacts from scenario 1. Action: read the structural fingerprint from each. Expected: the run artifact's structural fingerprint equals the graph artifact's fingerprint from the same build (C22 fingerprint-match criterion).

3. **Node coverage.** Setup: the two artifacts. Action: collect the node set from the graph artifact and the node set covered by the run artifact. Expected: every node in the graph artifact appears at least once in the run artifact, including any never-ran nodes carrying propagated terminal states (C22 node-coverage criterion).

4. **Phase durations sum exactly.** Setup: the run artifact. Action: for each attempt record, sum its named phase durations and compare to the attempt total. Expected: exact equality for every attempt (both derive from monotonic offsets), with no floating-point slack.

5. **Slowest node is identifiable from artifacts alone.** Setup: only the run artifact loaded (event stream and binary deliberately unavailable to the explainer). Action: the explainer ranks attempts by total elapsed and names the slowest node. Expected: it returns the pipeline's designed bottleneck node deterministically across repeated runs.

6. **"Waiting or working" is answerable from artifacts alone.** Setup: the run artifact only. Action: for the slowest node, the explainer compares waiting phases (ready-wait + permit-wait) against the working phase (executing). Expected: it classifies the designed compute-bound node as "working" and, for a variant where the pipeline is arranged so a node is queue/permit-limited, classifies that node as "waiting" — each classification is a mechanical, reproducible verdict, not a heuristic guess.

7. **Structure-limited vs resource-limited is distinguishable at the summary.** Setup: the run-artifact summary (total elapsed, critical-path time). Action: compare the two summary numbers. Expected: the demo asserts the documented relationship for this pipeline (e.g. total elapsed close to critical path ⇒ structure-limited; total elapsed well above critical path ⇒ resource-limited), consistent with T43's distinguishing test and system criterion 5.

8. **Metrics reached the artifact unmodified.** Setup: a node that attaches a task metric and relies on framework-contributed metrics. Action: read that node's attempt record. Expected: the task metric is present with its declared value and unit-suffixed name, and framework metrics (e.g. allocator-attributed peak memory, phase timings) are present too (C23).

9. **Overlay renders from artifacts only.** Setup: the graph and run artifacts, and no running pipeline. Action: render the run-overlaid diagram in DOT and in Mermaid. Expected: both outputs are produced without touching the binary; `dot` parses the DOT and the Mermaid parser accepts the Mermaid in CI; every node/edge appears; terminal states map to documented distinct styles with originated skips distinguishable from propagated ones (C24/C47).

10. **No log line was consulted.** Setup: the full demo flow. Action: run the explainer with the event stream / log output path made inaccessible (or asserted-unread) for the duration of the explain step. Expected: the explainer still produces answers 5, 6, and 7 — proving the M3 claim that the questions are answerable "without reading a single log line."

11. **No non-allowlisted environment leaks (sentinel).** Setup: a sentinel environment variable set but not on the pipeline's declared allowlist. Action: run the demo and scan the emitted run artifact for the sentinel value. Expected: the sentinel appears nowhere in the artifact (C22 allowlist criterion).

12. **Determinism of the demo.** Setup: two runs of the reference pipeline in CI. Action: run the explainer on each. Expected: identical slowest-node and waiting-vs-working verdicts, and identical structural fingerprints (generation time aside) — the demo does not flake, so it can gate the milestone.

13. **Criteria-matrix wiring.** Setup: the checked-in criteria matrix (T0.10). Action: run the matrix-coverage CI check. Expected: the M3 done-when and system criterion 5 map to this demo test id, and the matrix-coverage check passes.

## Definition of done
- [ ] A deterministic reference pipeline exists whose structure makes "slowest node" and "waiting vs working" unambiguous, with at least one compute-bound and one wait/queue-limited node.
- [ ] A CI test runs the pipeline end to end and produces both a graph artifact (C20) and a run artifact (C22) in a temp run store, each validating against its published schema.
- [ ] The run artifact names a structural fingerprint matching the graph artifact from the same build (C21/C22).
- [ ] Every node in the graph artifact appears at least once in the run artifact, including never-ran nodes with propagated terminal states (C22).
- [ ] For every attempt in the run artifact, named phase durations sum exactly to the attempt total (monotonic-offset derivation) (C22).
- [ ] A programmatic explainer, reading artifacts only, identifies the slowest node by attempt elapsed time (C22, system criterion 5).
- [ ] The explainer classifies the slowest node as waiting vs working from its per-attempt phase durations, and the demo cross-checks the summary's total-elapsed vs critical-path numbers to distinguish structure-limited from resource-limited (C22, T43, system criterion 5).
- [ ] Task-attached and framework-contributed metrics reach the run artifact unmodified and are read by the explainer where relevant (C23).
- [ ] The run-overlaid diagram is rendered in DOT and Mermaid from artifacts only; both are accepted by their reference tools in CI; every node/edge appears; terminal states map to documented distinct styles with originated vs propagated skips distinguishable (C24/C47).
- [ ] The explain step is proven to consume no event-log/stdout stream and no access to the producing binary — the "without reading a single log line" claim is enforced by the test.
- [ ] No environment value outside the declared allowlist appears in the run artifact, verified with a planted sentinel (C22).
- [ ] The demo is deterministic across repeated CI runs (verdicts and fingerprints stable, generation time aside).
- [ ] This demo test is registered in the checked-in criteria matrix (T0.10) as the machine test covering the M3 done-when and system criterion 5, and the matrix-coverage CI check passes.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
None.

## Out of scope
- New component behaviour: this ticket wires together C20–C25 and C47 as they already exist; it does not add fields, metrics, phases, or render styles. Gaps found here become tickets against the owning component (C22/T43, C23/T44, C24/T47), not this demo.
- The crashed-run and single-node-replay artifact variants beyond the successful full run — the crash fold path is exercised by T68, and replay/verb wiring belongs to M4 (C26/T55). This demo asserts the full-run happy path only.
- The M4 done-when (kill, resume, review) and its structure-diff review claim — that is T63/T65, not this ticket.
- README/quickstart/cookbook prose and the full system acceptance gate — those are T64 and T65, which this ticket blocks; keep them downstream.
- Scale/benchmark artifacts (the ten-thousand-attempt corpus is T48/T69); this demo uses a small pipeline chosen for legibility, not volume.
- Scope-boundary temptations to resist: do not add any scheduling, distributed execution, metadata-store query layer, web UI, or DSL to make the explanation "nicer"; the explainer is a plain in-test reader over local artifact bytes, and the graph shape is fixed at build time.
