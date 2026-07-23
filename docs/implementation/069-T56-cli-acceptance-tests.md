# 069 · T56 — C26: CLI acceptance tests

> **Milestone:** M4 · **Size:** M · **Type:** feature (tests) · **Components:** C26
> **Branch:** `feat/t56-cli-acceptance-tests` · **Depends on:** T55 · **Blocks:** T63

## Why / context
T55 wired the library-supplied command-line contract (C26): the verbs, the typed-parameter parsing, the reserved-flag namespace, and the exit-code-by-cause table. This ticket proves that contract holds as an operator sees it — from the outside, by invoking a compiled binary and observing exit codes, stdout/stderr, and artifacts on disk — rather than through unit tests of internal functions. It is governed entirely by C26 · Command-line contract (arch.md), with the artifact markings it asserts owned by C22 (`bootstrap-failed`, the single-node-replay variant, `not-requested`) and the durable-input rehydration path owned by C27 (C26's "Run a single node"). Because C26 promises that *every verb behaves identically across all pipelines*, the suite runs the same assertions against **two** distinct sample pipelines. T63 (the M4 demo) depends on this suite being green.

## Objective
Build a black-box acceptance suite that drives two compiled sample-pipeline binaries through the C26 verbs and asserts every observable outcome. Concretely:
- Author (or reuse from earlier milestones) **two** sample pipelines with different shapes — enough to exercise a durable stage boundary, a no-input standalone node, a node reachable only from a prior run, a non-teardown node that can be made to fail, and a skip-only path. The two must differ structurally so that "identical verb behaviour" is a real claim, not a tautology.
- Run each verb (`graph`, `validate`, `render`, `run`, single-node replay, `resume` where stubbed, `fold`, `prune`, and the no-argument invocation) against both binaries and assert the observable contract.
- Cover **every** exit code in the C26 table — success (including a skip-only run), run failure, assembly failure, bootstrap failure, cancellation, resume/replay refusal, sink failure, and invalid usage — with at least one scenario per code, keyed to the exit-code table so the table's exhaustiveness-over-verbs-and-causes claim is checked.
- Assert the stop-on-first-failure precedence case: a run where a node failure triggers self-cancellation still reports the run-failure code, not the cancellation code.
- Assert single-node replay end to end: rehydrated durable inputs, the non-durable-input refusal that names the offending input and why, the standalone no-input run, and the `not-requested` marking in the replay-variant artifact.
- Assert `prune` by count and by age, first proving nothing was deleted implicitly by any earlier run in the same run store.
- Structure the assertions so a single helper enforces byte-for-byte identical verb behaviour across the two binaries where the spec demands it (differing only where the pipelines legitimately differ).

## Test plan (write these first — TDD)
Each scenario is black-box: build the sample binary, invoke it as a subprocess with argument vectors and (where relevant) a temp run store, then assert on exit code, captured stdout/stderr, and files written under the run store. Unless stated, run every scenario against **both** sample binaries and assert the contract holds for each.

**Verb parity**
- *Identical verbs across pipelines.* Given the two compiled sample binaries, when each is invoked with the `graph`, `validate`, and `render` verbs, then both accept the same verb names and flag namespace, both exit with the same success code on a valid graph, and the shape of the output (the presence of the graph/diagram, not its pipeline-specific content) matches — proving the verb set is library-supplied, not per-pipeline.
- *No-argument help.* Given a sample binary, when it is invoked with no arguments, then it prints the list of available verbs to stdout and exits with the success code (not the invalid-usage code).
- *Invalid usage.* Given a sample binary, when it is invoked with an unknown verb or a malformed flag, then it exits with the invalid-usage code and writes a usage message to stderr.

**Validate**
- *Validate on a healthy graph.* Given a binary whose assembly succeeds, when `validate` runs, then it exits with the success code and no run store is opened or written (an inspection verb runs assembly with no store).
- *Validate prints every problem.* Given a sample binary deliberately built to fail assembly with at least two independent problems, when `validate` runs, then it exits non-zero with the assembly-failure code and its output enumerates *all* problems found, not just the first.

**Exit-code table coverage** (one scenario per code, each cross-referenced to the C26 table)
- *Success — normal run.* A run in which every requested node succeeds exits with the success code and writes a completed run artifact.
- *Success — skip-only run.* A run whose requested nodes all resolve to a skip (no non-teardown node executes) still exits with the success code.
- *Run failure.* A run in which one non-teardown node ends `failed` (or `timed-out`) exits with the run-failure code, and the artifact records that terminal state.
- *Run failure beats cancellation (stop-on-first-failure precedence).* Given a pipeline whose stop-on-first-failure behaviour cancels siblings after a node fails, when the run is executed, then it exits with the **run-failure** code — the self-inflicted cancellation does not mask or replace the failure — and the artifact attributes the outcome to the failure, not to cancellation.
- *Cancellation.* Given a run terminated by an externally originated signal with **no** run failure present, when it shuts down within the budget, then it exits with the cancellation code (distinct from run failure).
- *Assembly failure.* Given a binary whose graph fails assembly, when `run` is invoked, then it exits with the assembly-failure code and — because run verbs mint identity and open the store before assembly — an `assembly-failed` artifact exists in the run store with the complete error list and zero attempts.
- *Bootstrap failure.* Given a valid graph but an invalid invocation (see the parameter scenario below), when `run` is invoked, then it exits with the bootstrap-failure code and a `bootstrap-failed` artifact (distinct from `assembly-failed`) is written before any node executes.
- *Sink failure.* Given a run store location that cannot be written (or a sink made to fail), when `run` is invoked, then the binary exits with the sink-failure code within a bounded wait rather than hanging.
- *Resume/replay refusal.* Covered by the non-durable-input replay refusal below, which must exit with the resume-refusal code.
- *Assert exhaustiveness.* A meta-check enumerates the codes exercised above against the C26 exit-code table and fails if any table entry has no scenario — so adding a code without a test breaks the build.

**Parameters at bootstrap**
- *Invalid parameter rejected before execution.* Given a binary with a typed parameter struct, when `run` is invoked with a value that fails the declared parameter validation, then it exits with the bootstrap-failure code, a `bootstrap-failed` artifact is written, and no node-execution events appear in the event stream — proving rejection happens at bootstrap, after assembly, before any node runs.
- *Parameter/flag collision rejected.* Given a sample pipeline whose parameter struct declares a name that collides with a reserved library flag, when the binary is invoked, then it is rejected (the pipeline parameter cannot shadow or collide with a library flag) with a message naming the collision.

**Single-node replay**
- *Replay with rehydrated durable inputs.* Given a completed prior run R that recorded durable references for node N's inputs, when the binary is invoked to replay N from R, then N's inputs are rehydrated from those references, N re-executes, and it exits with the success code.
- *Replay refused on a non-durable input.* Given a prior run R in which one of N's inputs was **not** durable, when replay of N from R is requested, then the binary refuses with the resume-refusal code and the error names the specific input and why it is not replayable.
- *Standalone no-input replay.* Given a node that consumes nothing, when it is replayed with no prior run supplied, then it runs standalone and exits with the success code.
- *`not-requested` marking.* Given a successful single-node replay, when its artifact is inspected, then it is the replay-variant artifact and every node outside the request is marked `not-requested` (an artifact marking, not a terminal state) while the replayed node carries its real terminal state.

**Fold (crashed-run path)**
- *Fold a partial stream.* Given a killed run that left a partial event stream, when the `fold` verb is invoked on that stream, then it produces an interrupted run artifact for the dead run and exits with the success code — the crash-clause path.

**Prune**
- *Nothing deleted implicitly.* Given a run store populated by several prior runs in this suite, when the store is inspected before any `prune` is invoked, then every prior run directory is still present — proving retention is not applied implicitly at run end.
- *Prune by count.* Given a run store with more runs than the requested keep-count, when `prune` is invoked by count, then exactly the excess oldest runs are removed, the newest keep-count remain, and it exits with the success code.
- *Prune by age.* Given a run store with runs older and newer than a requested age threshold, when `prune` is invoked by age, then only runs past the threshold are removed and the rest remain, exiting with the success code.

## Definition of done
- [ ] Two structurally distinct sample-pipeline binaries exist and are built as part of the suite, together covering: a durable stage boundary, a no-input standalone node, a node replayable from a prior run, a controllable non-teardown failure, an assembly-failure variant, a skip-only path, and a parameter/flag-collision variant.
- [ ] A parity helper asserts the C26 verbs behave identically across both binaries wherever the spec demands identical behaviour, differing only where the pipelines legitimately differ.
- [ ] Every verb behaves identically across both sample binaries: `graph`, `validate`, `render`, `run`, single-node replay, `resume` (as stubbed by T55), `fold`, `prune`, and the no-argument invocation.
- [ ] `validate` exits non-zero on any assembly failure and its output enumerates every problem found, not just the first.
- [ ] `run` exits with the run-failure code whenever a non-teardown node ended `failed` or `timed-out`, including under stop-on-first-failure where the consequent self-cancellation does not mask the failure; the artifact attributes the outcome to the failure, not to cancellation.
- [ ] Every code in the C26 exit-code table has at least one black-box scenario — success (incl. skip-only), run failure, assembly failure, bootstrap failure, cancellation, resume/replay refusal, sink failure, invalid usage — and a meta-check fails if any table entry is left untested.
- [ ] Invalid parameters are rejected at bootstrap, after assembly and before any node executes, producing a `bootstrap-failed` artifact and no node-execution events.
- [ ] A pipeline parameter that would shadow or collide with a reserved library flag is rejected with a message naming the collision.
- [ ] Running a binary with no arguments prints the available verbs and exits with the success (not invalid-usage) code.
- [ ] Single-node replay rehydrates durable inputs from a prior run's recorded references; refuses (with the resume-refusal code) a node whose input was not durable, naming that input and why; runs a no-input node standalone with no prior run; and its artifact is the replay variant with nodes outside the request marked `not-requested`.
- [ ] The `fold` verb, invoked on a killed run's partial stream, produces the interrupted run artifact (the crash-clause path).
- [ ] `prune` removes runs by count and by age, and a pre-prune assertion confirms nothing was deleted implicitly by any earlier run in the same store.
- [ ] All scenarios are true black-box tests: they build/invoke a compiled binary as a subprocess and assert only on exit code, captured stdout/stderr, and files under the run store — no reaching into library internals.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
None.

## Out of scope
- The resume algorithm itself and its acceptance suite — C27 core lands in T58 and is tested in T59; here `resume` is exercised only as the stubbed verb T55 provided, and replay refusal only insofar as it shares the resume-refusal exit code.
- Defining or renumbering the exit-code table, the verb set, parameter parsing, or the reserved flag namespace — those are T55's deliverables; this ticket asserts them, it does not author them.
- Durable-output declaration/recording (T57) and structure-snapshot or full-pipeline fakes harnesses (T61, T62) — reused as available, not built here.
- The M4 kill/resume/review demo (T63), which consumes this suite.
- Signal-handling and shutdown-budget mechanics beyond observing the cancellation and sink-failure exit codes at the CLI boundary (owned by C14/C16).
- Anything past the permanent scope boundary: no scheduling of when a run happens, no cross-process coordination of concurrent runs, no distributed execution, no metadata store, no web interface, and no runtime graph-shape changes — the suite drives a single already-triggered binary and never introduces a trigger, a coordinator, or a mutable graph.
