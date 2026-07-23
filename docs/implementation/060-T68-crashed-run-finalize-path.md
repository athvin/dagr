# 060 · T68 — Crashed-run finalize path

> **Milestone:** M3 · **Size:** S · **Type:** feature (tests) · **Components:** C22
> **Branch:** `feat/t68-crashed-run-finalize-path` · **Depends on:** T42 · **Blocks:** T49

## Why / context
System criterion 3 promises that *every* run produces artifacts — including runs that crashed — and the spec's chosen mechanism is that a dead run's artifact is produced by the *next* invocation of the binary, by folding the partial event stream the dead run left behind (arch.md §"C22 · Run artifact"; §"System-level acceptance" criterion 3; §"C19 · Event stream"). T42 already delivered the standalone fold function and tested it over *hand-built* truncated streams. This ticket closes the remaining gap: it takes a **real** run, kills it abruptly mid-execution, and proves that the on-disk stream that survived — written continuously through the run store's sink, not buffered to exit — folds through T42's function into an artifact correctly marked `interrupted` and containing everything up to the kill. This is the integration proof for the crash clause; the CLI `fold` *verb* that operators will actually invoke lands in T55 (C26), so this ticket exercises the library path, not the command line. It is a direct prerequisite for the M3 demo (T49), which explains a run from its artifacts.

## Objective
Deliver an integration test (and any small test-support scaffolding it needs) that drives a real run to a live, mid-execution state, kills its process abruptly, and folds the surviving on-disk event stream with T42's standalone function into a valid `interrupted` run artifact — proving the crash clause of system criterion 3 through the tested (library) path.

Concrete pieces of work:
- A test harness that launches a real pipeline run as a **separate OS process** (so it can be killed abruptly with no chance to run an exit handler — the dominant container failure mode C19 exists to survive), writing its event stream to a run-store directory on disk that outlives the process.
- A run fixture built for this purpose: a small pipeline whose execution reliably reaches a mid-run state (at least one attempt started and recorded, at least one node still to run) and holds there long enough to be killed deterministically — synchronization via an observable stream event or an on-disk marker, never a fixed sleep.
- The kill step: send an abrupt, uncatchable termination signal (equivalent of `SIGKILL`) to the child once it is confirmed mid-run, leaving no opportunity for graceful finalization.
- The fold step: after the child is dead, read the surviving stream from the run store and fold it with T42's standalone fold function — with **no access to the original (dead) run's live state**, exactly as a later binary invocation would have.
- Assertions over the produced artifact: outcome marked `interrupted`; every event recorded before the kill is present; the header is complete from the run-started event; at most one trailing partial record was tolerated and discarded; no error was raised for that single partial.
- Confirm the reverse direction of the guarantee too: a run that is *not* interrupted (allowed to finish) does **not** produce an `interrupted` artifact — so the marking is a genuine signal, not always-on.

## Test plan (write these first — TDD)
Each scenario is independent and asserts an observable property of a real (or captured-from-real) crashed run's folded artifact. Timing is coordinated by observing stream events or an on-disk marker — never by a fixed-duration sleep — so the tests are deterministic and not flaky.

- **A real killed run folds into an interrupted artifact.** Setup: launch the run fixture as a child process pointed at a temp run-store directory; wait until the stream on disk shows the run has started at least one attempt (a node is executing) with at least one node still pending. Action: abruptly kill the child (uncatchable signal), confirm it is dead, then fold the surviving stream with T42's standalone function. Expected: the fold succeeds and returns an artifact whose overall outcome is `interrupted`.
- **Everything up to the crash is present.** Setup: same as above, but arrange the fixture so a known set of transitions (run-started, node-ready, admitted, attempt-started for node A) is recorded before the kill point. Action: kill, then fold. Expected: the artifact contains every one of those recorded transitions/attempts — the folded body reflects the state the run had reached, not an empty or exit-only record.
- **Header is complete despite the crash.** Setup: kill the child as early as possible — right after the run-started event and one following event are on disk. Action: fold. Expected: the artifact header (run identity, pipeline identity, both fingerprint hashes, invocation parameters, data interval, resume lineage when present, allowlisted environment values) is fully populated from the run-started event; only the overall outcome (`interrupted`) and the summary reflect the truncation.
- **At most one trailing partial is tolerated, silently.** Setup: kill the child at an arbitrary instant so the last stream write may be a byte-truncated partial record (the realistic outcome of an abrupt kill mid-write). Action: fold. Expected: the fold discards at most one trailing partial record and raises no error for it; the artifact still folds and is marked `interrupted`. (This is the real-kill counterpart to T42's hand-built trailing-partial test.)
- **The dead run's live state is never touched.** Setup: kill the child, then delete or make inaccessible everything except the stream file the fold is handed. Action: fold the stream bytes alone. Expected: the fold succeeds using only the stream — it opens no run store beyond the given bytes, no network, and no live graph, matching the "produced by the next invocation" contract (C19 fold criterion; C22).
- **Kill at different points all fold.** Setup: run the fixture several times, killing at distinct observable checkpoints (just after run-started; after the first attempt-started; after a node reached a terminal state with others pending). Action: fold each surviving stream. Expected: every one folds successfully into an `interrupted` artifact that contains exactly the transitions recorded before its kill point — demonstrating "killing at any moment" survives (C19 abrupt-kill criterion) through the finalize path.
- **Interrupted marking is not always-on (negative control).** Setup: launch the same fixture but let it run to natural completion (no kill). Action: fold its (complete) stream. Expected: the resulting artifact is **not** marked `interrupted` — its outcome reflects the actual terminal result — proving the interrupted marking distinguishes crashed runs from finished ones.
- **Crash-surviving artifact requires only a persistent store, nothing else.** Setup: point the child's run store at a plain on-disk directory (no server, no database, no scheduler running). Action: run, kill, fold. Expected: the interrupted artifact is produced from the on-disk stream alone — confirming criterion 7's "the store location is the only operational requirement" for the crash path.

## Definition of done
- [ ] A test launches a real run as a separate OS process, drives it to a confirmed mid-run state, and kills it abruptly (uncatchable signal, no chance to run an exit handler).
- [ ] The surviving on-disk event stream folds through T42's **standalone** fold function — with no access to the original run's live state — into a run artifact (C22; C19 fold criterion).
- [ ] The produced artifact's overall outcome is marked `interrupted` and it contains every transition/attempt recorded before the kill (C22 crashed-run criterion; system criterion 3 crash clause).
- [ ] The fold tolerates and discards at most one trailing partial record produced by the abrupt kill, raising no error for that single partial (C19).
- [ ] The artifact header is complete from the run-started event even when the kill lands one event later; only outcome and summary reflect the truncation (C22; C19 run-started-completeness).
- [ ] Killing at multiple distinct observable checkpoints each yields a valid `interrupted` artifact containing exactly the transitions recorded before that point (C19 "killing at any moment").
- [ ] A negative-control test confirms a run allowed to finish is **not** marked `interrupted`, so the marking is a genuine crash signal.
- [ ] The crash path uses only an on-disk run-store directory — no server, database, or scheduler (system criterion 7 crash-survival clause).
- [ ] Test synchronization is deterministic (observed stream event or on-disk marker), with no fixed-duration sleeps, so the test is not flaky.
- [ ] The run fixture and any process-launch/kill test scaffolding are checked in and reusable by T49; public test-support items (if any) carry rustdoc.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
None.

## Out of scope
- The standalone fold function itself and its unit tests over hand-built streams — delivered by **T42**; this ticket consumes that function and proves it over a real kill.
- The CLI `fold` verb, its argument parsing, and its exit codes — that is **T55 (C26)**; the acceptance criterion "folding a crashed run's stream with the fold *verb* produces the interrupted artifact" is verified there. This ticket exercises the library path the verb will call.
- The `assembly-failed` and `bootstrap-failed` outcome variants, the allowlist positive/negative sentinel checks, phase-sum exactness, node coverage, and the fixture-corpus/schema-compatibility CI — all covered by **T42**; this ticket asserts only the `interrupted` marking and up-to-crash completeness over a real killed run.
- The event-stream writer's own crash properties (gapless sequence numbers, per-event flushing, trailing-partial tolerance at the reader) — those are **C19 / T19** responsibilities; here they are relied upon, not re-tested, except as observed through the folded artifact.
- Cancelled-run and failed-run artifact paths — other clauses of system criterion 3, covered by their own tests; this ticket is scoped to the *crash* clause only.
- The M3 demo that narrates a run from its artifacts — that is **T49**, which this ticket unblocks.
- Anything crossing the permanent scope boundary: this ticket kills a process and folds a static stream into a static artifact. It does not restart, resume, reschedule, or coordinate the dead run; it introduces no daemon, watcher, metadata store, or cross-run querying; and it never alters graph shape. Recovering a crashed run is "fold the stream it left behind," not orchestration.
