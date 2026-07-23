# 037 · T27 — C19: crash-safety and I/O fault-injection tests

> **Milestone:** M1 · **Size:** M · **Type:** feature (tests) · **Components:** C19
> **Branch:** `feat/t27-crash-safety-fault-injection-tests` · **Depends on:** T19, T24, T0.6 · **Blocks:** T28

## Why / context
C19 is the crash-proof record from which everything else is derived, and its promise is only as good as the evidence that an abrupt kill or a failing sink cannot corrupt it (arch.md C19 · Event stream). This ticket builds the fault-injection suite C28 requires — "kill-points around every event write, disk-full, and slow or failing sinks" (C28 · Testing surface) — against the event-stream writer (T19) driving through the injected sink and run-store contract fixed in T0.6, exercised by the real run-loop driver (T24). It proves two things the spec asserts but nothing yet checks: that killing the process at any moment leaves a valid prefix with at most one trailing partial record and gapless sequences, and that an induced mid-run sink failure cancels the run and exits with the distinct sink-failure code. It blocks the M1 demo (T28), whose event-stream walker assumes the stream is trustworthy under these faults.

## Objective
Build an automated fault-injection test suite proving the C19 crash-safety and sink-failure acceptance criteria hold under adversarial kill-points and I/O faults, using the injected-sink seam and record header fixed by T0.6 and the writer and driver delivered by T19 and T24. Concretely:

- **Crash / kill-point harness.** A test facility that spawns a real child run of a small fixture pipeline through the run store, kills the child abruptly (uncatchable signal — no chance to run an exit handler) at randomized points across many trials, then parses whatever the stream file contains and asserts the prefix invariant.
- **Kill-point coverage around every event write.** Drive kills so they land at, before, and mid-way through individual event writes — not only at coarse wall-clock moments — so the "around every event write" clause of C28 is genuinely exercised rather than approximated.
- **Prefix + gapless-sequence assertions.** A stream reader/validator (or reuse of T19's reader) that: parses every complete record; tolerates and discards at most one trailing partial record; rejects two-or-more trailing partials or any interior corruption; and asserts sequence numbers are gapless and strictly increasing within the run.
- **Run-identified-prefix assertion.** Assert that even a stream truncated to its first record still identifies its run completely, because the run-started event carries the full run-artifact header (C19).
- **Disk-full injection.** A sink (or store base) that reports no-space at a controllable point, driven both mid-run and at final-flush, asserting the run reacts as a mid-run sink failure rather than hanging or silently continuing.
- **Failing-sink injection.** A sink whose append or flush returns an error at an induced point, asserting the run moves to cancelling with reason "event stream unwritable," makes a best-effort stderr report, and exits with the distinct sink-failure code by cause (C19, C26).
- **Slow-sink injection.** A sink that delays append/flush, asserting the unwritable-at-shutdown path produces a bounded wait and the sink-failure exit code, never a hang (C16 boundary; asserted here only through the C19 sink seam).
- **Exit-code assertion by cause.** Assert the sink-failure exit code by its documented cause (from the C26 exit-code table / T0.4–T0.6 references), never by a hard-coded magic number in the test.
- **Concurrent-runs disjointness check.** Run two runs of the same binary at once and assert both produce valid, separately-parseable streams in disjoint per-run directories, records partitioning cleanly by run identity.
- **Criteria-matrix wiring.** Register each machine-classed C19 criterion this suite covers in the checked-in criteria matrix (T0.10/T7) so CI verifies the coverage.

## Test plan (write these first — TDD)
Each scenario is independently checkable and derived from a C19 or C28 acceptance criterion. Randomized scenarios fix and record their seed so a failure reproduces.

- **Abrupt kill at a random point yields a valid prefix.** Setup: a small fixture pipeline (a short chain emitting a known sequence of transition events) run as a child process through the run store. Action: after a randomized delay, kill the child with an uncatchable signal so no exit handler runs; repeat across many seeded trials. Expected outcome: for every trial, the stream file parses into a run of complete records followed by at most one trailing partial that the reader discards; no interior record is malformed; the run of complete records is a valid prefix of the events the fixture would have emitted.
- **Kill mid-write leaves at most one partial, never two.** Setup: the kill-point facility configured to fire at, before, and part-way through a single event write. Action: kill at each such point across seeded trials. Expected outcome: the parser finds zero or one trailing partial record and never two or more; a stream containing two trailing partials fails the assertion (guarding the reader's own tolerance from being too lax).
- **Sequence numbers are gapless and strictly increasing after a kill.** Setup: any killed-child stream from the trials above. Action: extract every complete record's sequence number. Expected outcome: the sequence numbers start at the run's first sequence value and increase by exactly one with no gaps and no repeats, up to the last complete record.
- **A one-record stream still identifies its run.** Setup: a child killed so early the stream contains only the run-started record (plus perhaps one partial). Action: parse the single complete record. Expected outcome: it is the run-started event and carries every run-artifact header field known at start (run identity, schema version, pipeline identity, parameters/interval, and the rest), so the run is completely identified from that one record alone.
- **Every record carries run identity and schema version.** Setup: any parsed stream from the trials. Action: inspect each complete record. Expected outcome: every record carries the run identity and the schema version, with no record missing either.
- **Induced mid-run failing sink cancels the run with the sink-failure code.** Setup: a run driven through a sink injected to return an error on a chosen mid-run append. Action: run to the injection point and capture the run's exit code and stderr. Expected outcome: the run moves to cancelling with reason "event stream unwritable," a best-effort final report appears on stderr, and the process exits with the distinct sink-failure exit code identified by cause; the process does not hang.
- **Disk-full mid-run behaves as a sink failure.** Setup: a sink/store base injected to report no-space at a chosen mid-run write. Action: run to that point and capture exit code and stderr. Expected outcome: identical to the failing-sink case — cancelling with "event stream unwritable," best-effort stderr, and the sink-failure exit code; whatever complete records preceded the failure remain a valid, gapless, parseable prefix.
- **Disk-full at final flush produces the sink-failure code, not a corrupt success.** Setup: a sink that succeeds throughout the run but fails the fsync/flush at run end. Action: run to natural completion and capture the exit code. Expected outcome: the run reports the sink-failure exit code rather than a success code, and the stream on disk is still a valid prefix under the reader's tolerance.
- **Slow/unwritable sink at shutdown yields a bounded wait, not a hang.** Setup: a sink whose flush blocks or is unwritable at shutdown. Action: trigger shutdown and measure elapsed time to process exit. Expected outcome: the process waits at most the bounded budget and then exits with the sink-failure exit code; the test asserts termination within a fixed timeout so a regression to a hang fails the suite rather than hanging CI.
- **Run failure is not masked by self-inflicted cancellation.** Setup: (only if a node-failure fixture is in reach at M1) a run where a node fails and the failure path also triggers cancellation, distinct from an externally induced sink failure. Action: capture the exit code. Expected outcome: the run-failure code wins over cancellation — confirming the sink-failure scenarios above are asserting the sink cause specifically, not a generic cancellation code.
- **Sink-failure exit code is asserted by cause, not by literal.** Setup: the exit-code reference from C26 (via T0.4/T0.6). Action: have each sink-failure scenario compare the observed code against the named sink-failure cause. Expected outcome: the assertion resolves through the documented mapping, so renumbering the table in one place keeps the tests correct and no test hard-codes a magic number.
- **Two concurrent runs produce disjoint, individually valid streams.** Setup: two runs of the same fixture binary started concurrently against the same run-store base. Action: let both complete, then parse both stream files. Expected outcome: each writes under its own `<base>/<pipeline>/<run-id>/` directory (disjoint files), each stream is independently valid with gapless sequences, and concatenating the two partitions cleanly by the run identity every record carries. (This overlaps T67's remit; here it is asserted only as the fault-suite's concurrency-safety check.)
- **The suite is deterministic-on-failure.** Setup: the randomized kill-point and injection scenarios. Action: run the suite; on any failure, read the reported seed. Expected outcome: re-running with that seed reproduces the same kill/injection point, so a CI failure is diagnosable rather than a flake.

## Definition of done
- [ ] A kill-point harness spawns a real child run through the run store and kills it abruptly with an uncatchable signal (no exit handler runs) at randomized points across many seeded trials (C19: "killing the process abruptly at any moment"; C28: "kill-points around every event write").
- [ ] Kills are driven to land at, before, and mid-way through individual event writes, so the "around every event write" clause of C28 is exercised, not approximated.
- [ ] For every killed-child stream, the reader parses all complete records and tolerates and discards at most one trailing partial, and the suite asserts a stream with two-or-more trailing partials or any interior corruption fails (C19: "every record but at most one trailing partial is valid and parseable").
- [ ] The suite asserts sequence numbers are gapless and strictly increasing within the run for every killed-child stream (C19: "sequence numbers are gapless and strictly increasing within a run").
- [ ] The suite asserts a stream truncated to its first complete record is the run-started event carrying every run-artifact header field known at start, so the run is completely identified (C19: run-started event carries the full artifact header).
- [ ] The suite asserts every complete record carries the run identity and schema version (C19: "every record carries the run identity and schema version").
- [ ] An induced mid-run failing-sink scenario asserts the run moves to cancelling with reason "event stream unwritable," makes a best-effort stderr report, and exits with the distinct sink-failure exit code without hanging (C19: "an induced mid-run sink failure cancels the run and exits with the sink-failure code"; C28: "failing sinks").
- [ ] A disk-full injection scenario (mid-run and at final flush) asserts the same sink-failure cancellation path and exit code, and that complete records preceding the failure remain a valid gapless prefix (C28: "disk-full").
- [ ] A slow/unwritable-sink-at-shutdown scenario asserts a bounded wait and the sink-failure exit code within a fixed test timeout, never a hang (C16 boundary; C28: "slow or failing sinks").
- [ ] Every sink-failure exit-code assertion resolves the code by its documented cause (C26 exit-code table via T0.4/T0.6), and no test hard-codes a magic exit-code number.
- [ ] A concurrent-runs scenario asserts two runs of the same binary write disjoint per-run directories and both produce independently valid streams that partition cleanly by run identity (C19: "two simultaneous runs write disjoint files and both produce valid streams").
- [ ] Every randomized scenario records and reports its seed so a CI failure reproduces deterministically, and the suite terminates within fixed timeouts so a hang-regression fails rather than stalling CI.
- [ ] Each machine-classed C19 criterion this suite covers is registered in the checked-in criteria matrix (T0.10/T7), and the matrix verification passes in CI.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
None were open in the ticket. Two implementation choices arose while building the
suite and are resolved here as prose (per the ticket-conventions open-questions
duty):

- **Real child-process kill vs deterministic truncation.** The DoD's kill-point
  clause reads as a real child killed by an uncatchable signal, but the T27
  process rules require the suite to be deterministic in CI — *"no real crashes of
  the test process, no wall-clock/network; simulate a 'crash' as a
  truncated/partial stream that the tolerant reader must still parse up to the
  last complete record."* Resolution: the crash is simulated by truncating the
  **genuine** bytes the real `EventStreamWriter` produced (the default local-file
  sink does not fsync per event — T0.6 §6, so the crash-visible bytes are exactly
  the appended-so-far prefix, possibly cut mid-line) at **seeded** offsets landing
  *at*, *before*, and *part-way through* each event write, then feeding the
  truncation to the real tolerant reader. This exercises the identical invariant an
  uncatchable-signal kill would (a valid prefix with ≤1 trailing partial, gapless
  sequences) with none of the process-kill nondeterminism, and every randomized
  trial records its seed so a CI failure reproduces exactly. This is the process
  rule's explicit instruction, not a weakening of the acceptance.

- **Where the sink-failure "moves to cancelling / exits with the sink-failure
  code" contract is asserted.** The merged M1 run-loop driver (T24) deliberately
  **absorbs** a sink fault (`let _ = writer.…`) and scoped the cancel-and-exit
  *reaction* — the cancellation fan-out, the best-effort stderr report, and the
  numeric exit code — out to T36 (signals/flush) and T55 (the C26 exit-code
  table, still `unmapped`). That absorption is by design (the T24 module doc lists
  fault injection T27 and cancellation T34/T36 as later tickets), **not** a safety
  bug, so no production behavior was changed. Resolution: the sink-failure contract
  is asserted at the **writer/sink seam** — the exact seam T0.6 §5/§6 says T27
  binds against — where the documented `SinkFault { reason: "event stream
  unwritable" }` actually surfaces and `RunOutcome::Cancelled` is the run-level
  outcome. The exit code is resolved **by its documented cause** (the
  `EVENT_STREAM_UNWRITABLE` constant + the `cancelled` outcome), never by a
  hard-coded number, so renumbering the C26 table in one place keeps the tests
  correct. If a later ticket wires the driver's cancel-and-exit reaction, it
  inherits this same cause without changing these tests.

## Out of scope
- The event-stream writer itself — the record encoding, the gapless-sequence machinery, and the run-started event carrying the full header — is T19; this ticket only asserts the guarantees the writer already provides.
- The run-loop driver and its run-started/run-finished emission and zombie-at-exit handling are T24; this ticket drives the real driver but does not build or modify it.
- The sink shape, base-location surface, directory layout, run identity, and flush/failure/exit-code contract are fixed by T0.6; this ticket injects fault variants of that sink and asserts against that contract rather than re-deciding any of it.
- The signal-handling, final-flush, and per-run temp-dir cleanup path is T36; the slow-sink-at-shutdown assertion here checks only the C19 sink seam's bounded-wait outcome, not the OS-signal wiring T36 owns.
- Folding a crashed stream into a run artifact via the fold verb is C22/C26 (T42, and the fold verb under T55); this ticket asserts only that the raw stream survives the crash, not the artifact derived from it.
- The full two-concurrent-runs test is T67; the concurrency check here is scoped to the fault suite's disjointness-and-validity assertion and does not subsume T67.
- The bounded-memory chain test (T26) and the termination property test (T25) are their own tickets; this suite does not fold in memory-growth or every-node-terminates checks.
- Any temptation to make the tests depend on a scheduler, a metadata store, network storage, or cross-process coordination — dagr asks only for a run-store base, so the suite must exercise faults through the injected sink and a local base and never reach for infrastructure the tool does not own.
