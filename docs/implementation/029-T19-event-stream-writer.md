# 029 · T19 — C19: event stream writer

> **Milestone:** M1 · **Size:** M · **Type:** feature · **Components:** C19
> **Branch:** `feat/t19-event-stream-writer` · **Depends on:** T4, T13, T0.6 · **Blocks:** T20, T24, T27, T36, T42

## Why / context
C19 is the crash-proof record of what a run did; every downstream artifact (C22 run artifact, C24 renderers) is derived from it, so it must exist and be well-formed before anything else can be trusted. This ticket builds the append-only writer that emits one single-line record per state transition through the run-store sink from T0.6, using the JSONL event encoding and schema-version semantics locked by T4 and the node identity from T13. It is governed by arch.md `### C19 · Event stream` and the load-bearing phase boundary in `## The shape of a run`, plus the normative event vocabulary in `## Vocabulary — terminal states and trigger rules` (locked by T0.4). The writer must open at bootstrap *before* assembly results are acted on, so even an assembly failure has a place to record itself.

## Objective
Build the append-only event-stream writer for a single run: a component that opens the stream through the T0.6 run-store sink, stamps every record with run identity, schema version, a gapless strictly-increasing sequence number, an informational wall-clock stamp, and an authoritative monotonic offset from run start; emits a `run-started` event carrying the full run-artifact header known at start; writes each record before its transition is considered recorded; and treats a mid-run sink failure as a run-level fault.

Concrete pieces of work:
- A stream-writer type constructed at bootstrap from an injected sink (T0.6), a minted run identity (UUIDv7, operator-overridable), and a captured run-start instant that anchors the monotonic offset.
- A monotonic sequence counter that is gapless and strictly increasing, starting at the `run-started` record.
- Per-record envelope population: run identity, pipeline identity, `schema_version` (semantics per T4), sequence number, wall-clock stamp (informational), and monotonic offset in a fixed unit (authoritative; durations are computed from offsets, never wall clocks).
- One record shape per state-transition in the C19 vocabulary: run started, node became ready, node admitted, attempt started, attempt succeeded, attempt failed, node reached terminal state, zombie-at-exit (C14), run finished. Terminal-state records carry the normative terminal state from the vocabulary; the `run-started` record carries every run-artifact header field known at start (run identity, pipeline identity, both fingerprint hashes when assembly succeeded, parameters and data interval, allowlisted captured environment values, resume lineage when resumed) — everything except overall outcome and summary, which exist only at run end.
- Write-through-then-record discipline: no user-space buffering; each record is appended and flushed to the sink before the transition is treated as recorded. An fsync (delegated to the sink) at run end and at cancellation. The default local-file sink does not fsync per event.
- Mid-run sink-failure handling: on a sink append/flush error, surface a run-level fault that moves the run to cancelling with reason "event stream unwritable," and expose the sink-failure exit code path (best-effort final report to stderr, distinct sink-failure code per C26).
- The per-run directory contract: the stream lives under `<base>/<pipeline>/<run-id>/`, so two simultaneous runs never share a file.
- The stream must be foldable into a run artifact by a standalone function needing no access to the original run (the fold itself is C22/T42; this ticket must produce a stream that satisfies that contract, and a reader that tolerates and discards at most one trailing partial record).

## Test plan (write these first — TDD)
- **Envelope completeness.** Setup: a writer constructed with a fixed run identity, pipeline identity, and schema version, backed by an in-memory capture sink. Action: emit `run-started` followed by one of each transition record. Expected: every captured record, when parsed as one line, carries the same run identity, the same pipeline identity, the schema-version field, a sequence number, a wall-clock stamp field, and a monotonic-offset field — no record is missing any envelope field.
- **Gapless strictly-increasing sequence.** Setup: a writer over a capture sink. Action: emit N records of mixed transition kinds. Expected: the sequence numbers are exactly the contiguous run `0..N-1` (or the documented start value), strictly increasing with no gaps and no repeats, regardless of record kind.
- **Offsets are monotonic and authoritative.** Setup: a writer whose run-start instant is captured, with control over the monotonic clock source in the test. Action: emit several records at advancing monotonic instants, including a case where the wall clock is stepped backward between two records. Expected: every record's monotonic offset is non-decreasing and reflects elapsed-since-run-start; the backward wall-clock step does not make any offset decrease; a duration computed as (later offset − earlier offset) is non-negative and matches the injected elapsed time.
- **run-started carries the full header.** Setup: a bootstrap context with run identity, pipeline identity, both fingerprint hashes, parameters, data interval, an allowlisted-env map, and a resume lineage. Action: emit the `run-started` event. Expected: the record contains every header field known at start and contains neither an overall-outcome field nor a summary field; a consumer reading only this one record can fully identify the run.
- **Header when assembly failed.** Setup: a bootstrap where identity and store opened but assembly then failed (no fingerprint available). Action: open the stream, emit `run-started`, then emit an assembly-failure-shaped `run-finished`. Expected: the stream is a valid two-record stream; `run-started` omits the fingerprint hashes (present only when assembly succeeded) yet still identifies the run; the stream is parseable end to end.
- **Stream opens before assembly is acted on.** Setup: a run-verb bootstrap harness that mints identity and opens the store/stream, then runs assembly and injects an assembly failure. Action: run the bootstrap sequence. Expected: the `run-started` record is present in the stream and was written before the assembly-failure outcome was acted on — i.e., an assembly failure still has a recorded place; inspection verbs (validate, graph, render) run the same assembly with no stream opened at all.
- **Write-through, not buffered.** Setup: a capture sink that records the order of append and flush calls and the point at which the writer returns from recording a transition. Action: record a single transition. Expected: the sink saw the append-and-flush for that record *before* the writer returned / before the transition is treated as recorded; there is no batch of records deferred to run end.
- **fsync at run end and cancellation.** Setup: a sink that counts fsync requests. Action (case A): drive a normal run to `run-finished`. Action (case B): drive a run to cancellation. Expected: in both cases exactly one fsync is requested at the appropriate boundary; no per-event fsync is requested by the default path during steady-state records.
- **Abrupt-kill parseability (crash safety at unit level).** Setup: a completed stream file, then produce a truncated copy that cuts the final record at an arbitrary byte. Action: run the tolerant reader over the truncated copy. Expected: every complete record parses, the single trailing partial record is discarded, and the reader reports success rather than erroring — at most one trailing partial is tolerated. (Full process-kill fault injection is T27; this scenario exercises the reader/writer contract that T27 depends on.)
- **Concurrent-run disjointness.** Setup: two writers for two run identities under the same base and pipeline. Action: interleave records from both. Expected: each writes to its own `<base>/<pipeline>/<run-id>/` path, the two files are disjoint, and each file is independently valid; concatenating both files and partitioning by the per-record run identity recovers each run's records exactly. (The end-to-end two-runs test is T67; this validates the file-path and partitioning contract.)
- **Mid-run sink failure cancels the run.** Setup: a sink configured to fail its append/flush after K successful records. Action: emit records until the failure fires, then continue driving the run. Expected: the writer surfaces a run-level fault; the run transitions to cancelling with reason "event stream unwritable"; the run's terminal path exits with the distinct sink-failure code; a best-effort final report goes to stderr.
- **Store-open failure has no stream.** Setup: a run-store open that fails at bootstrap. Action: attempt to open the store/stream. Expected: no stream file is created, the error is reported to stderr, and the sink-failure exit code is used — the spec promises nothing written.
- **Foldability with no original-run access.** Setup: a valid completed stream file only (no live writer, no run object). Action: hand the file to the fold contract's reader interface used by C22/T42. Expected: the reader consumes the stream and yields the ordered record sequence with envelope fields intact, using nothing but the file — demonstrating the stream is self-contained for folding.

## Definition of done
- [ ] The writer produces an append-only sequence of single-line records written through the T0.6 run-store sink as events occur, never buffered until run end.
- [ ] Every record carries run identity and schema version; records from concurrent runs concatenate and partition safely by run identity.
- [ ] Every record carries a monotonic sequence number that is gapless and strictly increasing within a run.
- [ ] Every record carries an informational wall-clock stamp and an authoritative monotonic offset from run start; durations are derivable from offsets alone and never depend on the wall clock.
- [ ] Every state transition in the C19 vocabulary is emitted as an event: run started, node became ready, node admitted, attempt started, attempt succeeded, attempt failed, node reached terminal state, zombie-at-exit (C14), run finished; terminal records name the normative terminal state from the vocabulary.
- [ ] The `run-started` event carries every run-artifact header field known at start (run identity, pipeline identity, both fingerprints when assembly succeeded, parameters, data interval, allowlisted captured environment, resume lineage when resumed) and omits overall outcome and summary; a stream ending immediately after it still identifies its run completely.
- [ ] For run verbs, identity is minted and the store and stream open before assembly executes, so an assembly failure still has a recorded place; inspection verbs (validate, graph, render) run assembly with no store/stream.
- [ ] "Flushed" is honored: each record is written to the sink before its transition is considered recorded (no user-space buffering), with an fsync delegated to the sink at run end and at cancellation; the default local-file sink does not fsync per event.
- [ ] A reader tolerates and discards at most one trailing partial record; every other record in a killed-at-any-moment stream is valid and parseable.
- [ ] Each run writes under its own `<base>/<pipeline>/<run-id>/` directory; two simultaneous runs of the same binary write disjoint files and both produce valid streams.
- [ ] Run identity is a UUIDv7 and is operator-overridable.
- [ ] A mid-run sink failure moves the run to cancelling with reason "event stream unwritable," makes a best-effort final report to stderr, and exits with the distinct sink-failure code.
- [ ] If opening the store itself fails, no stream is written; the error goes to stderr with the sink-failure exit code and nothing more is promised.
- [ ] The produced stream can be folded into a run artifact by a standalone function that needs no access to the original run (the C22/C26 fold contract), verified through a reader that consumes only the stream file.
- [ ] Record encoding and the `schema_version` field follow the T4 ADR (JSONL events); node identity in records follows T13.
- [ ] Public items carry rustdoc; the record envelope and event kinds are documented against the C19 vocabulary.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
None were stated in this ticket or in the `docs/tasks.md` T19 entry, and none
were discovered. The interface-presentation choices the governing decisions left
open (T4 fixed the encoding/schema-version, T0.6 fixed the sink/header/identity)
were resolved during implementation and are recorded here for the record:

- **Where the writer lives.** The `dagr-artifact` crate — its manifest already
  names it the home of "the event-record shapes derived into them (C19)", and it
  depends on no other workspace crate, so it stays the clean C24 boundary while
  gaining the writer. `dagr-core` stays dependency-free per its own manifest.
- **Dependencies added (to `dagr-artifact` only).** `serde_json` (the T4-named
  runtime JSON stack) and `uuid` (`v7` feature, for the UUIDv7 run identity of
  T0.6 §4). `serde` is not a direct dependency — the writer builds
  `serde_json::Value`s directly and applies the T4 §6 canonicalization itself
  (serde_json does not sort keys), so no derive is needed. `deny.toml` gains
  `Unicode-3.0` (required unconditionally by the `unicode-ident` build tool under
  `serde_derive`/`proc-macro2`); the license gate stays MIT-first, not weakened.
  `cargo deny check` and `cargo audit` both pass.
- **Record envelope shape.** `{schema_version, run_id, seq, wall, offset_ns,
  event, body}` — the five T0.6 §7 header fields, a kebab-case `event` kind name,
  and a per-kind `body` object. Emitted canonical per T4 §6 (keys sorted
  lexicographically, compact, integers only, minimal UTF-8 escaping), so two
  emissions of the same record are byte-identical.
- **Node identity in records.** The author-declared registration name string,
  verbatim (T13): node identity *is* the name, and `NodeId` is opaque with no
  route back to a name, so records carry the name a consumer can read.
- **Sequence start.** Gapless, strictly increasing, **starting at 0** on
  `run-started` (the "documented start value" the Test plan allows). A record is
  only counted after its append succeeds, so a faulted record leaves no gap.
- **Clock injection.** The authoritative monotonic offset comes from an injected
  `MonotonicClock` (its zero is the run-start instant); the informational wall
  stamp is an overridable `fn() -> u64` (default Unix ms). The wall stamp is a
  record's analog of an artifact's excluded generation-time field (T4 §6): held
  fixed, two emissions are byte-identical; it never feeds a duration.
- **Sink and per-directory contract.** The two-operation `EventSink` trait
  (append a line, flush) is defined here (T0.6 §1 named T19 as its owner); the
  default local-file sink is **injected**, not built here (owned by T0.6/C18).
  `stream_path(base)` yields `<base>/<pipeline>/<run-id>/events.jsonl`, disjoint
  by run id, so concurrent runs never share a file.
- **Sink-failure surface.** A sink append/flush error becomes a `SinkFault`
  carrying reason `"event stream unwritable"` (verbatim, arch.md C19 / T0.6 §5);
  the run loop (T24) reacts by cancelling and exiting with the sink-failure code.
  Driving the process exit and the store-open-failure path (no stream) are T24's
  and the run store's; this writer surfaces the fault and never opens a stream
  itself, so "store-open failure → no stream" holds by construction.
- **Tolerant reader.** `read_records(bytes)` parses each physical line; a
  terminated non-final line that fails to parse is a corruption (`ReadError`),
  and a single unterminated final line is the one tolerated trailing partial —
  the crash-safety (T27) and fold (C22/T42) reader contract, self-contained (it
  needs only the bytes).

## Out of scope
- The abrupt-process-kill crash-safety and I/O fault-injection test suite is **T27** — this ticket delivers only the unit-level reader/writer parseability and induced-sink-failure contracts those tests build on.
- Folding a stream into a run artifact is **C22 / T42**; this ticket produces a foldable stream and the tolerant reader contract, not the fold function or the run-artifact schema.
- OS-signal handling, the final flush wired into shutdown, and temp cleanup are **C16 / T36**; the attempt-execution records are populated by **C14 / T20**; the full run-loop that drives transitions is **T24**.
- The definitive run-store sink (durability guarantees, default local-file implementation) is owned by **T0.6 / C18** and is injected, not built here.
- The serialization encoding and schema-versioning policy are fixed by **T4** and consumed, not re-decided.
- No push-export of metrics from inside the process; live telemetry is done by an external tailer of the stream (explicitly a task/resource concern, not a framework feature) — do not add an exporter, a metadata store, a scheduler surface, or any runtime-mutable stream shape. The event vocabulary is closed and the graph shape never changes at runtime.
- Retention/pruning of old streams is the **C26** prune verb; nothing is deleted implicitly here.
