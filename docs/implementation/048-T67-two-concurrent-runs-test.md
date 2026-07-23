# 048 · T67 — Two-concurrent-runs test

> **Milestone:** M2 · **Size:** S · **Type:** feature (tests) · **Components:** C19
> **Branch:** `feat/t67-two-concurrent-runs-test` · **Depends on:** T24, T0.6 · **Blocks:** T38

## Why / context
The Operational model states that concurrent runs on a shared host are safe with respect to the run store because per-run directories never collide, and C19 requires that two simultaneous runs of the same binary write disjoint files and both produce valid, safely-partitionable streams. Nothing in the tool coordinates between processes — that is a deliberate scope boundary (it "ends in building a scheduler"), so the guarantee is purely a store-layout and per-record-identity property, not a locking or arbitration mechanism. This ticket adds the test that pins that guarantee: it builds on the M1 run-loop driver (T24), which mints run identity at bootstrap and opens the store and stream before assembly, and on the run-store contract (T0.6, `<base>/<pipeline>/<run-id>/`). It is a prerequisite for the M2 overcommit-and-clean-stop demo (T38), which runs real workloads and must not be undermined by cross-run file interference. Governing arch.md sections: `C19 · Event stream` and `Operational model`.

## Objective
Prove, with an automated test, that two simultaneous runs of one dagr binary on one machine coexist cleanly: disjoint run-store directories, no shared or colliding file, and two event streams that are each independently valid, gapless, and safely concatenable-and-partitionable by run identity.

Concrete pieces of work:
- Add an integration test that launches two runs of the same pipeline binary concurrently against a shared run-store base, with distinct run identities (both auto-minted UUIDv7 and the operator-overridden case).
- Assert directory disjointness: each run writes only under its own `<base>/<pipeline>/<run-id>/`, and no path is written by both runs.
- Assert per-stream validity: each stream parses fully, every record carries that run's identity and the schema version, and sequence numbers are gapless and strictly increasing within each run.
- Assert cross-run safety: the two streams concatenated and then partitioned by run identity reproduce each run's stream exactly, and neither stream contains a record bearing the other run's identity.
- Keep the test hermetic (temp run-store base, no reliance on wall-clock ordering, no external services) and deterministic enough to run in CI without flaking, while still exercising genuine simultaneity.

## Test plan (write these first — TDD)
- **Disjoint directories under a shared base.** Setup: a temporary run-store base and one pipeline binary. Action: start two runs concurrently against that same base and let both reach a terminal state. Expected: exactly two run directories exist under `<base>/<pipeline>/`, their run-id segments differ, and every file either run wrote lives strictly under its own run directory — the set of paths touched by run A and the set touched by run B are disjoint.
- **Each stream is independently valid and gapless.** Setup: the two completed runs from above. Action: parse each run's event stream in isolation. Expected: each stream parses with zero errors, contains a run-started and a run-finished record, and its sequence numbers form a gapless, strictly increasing series within that run.
- **Run identity on every record, no cross-contamination.** Setup: both parsed streams. Action: inspect the run-identity field of every record in each stream. Expected: every record in run A's stream carries run A's identity and the schema version; every record in run B's stream carries run B's identity; no record in either stream carries the other run's identity.
- **Concatenate-then-partition is lossless and safe.** Setup: the raw bytes of both streams. Action: concatenate the two streams into one buffer, parse it, and partition the records by run identity. Expected: partitioning yields exactly two groups matching the two run identities; each group, read in stored order, equals that run's original stream record-for-record; partitioning succeeds regardless of interleaving order because identity travels on every record.
- **No file collision under genuine simultaneity.** Setup: two runs started so their writing windows overlap in wall-clock time (both actively emitting events at the same moment). Action: run to completion and inspect the store. Expected: neither run's stream shows a truncated, interleaved, or foreign record; no lockfile contention, overwrite, or shared-handle error occurred; both runs report success and both artifacts are present at their predictable per-run locations.
- **Operator-overridden identities remain disjoint.** Setup: two runs launched with explicit, distinct operator-supplied run identities against the shared base. Action: run both concurrently to completion. Expected: directories are named by the supplied identities, remain disjoint, and both streams are valid and partition cleanly — confirming the disjointness property is a store-layout guarantee, not an artifact of UUIDv7 monotonicity.
- **Same pipeline, same base, no coordination assumed.** Setup: the pair of runs. Action: verify the test never relies on either process observing or waiting on the other. Expected: the test passes with no inter-process signalling, locking-for-arbitration, or ordering handshake between the runs — the guarantee holds because the store partitions by identity, not because the processes cooperate.

## Definition of done
- [ ] An automated integration test launches two simultaneous runs of one dagr binary against a shared run-store base and drives both to terminal states.
- [ ] The test asserts each run writes only under its own `<base>/<pipeline>/<run-id>/` directory and that the two runs' written-path sets are disjoint (C19: two simultaneous runs write disjoint files).
- [ ] The test asserts no file is shared, overwritten, or collided between the two runs.
- [ ] The test asserts every record in each stream carries that run's identity and the schema version (C19: every record carries run identity and schema version).
- [ ] The test asserts sequence numbers are gapless and strictly increasing within each run (C19: sequence numbers gapless and strictly increasing within a run).
- [ ] The test asserts both streams are individually valid and fully parseable, each containing run-started and run-finished records (C19: both produce valid streams).
- [ ] The test asserts the two streams concatenated and partitioned by run identity reproduce each run's stream exactly and that no record bears the other run's identity (C19: records from concurrent runs can be concatenated and partitioned safely).
- [ ] The test covers both auto-minted UUIDv7 identities and operator-overridden identities.
- [ ] The test relies on no inter-process coordination, locking-for-arbitration, or ordering handshake between the two runs (Operational model: the tool does not coordinate between processes).
- [ ] The test is hermetic (temporary run-store base, no external services) and stable under CI, exercising genuinely overlapping write windows rather than sequential runs.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
None in the ticket file, and `docs/tasks.md`'s T67 entry carries no `Q:` items.
Two implementation decisions were resolved during authoring and are recorded here
for traceability (neither moves a merged decision; both stay inside this ticket's
tests-only scope):

- **How the two runs are launched concurrently and made to overlap
  deterministically.** `drive()` is a synchronous call that builds its own
  isolated framework + task runtimes internally, so "concurrent" is realised by
  spawning each `drive()` on its own `std::thread`, each with its own injected
  `MemorySink` + `TickClock` and its own `RunConfig`. Genuine simultaneity (an
  overlapping *write* window, not two sequential runs) is forced by a shared
  `std::sync::Barrier` of width 2: a `gate` source task in each run rendezvouses
  at it and blocks until both runs have arrived, so both are provably mid-run and
  emitting attempt events at the same instant. This is synchronised on an
  observable signal, never a sleep, and asserts nothing about which run wins —
  avoiding the T35-style ordering-race flake — because partition-by-identity is
  order-independent by design.
- **Where directory disjointness is observed.** No production `LocalFileSink`
  has shipped yet (the injected `EventSink` is the T0.6 §1 seam), so stream
  *content* disjointness is asserted over each run's own in-memory sink bytes,
  while on-disk *directory* disjointness is asserted against the real
  `<base>/<pipeline>/<run-id>/` directories that `drive()` itself creates at
  bootstrap (the per-run temp dir), enumerated after both runs complete. The two
  are complementary halves of the C19 "disjoint files" guarantee.

## Out of scope
- Any inter-process coordination, arbitration, cross-run locking, or capacity-sharing between simultaneous runs — the tool deliberately does not coordinate between processes (that "ends in building a scheduler"); pool-pinning to split machine capacity across runs is the operator's call via the C12 flag and belongs to admission-control tickets (T31/T32), not here.
- Crash-safety, abrupt-kill, disk-full, and failing-sink fault injection on the stream — those are T27 (C19 crash-safety and I/O fault-injection).
- Bounded-memory and overcommit behaviour of two memory-hungry runs sharing a host — the M2 overcommit demo T38 and the admission tickets own that; this ticket only proves store-layout disjointness, not resource jointness.
- The event-stream writer implementation itself (T19/C19) and the run-loop driver (T24) — this ticket consumes them and adds tests only; no writer or driver behaviour is defined or changed here.
- Run-artifact folding, resume lineage, and cross-run analysis tooling (C22/C26) beyond the minimal concatenate-and-partition assertion needed to prove safety.
- Windows behaviour — explicitly unsupported in v1; the test targets the Tier-1 Linux and dev-supported macOS platforms only.
