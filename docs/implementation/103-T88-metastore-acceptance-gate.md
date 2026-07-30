# 103 · T88 — M7 metastore acceptance gate

> **Milestone:** M7 · **Size:** M · **Type:** feature (gate) · **Components:** system-level
> **Branch:** `feat/t88-metastore-acceptance-gate` · **Depends on:** T85, T86, T87 · **Blocks:** T89

## Why / context

M7's done-when is "one binary hosting many DAGs writes and serves a single, queryable, guaranteed run index — without touching the zero-dep core or crossing the coordination boundary." This gate executes that end to end and asserts the milestone's invariants in one place, exactly as T65 does for the whole product and T28/T38/T49/T63 do for their milestones. Like every gate ticket, it **adds zero capability** — any missing behavior belongs to its owning ticket (T83–T87), which means STOP, not scope creep.

## Objective

Assert the M7 milestone end to end and lock its structural invariants.

- **End-to-end path.** From a many-DAGs binary: `metastore init`; run several DAGs with the live toggle on (multi-process) so rows land live; then `sync` the run store and assert the store is complete and consistent; then query it natively and assert cross-run results.
- **Guarantee invariant.** Re-assert (composed from T86) that a durable metastore-write failure surfaces as a `SinkFault` in the run's exit code — the write is guaranteed, not best-effort.
- **Parity invariant.** Assert live-produced rows equal reconcile-produced rows for the same runs (from T84/T86).
- **Boundary invariants (structural, CI-enforced).** `cargo build --all` (default features) pulls **no** `libsql`/`dagr-metastore`; `cargo build --all --no-default-features` shows `dagr-core` with an unchanged empty runtime dependency set; the `dagr-metastore` crate has no dependency path to `dagr-core`; the diff since M6 adds no scheduler/server/pgwire surface.
- Wire the gate into CI (feature-on job) alongside the existing acceptance jobs; it must run green on its own PR.

## Test plan (write these first — TDD)

**End-to-end**
- Given a many-DAGs binary built with `--features metastore`, when `init` runs, several DAGs run live (as separate processes) against one store, and `sync <base>` then runs, then the store holds one `dag` row per DAG and complete `dag_run`/`node_attempt`/`node_terminal` rows, and a native (`sqlite3`) query returns the expected cross-run aggregate.

**Invariants**
- Given an injected metastore-write failure during a live run, then the run's exit code reflects a `SinkFault` (guaranteed-write invariant holds).
- Given the same runs executed live vs. reconciled from the streams, then their rows are identical (parity invariant holds).

**Boundary (structural)**
- Given `cargo build --all` (default) and `cargo build -p dagr-cli --no-default-features`, then neither pulls `libsql`/`dagr-metastore`; given `cargo build --all --no-default-features`, then `dagr-core`'s runtime dependency set is empty/unchanged.
- Given a crate-graph check, then `dagr-metastore` has no path to `dagr-core` (artifact-only edge), and the M6→M7 diff introduces no server/scheduler/pgwire surface.

## Definition of done

- [ ] An end-to-end test drives `init` → live multi-process runs → `sync` → native query and asserts a complete, consistent, queryable store for a many-DAGs binary.
- [ ] The guaranteed-write invariant (metastore `SinkFault` ⇒ run exit code) is asserted.
- [ ] The live==reconcile parity invariant is asserted.
- [ ] Structural boundary checks pass: default + `--no-default-features` builds pull no `libsql`/`dagr-metastore`; `dagr-core` runtime deps unchanged; `dagr-metastore` has no edge to `dagr-core`; no new scheduler/server/pgwire surface.
- [ ] The gate is wired into CI (feature-on job) and runs green on both `ubuntu-latest` and `macos-latest`.
- [ ] The gate adds no product capability (assertions only).
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
None. (A gate asserts already-merged behavior; any gap it surfaces is a STOP pointing at the owning ticket, not new scope here.)

## Out of scope
- Any capability not already delivered by T83–T87 (a gate adds none).
- Lineage (M8) — this gate covers M7's run/attempt index only.
- Server/remote/replica assertions — those paths are unshipped (behind the seam).
- Scope boundary restated: the gate proves a local, embedded, non-coordinating index; dagr remains not a scheduler, distributed execution system, coordinating metadata store, web interface, DSL, or backfill orchestrator.
