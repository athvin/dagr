# 100 · T85 — multi-process write validation + concurrency hardening

> **Milestone:** M7 · **Size:** M · **Type:** feature (tests) · **Components:** C19, system-level
> **Branch:** `feat/t85-metastore-multiprocess-write-tests` · **Depends on:** T84, T67 · **Blocks:** T88

## Why / context

ADR 097 bets the whole live-write design on one verified-but-worth-proving claim: **many OS processes can write to one embedded libSQL file** (SQLite WAL, single-writer-at-a-time), and dagr's write discipline (`busy_timeout` + `BEGIN IMMEDIATE` + bounded `SQLITE_BUSY` retry, from T83) makes that safe when the rows are disjoint. dagr is one-run-per-process, so "many DAGs at once" means many processes hammering one file — exactly the case that fails if the discipline is wrong. Earlier research flagged `busy_timeout` reliability in the binding as uncertain, so this ticket makes the guarantee a **test**, not a hope. It mirrors T67 (two-concurrent-runs), which proved run-store isolation across processes; here we prove metastore-write correctness across processes.

The deliverable is tests (and any hardening they force in T83's helper), adding **zero product capability**.

## Objective

Prove and harden concurrent multi-process writes to one metastore file.

- Add a test harness that spawns **N real OS processes** (a small test binary or `cargo` example, not just threads — the claim is multi-*process*), each opening the same `LocalFile` store and writing **disjoint** rows (distinct `run_id`s and `(run_id, node_id, try_number)`s) through `with_write_txn`, under deliberate contention (barrier-start, overlapping windows).
- Assert **zero lost writes** (every intended row present, exactly once), **no corruption** (the DB opens and passes an integrity check afterward), and that `SQLITE_BUSY` was **retried within the bound** (never surfaced as a spurious hard error for disjoint rows).
- Assert the **`BEGIN IMMEDIATE` rule**: add a targeted test that a write path which begins `DEFERRED` and upgrades hits the instant-`SQLITE_BUSY` footgun, demonstrating why the helper must use `BEGIN IMMEDIATE` (guards against regression).
- Assert the **retry cap** surfaces a hard, visible error under pathological over-contention (a wedged writer is observable, not silently dropped).
- If any assertion fails on the pinned `libsql` version, harden T83's connection/retry seam (e.g. timeout value, backoff schedule, connection-per-process discipline) until green — recording what changed and why.
- Run the harness on both `ubuntu-latest` and `macos-latest` (POSIX byte-range locks differ; ADR 097 requires same-host local FS).

## Test plan (write these first — TDD)

**Multi-process correctness**
- Given N processes each writing M disjoint runs to one file under a start barrier, when all finish, then all N×M `dag_run` rows and their `node_attempt` rows are present exactly once and an integrity check passes.
- Given the same under heavy overlap, when contention triggers `SQLITE_BUSY`, then it is retried and every write still lands (no lost update, no partial run).

**Discipline guards**
- Given a `DEFERRED`-then-upgrade write, then it reproduces the instant-`SQLITE_BUSY` failure `busy_timeout` cannot cure — proving `BEGIN IMMEDIATE` is required (this test passes by asserting the failure mode, and the production path avoids it).
- Given contention beyond the retry cap, then `with_write_txn` returns a hard error naming the exhausted retry (visible wedge, not a silent drop).

**Platform**
- The harness passes on `ubuntu-latest` and `macos-latest`.

## Definition of done

- [ ] A multi-**process** harness writes disjoint rows to one `LocalFile` store under contention and asserts zero lost writes, exactly-once rows, and a passing post-run integrity check.
- [ ] `SQLITE_BUSY` under disjoint-row contention is shown to be retried within bound and never surfaces as a spurious hard error.
- [ ] A guard test demonstrates the `DEFERRED`-upgrade instant-`SQLITE_BUSY` footgun, pinning the `BEGIN IMMEDIATE` requirement against regression.
- [ ] Over-contention past the retry cap returns a hard, visible error (no silent drop).
- [ ] Any hardening to the T83 seam that the tests forced is recorded with rationale.
- [ ] The harness passes on `ubuntu-latest` and `macos-latest`.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
None. (This ticket exists to answer the empirical `busy_timeout`-reliability question ADR 097 flagged; the answer is recorded in-PR and, if it forces a seam change, in T83's code + this ticket per §5.)

## Hardening record (resolved)

The multi-process harness surfaced two real failures against the merged T83 seam on the pinned `libsql =0.10.0-pre.4`, and forced two hardening changes to `crates/metastore/src/store.rs` (both in this ticket's Objective per §5; both recorded in-code):

1. **Concurrent open of a fresh file races on `PRAGMA journal_mode=WAL`.** With N real processes opening the *same brand-new* store simultaneously, a losing process's WAL-mode switch (which briefly takes an exclusive lock to write the WAL header) returned `SqliteFailure(5, "database is locked")`, and `busy_timeout` alone did not reliably absorb it — so `MetaStore::open` could hard-fail under a genuine multi-process open race (never seen with in-process *threads*, which libSQL's in-process locking coordinates — exactly why T85 needs real processes, not T67-style threads). **Fix:** the open-path pragmas now run under the same bounded `SQLITE_BUSY` retry (`is_busy` classification + jittered backoff) as `with_write_txn`, via `open_pragma_with_retry`. `set_pragma` now also drops its `Rows` handle so the row-returning WAL pragma is fully finalized before the connection is reused.

2. **A long `busy_timeout` made the retry cap unreachable (wedge not observable).** With `busy_timeout = 5 s`, a single `BEGIN IMMEDIATE` attempt blocked for seconds inside the kernel spin, so the app-level bounded retry never fired for real contention and a genuinely wedged writer could not surface a hard error within a bounded time. **Fix:** `busy_timeout` was lowered to **250 ms** so the *app-level* jittered/decorrelated retry (ADR 097 §3) is the primary contention absorber and the retry cap is reachable — the over-contention test now observes a `WriteError::BusyRetriesExhausted` (visible wedge, dedicated worker exit code), never a silent drop, while disjoint-row writers still lose nothing.

Everything else in the T83 seam (the `BEGIN IMMEDIATE`-for-every-write discipline, the `DEFERRED`-upgrade footgun avoidance) was **verified correct** by the guard tests and left unchanged.

3. **Harness determinism on `macos-latest` (not a store bug).** The over-contention wedge test released the contending writer once the store's `-wal` sidecar existed plus a fixed 150 ms grace. That sidecar is created at open/WAL-mode time — well *before* any `BEGIN IMMEDIATE` takes the writer, and it already exists from the test's pre-created schema — so on macOS's slower process spawn the holder had not yet acquired the writer when the wedger was released; the wedger occasionally won the writer, committed, and exited `0` instead of the expected BUSY-exhausted code `3`. This was a **rendezvous race in the test harness, not a cross-process store behaviour** (the store's retry-cap escape hatch is correct — it just wasn't being contended). **Fix (test-support only, no product/store change):** `write_worker` gained an optional `--acquired-marker` flag that creates an observable file the instant its transaction *provably* owns the writer (after `BEGIN IMMEDIATE` + the INSERT, before the hold); the harness now waits on that marker before releasing the wedger, so the wedge is deterministic regardless of spawn timing. No correctness assertion was weakened — the wedger still writes a disjoint row under a 2-attempt cap and must still surface a hard `WriteError::BusyRetriesExhausted` (exit `3`) against a genuinely-held writer.

## Out of scope
- The live tee sink itself — **T86** (this ticket exercises the store's write path directly, not via the driver).
- Cross-**host** access / `sqld` / embedded replicas — out of scope per ADR 097 (same-host local FS only).
- Any MVCC / `BEGIN CONCURRENT` path — that is the `turso` rewrite, rejected for M7.
- Scope boundary restated: proving concurrent local writes adds no coordination; dagr remains not a scheduler, distributed execution system, coordinating metadata store, web interface, DSL, or backfill orchestrator.
