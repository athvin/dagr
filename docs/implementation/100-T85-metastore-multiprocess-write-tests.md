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

## Out of scope
- The live tee sink itself — **T86** (this ticket exercises the store's write path directly, not via the driver).
- Cross-**host** access / `sqld` / embedded replicas — out of scope per ADR 097 (same-host local FS only).
- Any MVCC / `BEGIN CONCURRENT` path — that is the `turso` rewrite, rejected for M7.
- Scope boundary restated: proving concurrent local writes adds no coordination; dagr remains not a scheduler, distributed system, coordinating metadata store, web interface, DSL, or backfill orchestrator.
