# 098 · T83 — `dagr-metastore` crate: schema + libSQL connection seam

> **Milestone:** M7 · **Size:** M · **Type:** feature · **Components:** system-level (new `dagr-metastore`)
> **Branch:** `feat/t83-dagr-metastore-crate-and-schema` · **Depends on:** T82 · **Blocks:** T84, T85, T86

## Why / context

ADR 097 (T82) decided a lightweight, embedded, opt-in run index on **libSQL** (the `libsql` fork crate), derived from the event stream. This ticket lays the foundation the rest of M7 builds on: a new **`dagr-metastore`** crate, its schema, and a connection seam that encodes the ADR's concurrency recipe — **before** any reader (T84 reconcile) or writer (T86 live tee) exists, so the store, schema, and write discipline are built and tested in isolation.

Two structural rules from the ADR are load-bearing here. First, `dagr-core` stays runtime-dependency-free: the crate's **only** workspace edge is onto `dagr-artifact` (for the event/artifact types it maps and, later, `fold_stream`), mirroring how `dagr-render` depends on `dagr-artifact` and provably cannot reach `dagr-core` (C24). Second, `libsql` is heavy and pre-1.0, so it is gated behind a **default-off `metastore` cargo feature** — the same pattern as the default-on `dag` feature added in T79 (`crates/cli/Cargo.toml`), inverted to off — so a plain `cargo build` never pulls `libsql` and the zero-dep-core guarantee is untouched (`cargo build --all --no-default-features` proves it).

The connection seam must encode the verified libSQL concurrency footguns from ADR 097: WAL is single-writer, and a `DEFERRED` read-txn that later upgrades to a write hits an **instant `SQLITE_BUSY` that `busy_timeout` will not retry** — so every write transaction opens with `BEGIN IMMEDIATE`, and each is wrapped in an app-level bounded retry.

## Objective

Deliver the crate, schema, and a write-safe connection seam.

- Add a **`dagr-metastore`** workspace crate depending only on `dagr-artifact` + `libsql` (pinned exact, per ADR 097) + `tokio`; add it to the root workspace and to `crates/cli/Cargo.toml` behind a **default-off `metastore` feature** (`metastore = ["dep:dagr-metastore"]`); gate all wiring behind `#[cfg(feature = "metastore")]`.
- Define the schema as ordered, idempotent migrations (`CREATE TABLE IF NOT EXISTS …`) for the M7 tables: `dag`, `dag_version`, `dag_run`, `node_attempt`, `node_terminal`. Use `INTEGER` unix-ms timestamps, `TEXT`+`CHECK` for state enums (the run-level 6 and dagr's **9 terminal states**, canonical names per arch.md Vocabulary), `TEXT` JSON blobs, and indexes on `dag_id`, `run_id`, and `state`. Keys per the plan: `dag_run(run_id PK)`, `node_attempt UNIQUE(run_id, node_id, try_number)`.
- Provide a `MetaStore` seam: `MetaStore::open(mode)` with `mode` ∈ `{ LocalFile(path), … }` (only `LocalFile` this ticket; `RemoteSqld`/`SyncedReplica` reserved as recognized stubs per ticket-conventions §7), converging on one `libsql::Connection`. On open it sets `PRAGMA journal_mode=WAL`, `synchronous=NORMAL`, and a `busy_timeout`, and runs migrations.
- Provide the write discipline as one place: a `with_write_txn` helper that opens `BEGIN IMMEDIATE`, runs the caller's statements, `COMMIT`s, and on `SQLITE_BUSY`/`Busy` rolls back and retries the whole closure with **bounded** exponential backoff + jitter, surfacing a hard error after the cap.
- Add the `dagr metastore init [--store <path>]` CLI verb (behind the feature) that creates/opens the DB and applies migrations, exiting 0 on success.

## Test plan (write these first — TDD)

**Schema + migrations**
- Given a fresh temp path, when `MetaStore::open(LocalFile(path))` runs, then all five tables exist (queryable via `sqlite_master`), the file is a valid SQLite/libSQL file, and `journal_mode` reports `wal`.
- Given an already-initialized store, when `open` runs again, then migrations are idempotent (no error, no duplicate objects) and existing rows survive.
- Given a `dag_run` insert with a `state` outside the allowed set, then the `CHECK` constraint rejects it; given each of the 9 canonical `node_attempt` states, then all are accepted.

**Write discipline**
- Given `with_write_txn`, when two writers on the same file contend, then the loser retries under `BEGIN IMMEDIATE` + `busy_timeout` and both commits eventually succeed (no lost write); when contention exceeds the retry cap, then a hard error is returned (not a silent drop). (Deep multi-process coverage is T85; this asserts the helper's shape.)

**Feature gating**
- Given `cargo build -p dagr-cli --no-default-features` (and default `cargo build --all`), then there is **no** `libsql` or `dagr-metastore` edge and the `metastore` verb is absent; given `--features metastore`, they are present.
- Given `cargo build --all --no-default-features`, then `dagr-core`'s dependency set is unchanged (zero runtime deps).

**CLI**
- Given `dagr metastore init --store <tmp>`, when it runs, then the DB is created with all tables and it exits 0; a second `init` is a no-op success.

## Definition of done

- [ ] `dagr-metastore` crate exists, depends only on `dagr-artifact` + `libsql` (pinned exact) + `tokio`, and has no path to `dagr-core`.
- [ ] `crates/cli` gains a **default-off** `metastore` feature; `cargo build --all` and `cargo build -p dagr-cli --no-default-features` pull neither `libsql` nor `dagr-metastore`; `--features metastore` pulls both.
- [ ] `cargo build --all --no-default-features` shows `dagr-core` with an unchanged, empty runtime dependency set.
- [ ] Migrations create `dag`, `dag_version`, `dag_run`, `node_attempt`, `node_terminal` with the specified keys, indexes, and `CHECK` enums (canonical 9 terminal-state names), and are idempotent.
- [ ] `MetaStore::open(LocalFile)` sets WAL + `synchronous=NORMAL` + `busy_timeout` and applies migrations; `RemoteSqld`/`SyncedReplica` are recognized stubs (documented, not implemented).
- [ ] `with_write_txn` uses `BEGIN IMMEDIATE` + bounded `SQLITE_BUSY` retry (backoff + jitter) and returns a hard error past the cap.
- [ ] `dagr metastore init` creates/opens the store and exits 0; re-running is a no-op success.
- [ ] `cargo deny` / `cargo audit` are green with `libsql` in the tree, or any `deny.toml` change is recorded with justification.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
None. (Substrate, concurrency recipe, and boundaries are fixed by ADR 097; this ticket implements them. Any `deny.toml` license/advisory adjustment for `libsql` is recorded in-PR per §5.)

## Out of scope
- Event→row mapping and the `sync` reconcile command — **T84**.
- The live tee sink that writes during a run — **T86**.
- Multi-process write stress/validation — **T85** (this ticket asserts only the helper's single-file shape).
- `RemoteSqld` / `SyncedReplica` implementations and any `sqld` server — future work behind the seam (recognized stubs only here).
- Lineage columns/tables (`durable_reference_meta`, `output_produced`, `input_consumed`) — **M8**.
- Scope boundary restated: the store is a local, embedded, non-coordinating index; dagr remains not a scheduler, distributed system, coordinating metadata store, web interface, DSL, or backfill orchestrator.
