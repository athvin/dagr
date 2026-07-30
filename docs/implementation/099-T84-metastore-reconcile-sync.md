# 099 · T84 — event→row mapping + `dagr metastore sync` (reconcile)

> **Milestone:** M7 · **Size:** M · **Type:** feature · **Components:** C19, C22
> **Branch:** `feat/t84-metastore-reconcile-sync` · **Depends on:** T83, T42 · **Blocks:** T85, T86

## Why / context

T83 gave us the store, schema, and write discipline. This ticket makes it **useful from existing runs with zero core changes**: a mapping from dagr's structured run record to rows, and a `sync` command that folds every run under a run store into the DB idempotently. The event stream stays the source of truth (ADR 097); this is the derived, guaranteed projection.

The mapping reuses what already exists — no new run-time capture. `fold_stream()` (C22, `crates/artifact/src/fold.rs`, T42) already turns a run's `events.jsonl` into a structured `RunArtifact` with a header and one `AttemptRecord` per attempt; T84 maps that structure to rows. Because `fold_stream` is deterministic and the writes are UPSERTs keyed by `run_id` / `(run_id, node_id, try_number)`, re-syncing a run is a no-op — which is exactly what makes `sync` safe to run repeatedly and safe as a backfill/repair path for runs that predate the metastore or came from another machine. This mapping module is the shared seam T86's live tee sink will reuse, so it must not depend on the driver or a live run — only on `dagr-artifact` types.

## Objective

Deliver the mapping and the reconcile command.

- Add a mapping module in `dagr-metastore` (feature-gated) that turns a `RunArtifact` (and its `RunStartedHeader`) into row upserts: `dag` / `dag_version` (from pipeline identity + `structure_fingerprint` in the run-started header + the graph artifact when present), `dag_run` (identity, `state` from `overall_outcome`, timing, `interrupted`, `resumed_from`, params/env JSON, `events_path`), one `node_attempt` per `AttemptRecord` (state, `try_number`, timing + phase durations, worker/message/error/metrics/cost JSON, `durable_reference`, `satisfied_from_run`, `originating_node`), and `node_terminal` (latest terminal state per node). Map `overall_outcome` and each attempt `status` to the canonical enums; `assembly-failed`/`bootstrap-failed` runs and crash-truncated (`interrupted`) runs map faithfully.
- All writes go through T83's `with_write_txn` (one `BEGIN IMMEDIATE` transaction per run) and are **UPSERTs** (`INSERT … ON CONFLICT … DO UPDATE`) keyed idempotently.
- Add `dagr metastore sync [--store <db>] <run-store-base>` (feature-gated): walk `<base>/<pipeline>/<run-id>/` per the T0.6 run-store layout (`create_in_store` convention), fold each `events.jsonl` via `fold_stream`, and upsert; skip directories without a readable stream (report a count), never abort the batch on one bad run.
- Add `--follow` to `sync`: after the initial pass, tail for new/appended runs and upsert incrementally (single-writer consolidator), until interrupted.

## Test plan (write these first — TDD)

**Mapping fidelity**
- Given a fixture `events.jsonl` for a multi-node run with a retry, when `sync` folds and upserts it, then `dag_run` state/timing and every `node_attempt` (one row per attempt, correct `try_number`, phase durations, `durable_reference`, `satisfied_from_run`) match the `RunArtifact` produced by `fold_stream` on the same stream, and `node_terminal` holds the latest state per node.
- Given a crash-truncated stream (no `run-finished`), then `dag_run.interrupted = 1` and partial attempts are recorded without error; given an `assembly-failed` / `bootstrap-failed` run, then `dag_run.state` reflects it.

**Idempotency**
- Given a run already synced, when `sync` runs again, then no duplicate rows are created and row counts are unchanged (deterministic fold + UPSERT).

**Batch robustness**
- Given a run store with three runs where one directory has an unreadable/absent stream, when `sync <base>` runs, then the two good runs are upserted, the bad one is reported and skipped, and the command exits 0 with a summary count.

**Follow**
- Given `sync --follow` running against a base, when a new run directory appears (or an existing stream is appended to and finalized), then its rows appear without re-processing already-synced runs.

## Definition of done

- [ ] A feature-gated mapping module converts `RunArtifact` + `RunStartedHeader` into `dag` / `dag_version` / `dag_run` / `node_attempt` / `node_terminal` upserts, depending only on `dagr-artifact` types (no driver, no live run).
- [ ] `overall_outcome` and attempt `status` map to the canonical run/terminal enums; `interrupted`, `resumed_from`, `satisfied_from_run`, `originating_node`, metrics/cost/error JSON, and `durable_reference` are all carried.
- [ ] `dagr metastore sync <base>` folds each run via `fold_stream` and UPSERTs through `with_write_txn`; re-syncing is idempotent (no dupes, stable counts).
- [ ] A batch with one unreadable run skips + reports it and still exits 0 having synced the rest.
- [ ] `sync --follow` incrementally consolidates new/finalized runs without reprocessing synced ones.
- [ ] Mapping/sync tests pass on `ubuntu-latest` and `macos-latest`.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
None. (`fold_stream` is the folding authority per T42/C22; this ticket maps its output. Any run-store-walk edge cases are resolved against the T0.6 layout and recorded in-PR per §5.)

## Out of scope
- Writing rows **during** a run (the live tee sink) — **T86**; this ticket reads finished/observed streams only.
- Multi-process concurrent-write validation — **T85**.
- The `--features metastore` example and native-access docs — **T87**.
- Lineage rows (`output_produced` / `input_consumed`) and `durable_reference_meta` — **M8**.
- Scope boundary restated: `sync` reads the local run store and writes a local index; it coordinates nothing and adds no scheduler/backfill behavior. dagr remains not a scheduler, distributed execution system, coordinating metadata store, web interface, DSL, or backfill orchestrator.
