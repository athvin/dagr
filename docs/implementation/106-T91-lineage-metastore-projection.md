# 106 · T91 — lineage projection into the metastore (+ optional asset identity)

> **Milestone:** M8 · **Size:** M · **Type:** feature · **Components:** C22, system-level
> **Branch:** `feat/t91-lineage-metastore-projection` · **Depends on:** T90, T84, T86 · **Blocks:** —

## Why / context

T89/T90 put lineage into the event stream and fold; this ticket makes it **queryable in the one place** the operator asked for by projecting it into the metastore — closing the M8 loop. It reuses both projection paths already built: the reconcile mapping (T84) and the live tee (T86), so lineage lands the same way run/attempt rows do, live and on `sync`. It also adds the optional joinable endpoint — an `asset` identity row — but only in the trimmed, FK-free form the Airflow review recommended, so cross-run "which runs touched dataset X" is answerable without importing Airflow's asset-scheduler machinery.

## Objective

Project lineage into the metastore and add the optional asset endpoint.

- Extend the T84 mapping module: fold `outputs[]` → an **`output_produced`** table (`{ id, run_id, node_id, attempt, uri, content_hash, size_bytes, kind, produced_at_offset_ns, originating_run }`, append-only, **no FK** to any asset row) and consumed `inputs[]` → an **`input_consumed`** table (`{ id, run_id, node_id, attempt, uri, content_hash }`); add `durable_reference_meta` columns (`content_hash`, `size_bytes`, `scheme`, `produced_at_offset_ns`) to `node_attempt` from T89. Add these as forward migrations in `dagr-metastore` (existing stores upgrade in place).
- Wire projection through **both** paths: reconcile (`sync` maps the new fold outputs) and the live tee (`MetastoreSink` maps the new events), so lineage rows appear live and on backfill, with the same guaranteed-write + idempotent-UPSERT discipline.
- Add an optional **`asset`** identity table (`uri TEXT PK`, `extra TEXT` JSON) referenced **by value** (the `uri` string) from `output_produced`/`input_consumed` — never a hard FK (preserve the survives-GC property). Populate `asset` on first sight of a `uri`; keep it optional (feature/config), since the produced/consumed records already answer "what did this run produce/consume."
- Index for the cross-run queries (`uri`, `content_hash`, `run_id`) and document the join in the cookbook.

## Test plan (write these first — TDD)

**Projection (reconcile + live)**
- Given a run with produced/consumed lineage, when `sync` folds it, then `output_produced` / `input_consumed` rows and `node_attempt.durable_reference_meta` columns match `outputs[]`/`inputs[]`/`AttemptRecord`; re-sync is idempotent (no dupes).
- Given the same run executed with the live tee, then the same lineage rows appear live, and live == reconcile for lineage too.

**Cross-run query + no-FK discipline**
- Given two runs producing outputs at the same `uri` with different content hashes, when queried by `uri`, then both runs' `output_produced` rows join to the single `asset` row **by value**, and deleting/absent `asset` rows does not orphan or break `output_produced` (no FK).

**Migration + additivity**
- Given a metastore created at T83/M7, when opened after T91, then forward migrations add the lineage tables/columns without disturbing existing rows; a store with no lineage data has empty lineage tables and behaves as before.

## Definition of done

- [ ] `output_produced` and `input_consumed` tables and `node_attempt.durable_reference_meta` columns exist via forward migrations; existing stores upgrade in place.
- [ ] Both reconcile (`sync`) and the live tee project `outputs[]`/`inputs[]`/reference-metadata into these rows, guaranteed and idempotent; live == reconcile for lineage.
- [ ] `output_produced`/`input_consumed` reference `uri` **by value** with no hard FK; an optional `asset(uri PK, extra)` identity row joins by value and is populated on first sight; absent `asset` rows never orphan lineage.
- [ ] Indexes support cross-run `uri`/`content_hash`/`run_id` queries; the cookbook documents a "which runs produced/consumed dataset X" query.
- [ ] Migration/additivity tests prove M7 stores upgrade cleanly and no-lineage runs behave as before.
- [ ] Tests pass on `ubuntu-latest` and `macos-latest`.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
None. (The append-only + by-value-no-FK disciplines are fixed by T90 and ADR 097's lineage note; the `asset` table is optional and identity-only.)

## Out of scope
- Any asset-**scheduler** behavior — data-triggered scheduling, asset queues, partitions, watchers (Airflow's asset scheduler cluster) — permanently out of scope (no scheduler).
- Deadline/expected-runtime and `#[dag]`-discovery-failure records (the LOW-priority review items) — deferred; open a follow-up ticket only on explicit demand.
- Server/remote projection — behind the `MetaStore::open(mode)` seam, unshipped.
- Scope boundary restated: lineage projection is a local, non-coordinating index of per-run provenance; dagr remains not a scheduler, distributed execution system, coordinating metadata store, web interface, DSL, or backfill orchestrator.
