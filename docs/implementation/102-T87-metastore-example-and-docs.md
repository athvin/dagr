# 102 · T87 — native access, many-dags metastore example, and cookbook

> **Milestone:** M7 · **Size:** M · **Type:** feature (docs) · **Components:** C26, system-level
> **Branch:** `feat/t87-metastore-example-and-docs` · **Depends on:** T86, T81 · **Blocks:** T88

## Why / context

The store, reconcile, and live tee exist (T83–T86); this ticket makes the feature **usable and discoverable**, and proves the headline story end to end: *many DAGs in one binary, one queryable place for their state*. ADR 097 fixed **native access only** — no Postgres wire protocol — and libSQL's payoff is that the file is byte-compatible with stock SQLite, so the lowest-friction query path (`sqlite3 metastore.db "SELECT …"`) needs **zero new tools**, with the `turso` / `libsql` CLIs as alternatives. It builds directly on T81's many-dags example (`#[dag]` + inventory) by turning the metastore on.

Per ticket-conventions §4 (docs) and the gate discipline, this ticket adds **no capability** and its claims must not exceed shipped behavior.

## Objective

Ship the example and the documentation for querying cross-run state.

- Extend the T81 many-dags example so it can be built/run with `--features metastore`: running several of its DAGs (concurrently, as separate processes) populates one `metastore.db`, demonstrating the multi-process live-write path from T85/T86.
- Add a cookbook section ("Querying run state across DAGs") that shows: turning the metastore on via the `DAGR_*` toggle; running a few DAGs; then querying with plain `sqlite3` (and noting `turso db shell <file>` / the `libsql` CLI as equivalents) — e.g. runs per DAG by state, slowest nodes, latest terminal state per node. No pgwire, no server.
- Document the `dagr metastore init` / `sync [--follow]` verbs and the toggle in the CLI/reference docs, including the same-host local-FS constraint and the "guaranteed live + reconcile backfill" model.
- Keep every claim within shipped behavior (docs discipline): no lineage/asset claims (that is M8), no server/remote claims (behind the seam, not shipped).

## Test plan (write these first — TDD)

**Example builds and populates**
- Given the many-dags example built with `--features metastore`, when several of its DAGs run against one store path, then `metastore.db` contains one `dag` row per distinct DAG and the expected `dag_run` / `node_attempt` rows, queryable with plain `sqlite3`.
- Given the example built **without** the feature (default), then it still builds and runs unchanged (no `libsql`), proving the feature is additive.

**Docs are executable and truthful**
- Given the cookbook's copy-paste query block run against a populated `metastore.db`, when executed with `sqlite3`, then each query returns without error and matches the described shape (the doc's commands actually run).
- Given a docs-claims check, then no claim references unshipped behavior (no pgwire, no server, no lineage/asset tables).

## Definition of done

- [ ] The many-dags example builds and runs both with and without `--features metastore`; with the feature, running several DAGs populates one queryable `metastore.db` (multi-process live writes).
- [ ] A cookbook section shows enabling the toggle, running DAGs, and querying cross-run state with plain `sqlite3` (with `turso`/`libsql` CLI noted as alternatives); it explicitly states native-access-only (no pgwire) and same-host local FS.
- [ ] `dagr metastore init` / `sync [--follow]` and the toggle are documented in the reference/CLI docs with the guaranteed-live + reconcile model.
- [ ] The doc's query block executes cleanly against a populated store (verified), and a claims check confirms nothing exceeds shipped behavior (no server/remote/lineage claims).
- [ ] Example + docs checks pass on `ubuntu-latest` and `macos-latest`.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
None. (Native-access-only is decided in ADR 097; this ticket documents the shipped verbs and the SQLite-compatible query path.)

## Out of scope
- The end-to-end acceptance gate — **T88**.
- A `turso`-CLI or `sqld` **server** setup guide — future work behind the seam; this ticket documents embedded local access only.
- Any Postgres-wire/BI-tool guidance — rejected by ADR 097.
- Lineage/asset queries — **M8**.
- Scope boundary restated: querying a local index is non-coordinating; dagr remains not a scheduler, distributed execution system, coordinating metadata store, web interface, DSL, or backfill orchestrator.
