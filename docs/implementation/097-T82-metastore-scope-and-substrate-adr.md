# 097 · T82 — ADR: metastore scope carve-out and libSQL substrate

> **Milestone:** M7 · **Size:** S · **Type:** decision · **Components:** system-level
> **Branch:** `adr/t82-metastore-scope-and-substrate-adr` · **Depends on:** — · **Blocks:** T83

## Why / context

M6 made one binary host **many DAGs** (`#[dag]` + inventory, ADR 092). The operator now wants **one place to query the state of everything that runs** — a persistent, cross-run index — instead of scanning per-run `events.jsonl` files. This is a genuine capability decision because `arch.md` states, as a *permanent* non-goal, that dagr is **not "a metadata store"** (`arch.md`, "What this is"), and the run-store ADR (012 · T0.6) explicitly rejects "a cross-run metadata store or index." No feature ticket may cross that boundary until the boundary itself is amended and the decision is recorded — otherwise every M7 feature diff fails the orchestrator's scope check (ticket-conventions §8) or triggers a spec-conflict STOP (§10). **This ticket owns that amendment and the substrate decision; it ships no code.**

The distinction that makes the carve-out sound: `arch.md`'s permanent exclusions are about **coordination** — a scheduler, a distributed execution system, a *server the engine depends on to run*. A **local, embedded, opt-in, non-coordinating run index** that the engine writes the way it already writes the JSONL event stream — no server, coordinating nothing, and off by default — belongs with the in-scope **run store** (T0.6), not with the excluded coordinator. The event stream remains the source of truth; the index is a derived, guaranteed projection.

## Objective

Produce the ADR (written into this ticket file per ticket-conventions §6) and amend `arch.md`, recording these decisions:

- **Scope carve-out.** Amend `arch.md`'s permanent-non-goals sentence so "metadata store" continues to exclude a *coordinating* cross-run store / scheduler index, while a **local, embedded, opt-in, non-coordinating run index derived from the event stream** is explicitly permitted as an extension of the run store. The permanent exclusions (scheduler, distributed execution system, a required server, web interface, DSL, backfill orchestrator) stay verbatim. Do **not** rewrite the T0.6 ADR text; add a superseding note per §10 pointing at this ADR for the "no cross-run index" clause only.
- **Substrate = libSQL, not the Turso rewrite.** The store is the **libSQL** C fork via the `libsql` Rust crate (embedded local file; multi-process WAL; SQLite-file-compatible; embedded-replica/`sqld` sync available later). Reject the `tursodatabase/turso` rewrite for now: pre-1.0, and its row-level `BEGIN CONCURRENT`/MVCC is single-process and cannot use indexes — disqualifying for a queryable, multi-process store. Pin the crate exactly.
- **Concurrency model.** SQLite WAL: many readers, one writer at a time. Many concurrent run-processes write **directly** to one shared file (libSQL supports multi-process access); serialize with `busy_timeout` + **`BEGIN IMMEDIATE`** for every write txn + an app-level bounded `SQLITE_BUSY` retry (backoff + jitter). Same-host local filesystem only. No MVCC.
- **Write model = both, guaranteed.** A live tee sink writes durably during a run (same `SinkFault` contract as the JSONL sink); a `sync` reconcile pass folds event streams into rows idempotently for backfill/repair. Not best-effort.
- **Boundaries this ADR keeps.** `dagr-core` stays runtime-dependency-free; the store lives in a new `dagr-metastore` crate with an artifact-only edge, behind a default-off `metastore` feature. **Native access only** (`sqlite3`/`turso`/`libsql`); **no** Postgres wire protocol. Data/lineage enrichment is deferred to **M8**. A dedicated `sqld` server, embedded-replica sync, and a future migration to the `turso` rewrite are named as out-of-scope escape hatches behind a `MetaStore::open(mode)` seam.

## Test plan (write these first — TDD)

Decision ticket: the "tests" are mechanical file/content assertions, checked before authoring and then made true.

- **ADR completeness.** This file contains an ADR with all five sections — **Status**, **Context**, **Decision**, **Consequences**, **Rejected alternatives** — and Status is `Accepted` (or `Proposed` pending operator sign-off; see Open questions).
- **arch.md amended, exclusions intact.** `arch.md` is edited so the metadata-store non-goal distinguishes coordinating vs. embedded/non-coordinating; a grep confirms the words *scheduler*, *distributed execution system*, *web interface*, *domain-specific language*, and *backfill orchestrator* still appear in the permanent-non-goals sentence unchanged.
- **Substrate named.** The ADR names `libsql` (the fork) as the substrate and records the exact pinned version, and states why the `turso` rewrite is rejected (multi-process + index constraints under MVCC).
- **Supersession recorded.** The T0.6 ADR's "no cross-run metadata store or index" clause carries a "Superseded (in part) by ADR 097 (T82)" note; the rest of T0.6 is unchanged.
- **No code.** `git diff` for this branch touches only `docs/**` (this file, `arch.md`, and `README.md`); no `crates/**` changes.

## Definition of done

- [ ] This file contains an ADR with **Status / Context / Decision / Consequences / Rejected alternatives** sections capturing the five decisions in Objective.
- [ ] `arch.md`'s permanent-non-goals sentence is amended to permit a local, embedded, opt-in, non-coordinating run index while keeping every other exclusion verbatim; the run-store section notes the derived index as an extension of the run store.
- [ ] The T0.6 ADR (012) is marked "Superseded (in part) by ADR 097" for the cross-run-index clause only; no other T0.6 text changes.
- [ ] The ADR names `libsql` (pinned exact version) as the substrate, records the WAL + `busy_timeout` + `BEGIN IMMEDIATE` + bounded-retry concurrency model, the guaranteed live-tee + reconcile write model, the zero-dep-core / artifact-only-crate / default-off-feature boundaries, native-access-only (no pgwire), and the M8 lineage deferral.
- [ ] The diff is docs-only (no `crates/**`).
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions

- **Operator sign-off on amending a *permanent* boundary — RESOLVED, pre-approved (recorded per §5).** `arch.md` calls the metadata-store exclusion permanent, and ticket-conventions §8/§10 would normally make moving it a hard STOP. The operator (athvin) **explicitly accepted this carve-out on 2026-07-27** ("you don't need me to halt and sign off. I'm good with it"). The implementer therefore writes the ADR with **Status: Accepted**, cites this recorded operator acceptance in the ADR's Status/Context, and the loop may ship it through the normal branch/PR/merge flow **without halting**. No other contested decisions.

## Out of scope

- The `dagr-metastore` crate, schema, and connection seam — **T83**.
- The reconcile `sync` command and event→row mapping — **T84**; the live tee sink — **T86**.
- All data/lineage enrichment (content-hash metadata, produced/consumed records) — **M8** (T89–T91).
- A dedicated `sqld` server, embedded-replica/Turso-Cloud sync, and any migration to the `turso` rewrite — future work behind the `MetaStore::open(mode)` seam; not this milestone.
- A Postgres wire-protocol endpoint — rejected in this ADR; not a later ticket unless external Postgres tooling becomes a hard requirement.
- Scope boundary restated: even with this carve-out, dagr remains **not** a scheduler, distributed execution system, coordinating metadata store, web interface, DSL, or backfill orchestrator, and the graph's shape never changes at runtime.
