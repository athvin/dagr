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

---

# ADR: metastore scope carve-out and libSQL substrate

> This repo keeps each ADR inside its own implementation-ticket file (the T0.6
> run-store ADR embeds itself at `docs/implementation/012-…`, and the M5/M6 ADRs
> the same way). This ADR is committed here, at
> `docs/implementation/097-T82-metastore-scope-and-substrate-adr.md`, the ADR
> location for ticket T82 — satisfying ticket-conventions §6 (literal-DoD-first)
> with zero deviation. It amends `docs/arch.md` and marks the T0.6 ADR (012)
> superseded-in-part for its cross-run-index clause only; it ships **no code**.

## Status

**Accepted (2026-07-27).** This is a **decision** ticket (ticket-conventions §4):
it amends a *permanent* `arch.md` boundary and picks a substrate, and ships **no
production code** — the only committed artifacts are this ADR, the `arch.md`
amendment, and the partial-supersession note on ADR 012. The shipping crates
(`core`, `artifact`, `render`, `cli`) are unchanged and `Cargo.lock` is untouched.

**Operator acceptance is recorded, not pending.** `arch.md` calls the
metadata-store exclusion *permanent*, so moving it would normally be a hard STOP
under ticket-conventions §8/§10. The operator (athvin) **explicitly accepted this
carve-out on 2026-07-27** ("you don't need me to halt and sign off. I'm good with
it"), recorded in this ticket's `## Open questions` per ticket-conventions §5.
This ADR is therefore **Accepted** and the loop ships it through the normal
branch/PR/merge flow without halting. No other contested decisions exist (the
ticket's Open questions section lists exactly this one, resolved).

**No spike, no code.** This is a scope-and-substrate decision. The substrate's
concurrency footguns (single-writer WAL, the `DEFERRED`→write `SQLITE_BUSY` that
`busy_timeout` will not retry) are known from prior research and are *implemented
and proven* downstream — the connection seam in **T83** and the multi-process
write test in **T85** — not here. This ADR fixes the decisions those tickets
inherit; it validates nothing by running code, so there is no prototype to
quarantine and the tree stays clean.

## Context

`arch.md` states, as a *permanent* non-goal, that dagr is **not "a metadata
store."** ADR 012 (T0.6 · run store contract) sharpened that into an explicit
rejected alternative — "A shared cross-run index or metadata store … a cross-run
index is a metadata store dagr will never be" — and its §10 records that
"cross-run analysis is concatenation partitioned by the run identity … no
cross-run index and no shared metadata are needed or built." Every M7 feature
ticket (T83–T88) needs a cross-run index; none may cross that boundary until the
boundary itself is amended and the decision recorded, or every M7 diff fails the
orchestrator's scope check (ticket-conventions §8) or triggers a spec-conflict
STOP (§10). **This ticket owns that amendment.**

The distinction that makes the carve-out sound — and consistent with what ADR 012
already decided — is **coordination**. `arch.md`'s permanent exclusions are about
a store the engine *depends on to coordinate*: a scheduler, a distributed
execution system, a *server the engine hands off to run*. What M7 wants is the
opposite: a **local, embedded, opt-in, non-coordinating run index** that the
engine writes the way it already writes the JSONL event stream — no server,
coordinating nothing, off by default. The event stream remains the **source of
truth**; the index is a **derived, guaranteed projection** of it. ADR 012's own
rejection of a *coordinated multi-process store lock* ("the road to a scheduler")
is untouched by this ADR: the index adds no cross-process coordination — disjoint
run rows, single-writer WAL, no shared lock the engine waits on.

M6 (ADR 092) made one binary host **many DAGs** (`#[dag]` + inventory). The
operator now wants **one place to query the state of everything that runs** — a
persistent, cross-run index — instead of scanning per-run `events.jsonl` files.
That is a genuine capability, and it belongs with the in-scope **run store**
(T0.6), as its extension, not with the excluded coordinator.

## Decision

Five decisions, in the order the M7 tickets consume them.

### 1. Scope carve-out (arch.md amended; ADR 012 superseded in part)

`arch.md`'s permanent-non-goals sentence is amended so **"metadata store"**
continues to exclude a *coordinating* cross-run store / scheduler index, while a
**local, embedded, opt-in, non-coordinating run index derived from the event
stream** is explicitly **permitted** as an extension of the run store. The other
permanent exclusions — **scheduler, distributed execution system, web interface,
domain-specific language, backfill orchestrator** — stay **verbatim**, and the
graph's shape still never changes at runtime. The run-store section
("The shape of a run") gains a note that a derived, opt-in index may be built over
the event stream. ADR 012's cross-run-index clause carries a "Superseded (in
part) by ADR 097 (T82)" note; **no other T0.6 text changes** (ticket-conventions
§10 — merged decision text is never rewritten).

### 2. Substrate = libSQL (the fork), not the `turso` rewrite

The store is the **libSQL** C fork accessed via the **`libsql` Rust crate**,
**pinned exactly** at **`libsql = "=0.10.0-pre.4"`** (the latest published version
at decision time; T83 confirms the pin against the lockfile and re-pins with an
in-PR note if a newer release is chosen). Properties that make it the right choice:

- **Embedded local file**, **SQLite-file-compatible** (queryable with stock
  `sqlite3`, or the `turso`/`libsql` CLIs — the T87 payoff: zero new tools).
- **Multi-process WAL** — many OS processes read/write one shared file, which dagr
  needs because it is one-run-per-process and "many DAGs at once" means many
  processes against one file.
- **Escape hatches available later** — `sqld` server and embedded-replica sync
  exist behind the same crate, reserved (not built) behind a `MetaStore::open(mode)`
  seam (§out-of-scope, T83's recognized stub).

**The `tursodatabase/turso` rewrite is rejected for now** (see Rejected
alternatives): it is **pre-1.0**, and its row-level `BEGIN CONCURRENT` / MVCC is
**single-process and cannot use indexes** — disqualifying for a *queryable,
multi-process* store, which is exactly this store's job.

### 3. Concurrency model — SQLite WAL, single-writer, explicit write discipline

WAL gives **many readers, one writer at a time**. Many concurrent run-processes
write **directly** to one shared file (libSQL supports multi-process access). The
write discipline that makes disjoint-row writes safe:

- **`BEGIN IMMEDIATE`** for **every write transaction** — never a `DEFERRED`
  read-txn that later upgrades to a write, because that upgrade hits an **instant
  `SQLITE_BUSY` that `busy_timeout` will not retry**.
- **`busy_timeout`** set on open, plus an **app-level bounded `SQLITE_BUSY` retry**
  (backoff + jitter) around each write txn.
- **Same-host local filesystem only. No MVCC.**

The seam that encodes this is **T83**; the multi-process guarantee is proven as a
**test** in **T85** (mirroring T67's two-concurrent-runs run-store proof), and if
any assertion fails on the pinned version the seam is hardened there and the
change recorded — the reopen condition below.

### 4. Write model — both paths, guaranteed (not best-effort)

- A **live tee sink** writes durably **during a run**, under the **same
  `SinkFault` contract as the JSONL sink** (T86). It is a *guaranteed* durable
  write, not a fire-and-forget side effect.
- A **`sync` reconcile pass** folds event streams into rows **idempotently**, for
  backfill and repair (T84). Idempotent so a re-run of `sync` over the same
  streams converges on the same rows.

Both are **guaranteed**. The event stream stays the source of truth; the index is
a projection that either path can (re)produce.

### 5. Boundaries this ADR keeps

- **`dagr-core` stays runtime-dependency-free.** The store lives in a **new
  `dagr-metastore` crate** (T83) with an **artifact-only edge** — it depends on
  `dagr-artifact` (for the event/artifact types and, later, `fold_stream`) and
  has **no path to `dagr-core`**, mirroring how `dagr-render` depends on
  `dagr-artifact` and provably cannot reach core (C24).
- **Default-off `metastore` cargo feature.** `libsql` is heavy and pre-1.0, so it
  is gated behind a **default-off** feature (the inverse of the default-on `dag`
  feature). A plain `cargo build --all` and `--no-default-features` pull neither
  `libsql` nor `dagr-metastore`; the zero-dep-core guarantee is untouched.
- **Native access only.** Query paths are `sqlite3` / `turso` / `libsql` — the
  file is byte-compatible with stock SQLite. **No Postgres wire protocol** (see
  Rejected alternatives).
- **Data/lineage enrichment is deferred to M8** (T89–T91): content-hash metadata,
  produced/consumed records. M7 ships only the run index.
- **`sqld` server, embedded-replica sync, and a future `turso` migration** are
  named **out-of-scope escape hatches** behind the `MetaStore::open(mode)` seam —
  reserved, not built.

## Consequences

- **The M7 boundary is now open — and only this far.** The `arch.md` amendment
  and the ADR-012 supersession note let T83–T88 build the index without each
  tripping the scope check. Every one of them re-decides none of the above.
- **Each M7 ticket inherits a named seam:** **T83** (crate + schema + connection
  seam encoding §3's WAL/`BEGIN IMMEDIATE`/`busy_timeout`/retry discipline behind
  a default-off feature, §2's exact pin, §5's artifact-only edge); **T84**
  (idempotent `sync` reconcile, §4); **T85** (multi-process write test proving §3;
  hardens T83's seam on failure — the reopen point); **T86** (guaranteed live tee
  under the `SinkFault` contract, §4, gated by a `DAGR_*` toggle default-off);
  **T87** (example + cookbook querying via native `sqlite3`, §2/§5); **T88**
  (acceptance gate asserting the §5 boundary invariants structurally).
- **Coverage / criteria matrix: no change.** This is a docs-only decision that
  adds **no new numbered `arch.md` machine acceptance criterion** — it amends the
  "What this is" and "The shape of a run" prose, neither of which introduces a
  classified criterion. A decision ticket owes no covering test, and the criteria
  matrix classifies arch.md criteria, not tickets (the same posture ADR 012 took:
  "makes no edit to the coverage matrix or to the criteria-matrix partition").
  The M7 *feature* tickets carry their own tests; the acceptance gate is T88.
- **Reopen condition.** If T85's multi-process write test cannot be made green on
  the pinned `libsql` version by hardening T83's seam (timeout, backoff schedule,
  connection-per-process discipline) — i.e. the multi-process-WAL premise this ADR
  bets on does not hold — the substrate decision (§2/§3) **reopens here**, in this
  ADR, rather than being worked around locally. Likewise if the artifact-only edge
  or the default-off gate cannot be kept (§5), the boundary reopens here. A local
  workaround that silently diverges from this ADR is a defect, not a fix.

## Rejected alternatives

- **The `tursodatabase/turso` rewrite as the substrate.** **Rejected for now:** it
  is **pre-1.0**, and its row-level `BEGIN CONCURRENT` / MVCC concurrency is
  **single-process and cannot use indexes** — precisely the two properties this
  store cannot give up, because it must be **queryable** (indexes) and
  **multi-process** (one file, many run-processes). A migration to it later is a
  named escape hatch behind `MetaStore::open(mode)`, not a v1 choice.
- **A Postgres wire-protocol endpoint** (pgwire, so external Postgres tooling can
  connect). **Rejected:** it is a *server surface* — a coordinating access point
  the engine would host — which is exactly the "coordinating metadata store /
  service" the amended boundary still excludes. Native access (`sqlite3` /
  `turso` / `libsql` over the SQLite-compatible file) covers the query need with
  zero new server and zero new tools. Not a later ticket unless external Postgres
  tooling becomes a hard requirement.
- **A best-effort (fire-and-forget) write path.** **Rejected:** a run index that
  silently drops rows under load is worse than none — a query would lie. Both the
  live tee (`SinkFault` contract) and the `sync` reconcile (idempotent fold) are
  **guaranteed**, so the index is always a faithful projection of the event
  stream.
- **Putting the store in `dagr-core`, or on by default.** **Rejected on the
  zero-dep-core boundary:** `libsql` is heavy and pre-1.0; pulling it into core, or
  enabling it by default, would break the "core holds a minimal dependency set"
  commitment (arch.md Stability) and make every plain build pay for a feature it
  did not ask for. The store is a **separate, artifact-only crate** behind a
  **default-off feature**.
- **A coordinating cross-run store the engine depends on to run** (the thing the
  permanent non-goal always excluded — a scheduler index, a service other
  processes hand off to). **Still rejected, unchanged from ADR 012.** This carve-out
  permits only a *non-coordinating* projection: the event stream stays the source
  of truth, runs write disjoint rows, and no engine code path waits on a shared
  lock or a remote service to make progress. Moving that boundary is **not** what
  this ADR does.

*(Operator acceptance of the carve-out recorded in §Status and this ticket's
Open questions per §5. Reopen condition stated in §Consequences.)*
