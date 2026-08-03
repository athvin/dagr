# 126 · T111 — the submission projection and audit surface

> **Milestone:** M10 · **Size:** M · **Type:** feature · **Components:** C19, C26
> **Branch:** `feat/t111-submission-projection-and-audit` · **Depends on:** T108 · **Blocks:** T112

## Why / context

T108 emits the write-ahead `attempt-submitted` event (ADR 115 §9). This ticket projects
it into the run index so the operator's actual question — *"what was this task launched
with, and did it read what we told it to?"* — is answerable with `sqlite3` instead of by
grepping JSON Lines across run directories.

This ticket exists as its own unit for a specific reason: it is the only M10 work in
`dagr-metastore`, it sits behind a different feature gate (`metastore`, default-off)
from the executor's, and it must uphold an invariant that has nothing to do with
Kubernetes — **the index is a projection of the event stream and nothing else**. ADR 097
made that a guarantee and CI enforces it by asserting live-tee rows are byte-identical
to rows produced by folding streams after the fact. So the rule this ticket lives under
is: the new rows must be produced by the **same** `mapping::build_statements` path from
the **same** folded artifact, reachable identically by the live tee and by
`dagr metastore sync`. There is no shortcut where the executor writes SQL directly —
that is the rejected alternative in ADR 115, and this ticket is the reason it could be
rejected without giving up the capability.

There is a real ordering subtlety worth naming up front. Every existing projected row
derives from a *finished* attempt: the fold reduces terminal outcomes. An
`attempt-submitted` record is the opposite — it exists precisely so that an attempt with
**no outcome** still leaves a trace. So the fold must surface submissions that never
produced an attempt-outcome, and the projection must represent "submitted, never
completed" as a first-class state rather than an absence. That row is the audit trail's
whole point: it is what a crashed orchestrator left behind.

## Objective

Project the submission record and make it queryable, without weakening the projection
guarantee.

- Extend `fold_stream` to surface `attempt-submitted` records on the `RunArtifact` —
  including submissions with **no** corresponding attempt-outcome, which is the case the
  record exists for. The fold stays a **reader only**: no run store, no network, no live
  graph, and deterministic.
- Add an **additive** `attempt_submitted` table (and its indexes) through the existing
  `schema::migrations` / `ADDITIVE_COLUMNS` mechanism, so an existing store upgrades in
  place and a fresh store converges on the identical shape. **No foreign keys**, per the
  schema's existing discipline — an audit row must outlive garbage collection of
  anything it references.
- Project through the **same** `mapping::build_statements` path so the live tee and
  `sync` produce byte-identical rows, and the existing parity assertion covers the new
  table.
- Store the ordered inputs so **position is preserved and queryable** — order is
  load-bearing (dagr binds inputs positionally, ceiling 8), so a row set that loses
  order loses the audit's meaning.
- Represent **intent vs reality** as distinct columns (intended target name; observed
  name, UID, host) and **submitted-without-outcome** as a queryable state.
- Ship **worked audit queries** in the cookbook: what was attempt *N* of node *X*
  launched with; which attempts were submitted but never completed; which attempts read
  a reference whose content hash differs from what was submitted (the divergence T106's
  shard records make detectable); and which runs consumed a given `uri`, joined to the
  existing lineage tables.

## Test plan (write these first — TDD)

**The projection guarantee — assert this first, it is the reason for the ticket**
- Given a run with submissions, when its rows are produced by the **live tee** and by a
  post-hoc `sync` of the same stream, then the `attempt_submitted` rows are
  **byte-identical** — the existing live-equals-reconcile assertion, extended to the new
  table.
- Given `sync` run twice over the same stream, then the rows are unchanged
  (idempotent UPSERT, matching every other table).
- Given a pre-T112 store, when it is opened, then the new table is added **in place**
  and no existing row is disturbed; a fresh store converges on the identical shape.

**The case the record exists for**
- Given a stream with an `attempt-submitted` and **no** matching attempt-outcome, then a
  row exists and is identifiable as **submitted-but-never-completed** — not silently
  dropped, and not represented as a failure.
- Given a submission followed by a successful outcome, then the row is joinable to the
  `node_attempt` row on `(run_id, node_id, try_number)`.
- Given a stream truncated mid-record after a submission (the crash case), then the fold
  still yields the submission and marks the run `interrupted` as it does today.

**Ordering and shape**
- Given a node with N inputs, then the projected inputs preserve **positional order**,
  and a query can recover the reference at position *k*.
- Given a consume-nothing source, then its row records **zero** inputs and is
  distinguishable from a node whose inputs are unknown.
- Given a submission and its outcome, then intended and observed target identity are
  both queryable and can differ without either being lost.

**Audit queries actually answer the question**
- Given a run, then a documented query returns exactly what attempt N of node X was
  launched with, including content hashes.
- Given an attempt whose shard-recorded inputs differ from its submitted inputs, then a
  documented query surfaces the divergence.
- Given a garbage-collected referent, then its audit row still resolves (no foreign key
  broke) — the same property the lineage tables already guarantee.

**Boundaries**
- Given `--no-default-features` and a default `cargo build --all`, then neither the
  metastore nor `libsql` is reachable through `dagr-cli`, unchanged.
- Given `cargo tree`, then `dagr-metastore` still has **no** edge onto `dagr-core`.
- Given a run with the metastore toggle **off**, then behaviour and the event stream are
  unchanged.
- Given `scripts/check-metastore-acceptance-boundary.sh`, then it passes — a new table is
  not a server, and `RemoteSqld`/`SyncedReplica` are still `ModeNotImplemented`.

## Definition of done

- [ ] `fold_stream` surfaces `attempt-submitted` records on the `RunArtifact`, including
      submissions with no outcome; the fold stays a deterministic reader with no store,
      network, or graph access.
- [ ] An additive `attempt_submitted` table and its indexes land through the existing
      migration mechanism, with **no foreign keys**; an existing store upgrades in place
      and converges with a fresh one.
- [ ] Rows are produced through the **same** `build_statements` path; live-tee and
      `sync` rows are byte-identical and `sync` is idempotent.
- [ ] Positional input order is preserved and queryable; a consume-nothing source
      records zero inputs, distinguishable from unknown.
- [ ] Intended and observed target identity are separate columns;
      submitted-but-never-completed is a queryable state.
- [ ] Cookbook ships worked audit queries for: launch parameters of a given attempt,
      submitted-never-completed attempts, submitted-vs-read divergence, and the join to
      existing lineage tables.
- [ ] The metastore still has no edge onto `dagr-core`; the feature stays default-off; a
      toggle-off run is unchanged; the acceptance-boundary script passes.
- [ ] Tests pass on `ubuntu-latest` and `macos-latest`.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, the
      rustdoc lint, and cargo-audit/deny where configured).

## Open questions

- **One row per submission with inputs encoded, or a child `attempt_submitted_input`
  table with an explicit position column?** A child table makes position and per-input
  hashes first-class and joins cleanly to the lineage tables; encoding them in one
  column keeps the write a single statement and matches how `durable_reference` is
  already stored as opaque text. The child table is the likely answer *because* the
  audit queries need to filter on individual references, but it is decided in-PR
  against the queries the cookbook actually ships — the queries are the requirement,
  the normalisation is not.

  **Resolved: the child table.** Decided against the shipped queries, as the question
  asks. Two of the four cookbook queries filter and join on an **individual
  reference**: the divergence query joins `attempt_submitted_input` to
  `input_consumed` on `(run_id, node_id, attempt, uri)` and compares hashes per
  reference, and the lineage query selects a single `uri` and joins it by value to
  `output_produced`/`input_consumed`. Neither is expressible against an opaque
  encoded column without JSON-unnesting inside the join, which is exactly the
  "unwieldy to document honestly" case the second question guards against. Position
  is part of the child's primary key (`run_id, node_id, attempt, position`), so the
  order dagr binds by is recorded as a fact rather than as an array's incidental
  ordering, and `WHERE position = k` recovers reference *k* directly. The parent row
  keeps `input_count` so a consume-nothing source (`0`) stays distinguishable from a
  record that never stated its inputs (`NULL`) — a distinction a bare child-row count
  cannot make. Neither table carries a foreign key, to the other or to anything else.
- **Does the divergence query belong in SQL or as a verb?** SQL first: it is a join, the
  cookbook is where the other cross-run queries live, and adding a verb would grow the
  command surface for something a query answers. Revisit only if the query is too
  unwieldy to document honestly.

  **Resolved: SQL, in the cookbook.** The shipped query is a single join with one
  `IS NOT` predicate (`docs/cookbook.md`, "Auditing what a placed attempt was launched
  with"), documented honestly in full and executed verbatim against a real store by
  `crates/cli/tests/metastore_example_and_docs.rs`. Nothing about it needed a verb, and
  adding one would have grown the command surface for a join. No revisit condition was
  triggered.

Two further decisions this ticket had to make, recorded here rather than picked
silently:

- **Submissions are surfaced on the `RunArtifact` type, not in the artifact
  document.** T108 shipped, and CI asserts, that a stream carrying `attempt-submitted`
  records folds to a **byte-identical** `RunArtifact::to_value()` as one without them
  (`crates/artifact/tests/attempt_submitted_record.rs`). That is a merged guarantee
  this ticket does not own, and this ticket's DoD asks for the records "on the
  `RunArtifact`" — so `RunArtifact::submissions()` surfaces them on the *type*, which
  is what `mapping::build_statements` reads, and `to_value()` is untouched. Both
  guarantees hold simultaneously; neither was traded away.
- **Submitted-but-never-completed is `completed = 0` plus a NULL `outcome_state`, not
  a tenth state token.** arch.md's terminal taxonomy is closed at nine members and
  `outcome_state`'s `CHECK` is generated from that same list, so the projection cannot
  invent a "submitted" terminal state. The positive, queryable fact lives in the
  `completed` flag (`WHERE completed = 0`), which is what makes the state first-class
  rather than an absence — and it is never rendered as a failure.

## Out of scope

- Emitting the record, the `@1.3` writer, and the fixture artifact — **T108** (this
  ticket only projects what already exists).
- The schema revision itself — landed with ADR 115 (T100).
- Any executor, pod, or blob behaviour.
- Serving the index, remote access, `sqld`, embedded replicas, or letting a task pod
  read or write the index — rejected in ADR 115 and ADR 097, not deferred.
- A web view, a dashboard, or an alerting hook over the audit rows. The index is
  queried with `sqlite3`; a UI is a permanent non-goal.
- Retention or pruning of audit rows specifically — they live and die with their run's
  rows under the existing `prune` behaviour.
- Scope boundary restated: a local, embedded, non-coordinating projection of records the
  run already wrote adds no coordination and no server; the event stream stays the source
  of truth. dagr remains not a scheduler, a distributed execution system, a coordinating
  metadata store, a web interface, a DSL, or a backfill orchestrator, and the graph's
  shape never changes at runtime.
