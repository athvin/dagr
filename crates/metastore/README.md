# dagr-metastore

The local, embedded, **opt-in** run index for
[dagr](https://github.com/athvin/dagr): a queryable projection of the JSONL event
stream into a libSQL / `SQLite` file, so one many-DAG binary has a single place to
ask cross-run questions instead of scanning per-run `events.jsonl` files.

## What it is — and is not

dagr is permanently **not a coordinating metadata store**, and this crate does
not make it one. "Metadata store" there means a store the engine *depends on to
coordinate* — a cross-run scheduler index, a service other processes hand off to.
What this is instead is an extension of the run store: a derived index the engine
writes the way it already writes the event stream.

- The **event stream stays the source of truth**; the index is a projection of
  it, and can be rebuilt from it.
- It coordinates **nothing**. No server, no listener, no election, no shared
  lock, no queue.
- It is **off by default**. `dagr-cli` reaches it only behind a default-off
  `metastore` feature, so a plain `cargo build` pulls neither this crate nor
  libSQL.

That carve-out is a recorded decision (ADR 097), not an assumption.

## Its place in the workspace

```text
cli ──────► core, artifact, render  (+ metastore, behind a default-off feature)
render ───► artifact
metastore ► artifact, libsql, tokio   ◄── you are here
core ─────► macros  (build-time)
artifact ─► (nothing)
```

The only workspace edge is onto `dagr-artifact` — the event and artifact types
this crate maps into rows. There is deliberately **no** path to `dagr-core`, the
same C24-style boundary `dagr-render` keeps, so `dagr-core`'s
zero-runtime-dependency guarantee is untouched.

## The concurrency recipe

libSQL's WAL is **single-writer**, and a deferred read transaction that later
upgrades to a write hits an instant `SQLITE_BUSY` that `busy_timeout` will not
retry. This crate therefore encodes the verified discipline rather than leaving
it to each caller:

- open with `journal_mode=WAL`, `synchronous=NORMAL`, and a `busy_timeout`;
- open **every** write transaction with `BEGIN IMMEDIATE`;
- wrap each write transaction in an app-level bounded `SQLITE_BUSY` retry with
  exponential backoff and jitter, surfacing a hard error past the cap.

Many OS processes writing one file is proven by a multi-process test harness that
spawns genuine processes, not threads.

## Documentation

The component specification is
[`docs/arch.md`](https://github.com/athvin/dagr/blob/main/docs/arch.md) —
"The shape of a run", and its permanent-non-goals carve-out under "What this is".

Licensed MIT.
