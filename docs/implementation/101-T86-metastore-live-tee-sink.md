# 101 · T86 — guaranteed live metastore tee sink

> **Milestone:** M7 · **Size:** M · **Type:** feature · **Components:** C19, C26
> **Branch:** `feat/t86-metastore-live-tee-sink` · **Depends on:** T84, T24, T55 · **Blocks:** T87, T88

## Why / context

T84 projects finished runs into the store. This ticket makes the store update **live and guaranteed while a run executes**, which is the operator's real ask: query in-flight state, not just history. The integration point already exists and is idiomatic — dagr writes its event stream through an **injected `EventSink`** (`crates/artifact/src/event_stream.rs:69`), constructed once at `EventStreamWriter::new(...)` in the driver (`crates/cli/src/driver.rs:989`), and a `TeeSink` fan-out is already used elsewhere (`crates/cli/src/bin/dagr-t63-demo-run.rs:421`). So the live path is "implement one trait + tee it," not new driver machinery.

"Guaranteed" (ADR 097) means the metastore sink is **not** best-effort: a durable-write failure returns a `SinkFault` (`event_stream.rs:405`), which the driver already folds into its shutdown exit-code precedence — so a metastore write is as durable as an event-stream write. The sink maps each event to rows through the **same T84 mapping seam** (per-event, incrementally: `run-started` → `dag`/`dag_version`/`dag_run(running)`; `attempt-outcome`/`node-terminal` → `node_attempt`/`node_terminal`; `run-finished` → finalize `dag_run`), reusing T83's `with_write_txn` (`BEGIN IMMEDIATE` + bounded retry) so concurrent run-processes writing live to one file behave exactly as T85 validated. The feature stays off unless the operator asks for it, toggled via the established `DAGR_*` flag>env>default precedence (T76).

## Objective

Add a guaranteed live tee sink, wired behind the feature and a runtime toggle.

- Implement `MetastoreSink` in `dagr-metastore` (feature-gated) as an `EventSink` that, per event, upserts the incremental rows via the T84 mapping + T83 `with_write_txn`; on a durable-write failure it returns a `SinkFault` (guaranteed contract), not a swallow.
- In `dagr-cli` (behind `#[cfg(feature = "metastore")]`), when the metastore toggle is on, construct the driver's sink as a **tee** of the existing `FileSink` and a `MetastoreSink` at the single `EventStreamWriter::new` injection point — the JSONL stream is unchanged and remains the resume source of truth.
- Add the toggle via the T76 precedence helper: a `--dagr.metastore` flag / `DAGR_METASTORE` env / default-off, resolving the store path (default under the run store, e.g. `<base>/metastore.db`, or an explicit path). Off ⇒ zero behavior change and no `libsql` activity.
- Ensure a `MetastoreSink` `SinkFault` participates in the driver's existing sink-fault handling and exit-code precedence exactly as a `FileSink` fault does.

## Test plan (write these first — TDD)

**Live rows**
- Given a flow run with the metastore toggle on, when it executes, then `dag_run(state='running')` appears at `run-started`, `node_attempt` rows appear as attempts complete, and `dag_run` is finalized to its terminal state at `run-finished` — observable mid-run, not only at the end.
- Given the run also writes its `events.jsonl`, then the JSONL stream is byte-identical to a run with the toggle **off** (the tee does not perturb the event stream).

**Guaranteed, not best-effort**
- Given an injected metastore write failure (e.g. an unwritable/locked store), when the run executes, then a `SinkFault` surfaces and the run's exit code reflects the sink fault per the driver's precedence — the failure is **not** swallowed.

**Parity with reconcile**
- Given the same run executed with the live tee vs. executed with the toggle off then `sync`ed afterward, then the resulting `dag_run` + `node_attempt` + `node_terminal` rows are identical (live == reconcile).

**Toggle + gating**
- Given the toggle off (default), then no `libsql` activity occurs and behavior is unchanged; given flag vs env, then flag wins per the `DAGR_*` precedence.
- Given `--no-default-features`, then `MetastoreSink` and the tee wiring are absent.

## Definition of done

- [ ] `MetastoreSink: EventSink` upserts incremental rows per event via the T84 mapping + T83 `with_write_txn`, and returns a `SinkFault` on durable-write failure (guaranteed).
- [ ] With the toggle on, the driver's sink is a tee of `FileSink` + `MetastoreSink` at `EventStreamWriter::new`; the `events.jsonl` output is byte-identical to a toggle-off run.
- [ ] Live rows are observable mid-run: `running` at start, per-attempt rows as they finish, terminal finalize at `run-finished`.
- [ ] A metastore `SinkFault` flows through the driver's existing sink-fault + exit-code precedence (not swallowed).
- [ ] Live-produced rows equal reconcile-produced rows for the same run (parity).
- [ ] The toggle follows `DAGR_*` flag>env>default (default off); off ⇒ no `libsql` activity and no behavior change; `--no-default-features` omits the wiring entirely.
- [ ] Tests pass on `ubuntu-latest` and `macos-latest`.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
None. (The injected-sink seam, tee pattern, `SinkFault` model, and `DAGR_*` precedence are all merged mechanisms this ticket composes; store-path default is resolved against the T0.6 run store and recorded in-PR per §5.)

## Out of scope
- The many-dags `--features metastore` example and native-access docs — **T87**.
- The end-to-end acceptance gate — **T88**.
- Lineage events/rows and `durable_reference_meta` — **M8** (the live sink maps only what the event stream carries today).
- Any change to the event stream schema or the driver's core loop beyond wiring the tee at the existing injection point.
- Scope boundary restated: a live local index is still non-coordinating; dagr remains not a scheduler, distributed execution system, coordinating metadata store, web interface, DSL, or backfill orchestrator.
