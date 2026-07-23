# 046 · T36 — C16: OS signals, final flush, and temp cleanup

> **Milestone:** M2 · **Size:** M · **Type:** feature · **Components:** C16
> **Branch:** `feat/t36-os-signals-flush-and-cleanup` · **Depends on:** T19, T35, T0.6 · **Blocks:** T38, T55, T63, T70

## Why / context
The cancellation core already exists (T35: run-scoped token with per-attempt children, grace period, graceful drain, `cancelled`/`abandoned` classification, shutdown-budget arithmetic printed at startup). This ticket wires the *outside world* to that core and closes C16's shutdown story: OS termination signals must map to cancellation, the event stream (C19, via the T0.6 run-store sink) must be completed and fsync'd before the process exits within the shutdown budget, a per-run temp-directory convention must confine local task debris and be reclaimed by the next invocation, and an unwritable sink at shutdown must produce a bounded wait plus a distinct exit code rather than a hang. This is what the operational model owes an orchestrator: prompt, honest reaction to a kill signal within a stated budget (C16), and a complete artifact at a predictable location (C19). It governs C16's acceptance criteria for signal handling, final flush, temp cleanup, and the sink-failure exit path.

## Objective
Make a running dagr binary shut down correctly and honestly when the OS asks it to, and confine and reclaim the local debris a run leaves behind. Concretely:

- Install signal handlers so that `SIGTERM` and `SIGINT` each trigger the run-scoped cancellation from T35, with the cancellation reason attributed to an externally originated termination (not a run failure).
- Ensure a second identical signal during shutdown does not corrupt the shutdown path (idempotent / hardened against re-entry); the first signal starts the budgeted shutdown and subsequent ones do not shortcut the final flush.
- On the signal-driven shutdown path, complete the event stream: emit the remaining terminal/`run-finished` and any zombie-at-exit records (C19), then perform the run-end `fsync` through the T0.6 sink, all inside the worst-case shutdown budget (grace + teardown deadline + the bounded 2-second final flush).
- Establish the per-run temp-directory convention: everything a task writes locally lives under the run's temp directory (reached through the context), and the *next* invocation removes leftover per-run temp directories regardless of how the prior process ended; cooperative tasks that observe cancellation within grace still clean up their own temp artifacts.
- Handle an unwritable event sink *at shutdown*: wait a bounded time for the final flush to succeed, and if it cannot, exit with the distinct sink-failure exit code (C26) instead of blocking indefinitely.
- Keep the signal handling and safety machinery on the framework's isolated runtime so the final flush and signal reaction still work even when task workers are saturated (consistent with C13/T33's isolation guarantee).

## Test plan (write these first — TDD)

**Signal maps to cancellation with the correct reason.**
Setup: launch a child run of a sample pipeline with at least one long-running cooperative task in flight. Action: send `SIGTERM` to the process and let it complete. Expected: the run enters cancelling with an externally-originated-termination reason, the in-flight cooperative task that returns within grace is recorded `cancelled` (not `failed`), and the process exits with the cancellation exit code — because no non-teardown node ended `failed` or `timed-out`.

**SIGINT behaves identically to SIGTERM.**
Setup: same as above. Action: send `SIGINT` instead. Expected: the same cancellation path, the same `cancelled` classification, and the same cancellation exit code; the two signals are observably interchangeable for triggering shutdown.

**Complete, valid stream is written before exit on a signal.**
Setup: launch a child run and let it reach steady state with in-flight work. Action: send `SIGTERM`, wait for exit, then parse the run's event stream from disk. Expected: the stream is well-formed with gapless, strictly increasing sequence numbers; it contains the terminal-state events for the affected nodes and a `run-finished` record; every record carries the run identity and schema version. No record is lost that was emitted before exit (at most one trailing partial is tolerable per C19, but a clean signal shutdown should not produce even that).

**Final flush is fsync'd.**
Setup: launch a child run under a sink/harness that records `fsync` calls (or use a fault-injection sink from the T0.6 contract). Action: send `SIGTERM` and let it exit. Expected: exactly one run-end `fsync` is observed through the sink after the final records are written, matching C19's "fsync at cancellation/run end" guarantee.

**Shutdown fits inside the printed budget.**
Setup: start a run with configured grace and teardown deadlines whose arithmetic sum (grace + teardown + 2 s final flush) is a known value B, and confirm the binary printed that worst-case budget at startup (from T35). Action: send `SIGTERM` and measure wall-clock time from signal delivery to process exit against B. Expected: the process exits at or before B; a cooperative task that returns promptly makes it exit well before B.

**Abandoned work does not extend shutdown past budget.**
Setup: start a run with a synchronous task that ignores cancellation and keeps running past grace. Action: send `SIGTERM`. Expected: the task is recorded `abandoned` (distinct from `cancelled` and from `failed`), the process still exits within the budget without waiting for the zombie thread, and a zombie-at-exit event is present in the stream (C19). The exit code is the cancellation code, since `abandoned` attributes to cancellation and no run failure occurred.

**Repeated signals during shutdown do not corrupt the stream.**
Setup: launch a child run with in-flight work. Action: send `SIGTERM`, then send a second `SIGTERM` (and/or `SIGINT`) a fraction of a second later while the shutdown path is running; wait for exit and parse the stream. Expected: the second signal neither aborts the final flush early nor duplicates terminal records nor breaks sequence-number gaplessness; the stream is still complete and valid, and the exit code is unchanged.

**Cooperative task cleans up its own temp artifacts on cancellation.**
Setup: a task that, on start, writes a file under the run's temp directory (via the context) and, on observing cancellation within grace, removes it. Action: run the pipeline, send `SIGTERM`, wait for exit, and inspect the run's temp directory. Expected: the file the cooperative task created and removed is gone by exit, demonstrating the within-grace cleanup guarantee.

**Per-run temp directory is confined to the run.**
Setup: a task that writes local scratch files only through the context's temp-directory handle. Action: run to completion normally and inspect the filesystem. Expected: all of that task's local writes are inside the run's own per-run temp directory and nowhere else; two simultaneous runs get disjoint temp directories.

**Next invocation reclaims a leftover temp directory.**
Setup: simulate a prior process that was killed abruptly (e.g. `SIGKILL`) and left a populated per-run temp directory behind. Action: start a fresh invocation of the same binary. Expected: the leftover per-run temp directory from the prior run is removed by the new invocation regardless of how the prior process ended; the current run's own temp directory is unaffected.

**Unwritable sink at shutdown yields a bounded wait and the sink-failure code.**
Setup: run a pipeline whose sink is made unwritable (fault injection from the T0.6 contract) at the moment of the final flush. Action: send `SIGTERM` (or reach shutdown) and measure time to exit and the exit code. Expected: the process waits no longer than the bounded final-flush window, makes a best-effort stderr report, and exits with the distinct sink-failure exit code (C26) — it does not hang, and it does not report success or plain cancellation.

**Sink-failure at shutdown does not masquerade as run failure.**
Setup: a run where every node succeeded but the sink becomes unwritable during the final flush. Action: reach shutdown. Expected: the exit code is the sink-failure code, not the run-failure code — the failure to record is a sink fault, distinct from a node ending `failed`/`timed-out`.

**Isolation: shutdown works under worker saturation.**
Setup: a pipeline that saturates all task workers with blocking work (mirroring T33's isolation test). Action: send `SIGTERM`. Expected: the signal is still received, cancellation still propagates, and a complete, fsync'd event stream is still written before exit within budget — the signal/flush machinery is not starved by task workers.

## Definition of done
- [ ] `SIGTERM` and `SIGINT` each trigger the run-scoped cancellation (T35), attributed as externally originated termination, and on either signal the process writes a complete event stream before exiting, within the shutdown budget (C16).
- [ ] A task that observes cancellation and returns within grace is recorded `cancelled`; one that does not return within grace is recorded `abandoned`; both are distinct from `failed`, and abandoned work does not hold shutdown open past the budget (C16, C19 zombie-at-exit).
- [ ] The final flush completes the C19 stream and performs the run-end/cancellation `fsync` through the T0.6 sink; the resulting stream has gapless, strictly increasing sequence numbers and carries run identity plus schema version on every record.
- [ ] The worst-case shutdown budget (grace + teardown deadline + bounded 2 s final flush) is honored end to end from signal delivery to exit, and matches the value printed at startup (grace/teardown deadlines remain operator-configurable per T35).
- [ ] Temporary artifacts created by cooperative tasks that observe cancellation within grace are cleaned up on cancellation (C16).
- [ ] A per-run temp-directory convention exists and is reachable through the context; everything a task writes locally goes under it, two runs get disjoint temp directories, and the per-run temp directory is removed by the next invocation regardless of how the prior process ended (C16).
- [ ] An unwritable event sink at shutdown produces a bounded wait, a best-effort stderr report, and exit with the distinct sink-failure exit code (C26) — never a hang, never a success or plain-cancellation report.
- [ ] Repeated/duplicate termination signals during the shutdown path do not corrupt, truncate early, or duplicate the stream, and do not change the exit code (signal handling is re-entry hardened).
- [ ] Signal reception and final flush run on the framework's isolated runtime, so shutdown still produces a complete, fsync'd stream even when all task workers are saturated (consistent with C13/T33).
- [ ] The exit-code selection follows C26 precedence: run failure (a non-teardown node `failed`/`timed-out`) wins over cancellation; cancellation is reported only for externally originated termination with no run failure; sink failure is reported distinctly.
- [ ] Every test scenario in the Test plan is implemented, deterministic, and independently runnable.
- [ ] Public items are documented; rustdoc examples (if any) build; the temp-directory convention and its next-invocation reclamation are documented where task authors and operators will see them.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
None.

## Out of scope
- The cancellation core itself — run-scoped token, per-attempt children, grace period, graceful drain, `cancelled`/`abandoned` classification, and the shutdown-budget arithmetic printed at startup — all belong to T35 and are consumed here, not rebuilt.
- The event-stream writer, its schema, sequence numbering, run-started header, and the mid-*run* sink-failure cancellation path belong to C19/T19 and T27; this ticket only exercises the *shutdown-time* flush and the *at-shutdown* sink-failure path.
- The run-store/sink contract and its fault-injection hooks are defined by T0.6; this ticket uses them.
- Teardown-node execution under a fresh uncancelled signal and deadline is C17/T52; only the teardown deadline's contribution to the budget arithmetic is referenced here.
- The full exit-code table and CLI verb contract are C26/T55; this ticket only asserts the specific codes it produces (cancellation and sink failure) and their precedence, not the whole table.
- Durable scratch (C18/T53) is a different store from the ephemeral per-run temp directory and is not touched here.
- Signals other than `SIGTERM`/`SIGINT`, cross-process coordination, and any attempt to reap another process's leftover work beyond the next-invocation temp-dir reclamation. dagr does not become a scheduler, a supervisor of sibling processes, or a distributed-cleanup service — residual debris beyond the enforceable guarantees is explicitly the province of the temp-dir convention and the operator.
