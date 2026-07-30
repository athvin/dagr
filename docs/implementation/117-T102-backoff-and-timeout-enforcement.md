# 117 · T102 — real retry backoff and per-attempt timeout on the run-flow path

> **Milestone:** M10 · **Size:** M · **Type:** feature · **Components:** C5, C14, C16
> **Branch:** `feat/t102-backoff-and-timeout-enforcement` · **Depends on:** T100 · **Blocks:** T108

## Why / context

Two policies that `NodePolicy` declares, the graph artifact records, and the policy
fingerprint hashes are **not enforced** on the path every `#[dag]` and
`RunnableFlow` pipeline actually runs. Both were found while planning M10, and both
are defects in the *local* engine today — remote execution only raises the cost.

**1. The retry backoff delay is never slept.** `GenericNodeRunner::run`
(`crates/cli/src/run_flow.rs`) calls the retry loop with `|_delay: Duration| async {}`
as its timer. `execution::run_with_retries` computes the delay correctly, emits a
`BackoffStarted { delay }` event carrying it, and then awaits a future that returns
immediately. So the event stream *claims* a backoff that did not happen, and retries
fire back-to-back. The engine's own retry machinery is right — `Backoff`,
`nominal_delay`, subtractive full jitter, the cap — it is simply not connected to a
clock on this path. Note the honesty cost: an artifact reader (and the fold's
`backoff` phase duration) is told about wait time that never elapsed.

**2. No per-attempt timeout is armed.** `NodePolicy::timeout`
(`crates/core/src/assembly.rs:554`) is stored, fingerprinted, and rendered, but
`GenericNodeRunner` never reaches `execution::run_attempt_with_timeout`. Only
hand-written `NodeRunner`s in the test suite exercise it. The only timeout the real
`drive()` arms is the teardown deadline (`crates/cli/src/driver.rs:2579`). A task
that hangs on the main path hangs the run until the operator kills it.

Both are prerequisites for M10, not incidental cleanups. A remote attempt's timeout
is the *only* bound on a pod that never reports — ADR 115 §3's no-callback path has
no heartbeat to fall back on — and un-delayed retries against a throttled API server
are precisely the behaviour rate limiting exists to punish. **T108 cannot be correct
on top of either defect**, which is why this lands before it.

The mechanisms already exist and are tested in `dagr-core`; this ticket wires them
up. `run_attempt_with_timeout` already moves the permit into the raced work future,
so dropping the future on timeout releases capacity for free, and its race
combinator polls the deadline first so an already-elapsed deadline wins
deterministically even when task workers are jammed.

## Objective

Enforce both policies on the `RunnableFlow` path, without changing what a run that
uses neither does.

- Supply a **real timer** to the retry loop in `GenericNodeRunner::run` so the
  computed backoff actually elapses, and so the `BackoffStarted` delay in the event
  stream is a claim about elapsed time. The timer is injected, not hard-coded, so
  tests keep deterministic control and `dagr-core` gains no clock.
- **Arm the per-attempt timeout** from `NodePolicy::timeout` for await-bound nodes
  via `execution::run_attempt_with_timeout`, honouring the existing permit-into-the-
  future discipline.
- For **blocking and compute** classes, use the existing unkillable-work path
  (`mark_blocking_timed_out` / `TimeoutDecision` / `LateResultBarrier`): mark
  `timed-out` immediately, hold the permit until the closure returns, defer retry
  past the zombie, and refuse the late result's slot fill and scratch write. Do not
  invent a second mechanism.
- Thread the node's `RetryConfig` and timeout from the assembled policy to the
  runner at registration, reusing `NodePolicy::retry_config()`.
- Preserve byte-identical behaviour for a node with no timeout and no retries.

## Test plan (write these first — TDD)

**Backoff actually elapses**
- Given a node with retries and a known backoff, when its attempts fail
  retry-eligibly, then wall-clock elapsed between attempt N's failure and attempt
  N+1's start is at least the emitted `BackoffStarted` delay — currently ~0, so this
  test fails first.
- Given the same run, then each emitted `BackoffStarted` delay respects the
  configured cap and matches `Backoff::nominal_delay` under `NoJitter`.
- Given an injected test timer, then the assertion is on *recorded* delays rather
  than real sleeping, so the suite stays fast and deterministic.

**Timeout is enforced (await-bound)**
- Given an await-bound node whose body awaits forever and a policy timeout, when the
  run executes, then the node reaches `timed-out` at approximately the deadline and
  the run terminates — currently hangs, so this fails first.
- Given that timeout, then the node's permit is released (a subsequent node that
  needs the same capacity is admitted), proving the permit moved into the dropped
  future.
- Given a retry-eligible timeout with retries remaining, then a further attempt is
  made, and the terminal state is recorded exactly once.

**Timeout is enforced (blocking/compute)**
- Given a blocking node that busy-loops past its timeout, then the attempt is marked
  `timed-out` immediately, the permit is **held** until the closure returns, retry
  does not start while the zombie is live, and the late result cannot fill the slot
  or write scratch.
- Given the run ends with that zombie live, then exactly one `zombie-at-exit` event
  is emitted and the bounded grace is respected.

**No regression**
- Given a node with no timeout and no retries, then its event stream is
  byte-identical to before this change.
- Given the existing quickstart and demo examples, then their streams are unchanged.
- Given the thousand-node scale benchmark, then per-node overhead stays inside the
  published budget (arming a timeout must not cost a millisecond).

## Definition of done

- [ ] `GenericNodeRunner` supplies a real, injected timer to the retry loop; emitted
      `BackoffStarted` delays correspond to elapsed time.
- [ ] `NodePolicy::timeout` is armed per attempt on the `RunnableFlow` path:
      await-bound via `run_attempt_with_timeout`, blocking/compute via the existing
      `TimeoutDecision` / `LateResultBarrier` path.
- [ ] A hanging await-bound node reaches `timed-out` at its deadline and releases its
      permit; a hanging blocking node is marked `timed-out` while its permit is held
      until the closure returns.
- [ ] Retry after a timeout is deferred past a live zombie; a timeout never produces
      a second terminal state.
- [ ] A node with neither policy produces a byte-identical event stream; the scale
      benchmark stays inside the per-node overhead budget.
- [ ] `dagr-core` gains no clock and no runtime dependency; the timer is injected.
- [ ] Tests pass on `ubuntu-latest` and `macos-latest`.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, the
      rustdoc lint, and cargo-audit/deny where configured).

## Open questions

None. The retry loop, the timeout combinator, the unkillable-work path, the permit
discipline, and `NodePolicy::retry_config()` are all merged, tested mechanisms; this
ticket connects them at one call site. The timer injection point mirrors the existing
`Jitter` injection and is recorded in-PR.

## Out of scope

- Any remote or Kubernetes behaviour — **T105** onward. This ticket is local-engine
  correctness that M10 depends on.
- Changing the retry or timeout *policy surface* (new knobs, per-class defaults,
  timeout escalation). The declared policy is enforced as written; nothing is added.
- Changing `NodePolicy`, the policy fingerprint, or the graph artifact — behaviour
  changes, the recorded shape does not.
- Arming timeouts for teardown nodes, which already have the teardown deadline.
- Scope boundary restated: enforcing a declared policy adds no capability and no
  coordination; dagr remains not a scheduler, a distributed execution system, a
  coordinating metadata store, a web interface, a DSL, or a backfill orchestrator,
  and the graph's shape never changes at runtime.
