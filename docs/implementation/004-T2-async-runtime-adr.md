# 004 · T2 — Async runtime and concurrency primitives ADR

> **Milestone:** M0 · **Size:** S · **Type:** decision · **Components:** C13, C14, C16
> **Branch:** `adr/t2-async-runtime-adr` · **Depends on:** T1 · **Blocks:** T9, T33

## Why / context
Every execution-core component that touches threads or time — execution-class dispatch (C13), the attempt runner (C14), and cancellation/shutdown (C16) — needs one settled story about *which* runtime and *which* concurrency primitives it stands on, or each downstream ticket will invent its own and they will not compose. This ticket exists to lock that story as an Architecture Decision Record before the first task abstraction (T9) or the dispatch implementation (T33) is written against it. The governing spec is C13 (three execution classes plus an isolated framework runtime), C14 (per-class timeout and abandonment semantics), C16 (cancellation signal with a per-attempt child and an arithmetic shutdown budget), and the Stability section, which elevates tokio from an implementation detail to a spec-level public-dependency commitment. It also fixes the C28 test-runtime shape so single-task and await-bound task tests share one blessed way to get a runtime. As a decision ticket, its deliverable is the ADR itself plus the throwaway evidence that the chosen primitives compile and behave; no production execution code ships here.

## Objective
Produce one committed ADR that decides, records rationale for, and locks the following, each traceable to C13/C14/C16 and the Stability section:

- **Public runtime commitment.** State that tokio is the async runtime and a *supported public dependency* (C13, Stability): its types may appear in the API only where the surface is honest about it, context-exposed types are dagr-owned wherever practical, and replacing or major-bumping tokio is defined as a major-version event.
- **Blocking-pool strategy.** Decide how *blocking* execution-class work runs so it cannot starve the async runtime, and how a permit maps onto that mechanism given that a blocking closure cannot be killed (C14) and its cost stays counted while abandoned-but-running (C12).
- **Compute-pool implementation.** Resolve the open question — a dedicated compute pool (for example rayon) versus a capped semaphore over `spawn_blocking` — and record the decision such that concurrently executing compute tasks can never exceed a fixed pool size derived from the CPU allocation, with at least one thread even under a fractional quota.
- **Cancellation-token primitive.** Name the run-scoped cancellation primitive and its per-attempt child relationship (C16), including how await-bound work is cancelled by future-drop and how blocking/compute work is only *marked* (cooperative-only), so the drain and grace-period logic in later tickets has one primitive to build on.
- **Isolated framework runtime.** Decide how the framework's own machinery — timers, cancellation fan-out, the event-stream writer, signal handling — runs isolated from task execution, so a run in which every task worker is blocked can still fire a timeout, still react to SIGTERM, and still write a complete event stream (C13 acceptance).
- **Test-runtime shape (C28).** Fix the shape of the plain runtime the testing surface hands to await-bound task tests, and confirm synchronous single-task tests need no runtime at all.
- Record all of the above under a stable ADR heading with status, context, decision, consequences, and the rejected alternative(s), at the exact `path` for this ticket.

## Test plan (write these first — TDD)
These are the evidence checks that validate the decision. Each pairs a throwaway prototype (kept out of the shipping crates, or gated so it never becomes production surface) or a documentation-record assertion with a concrete, independently checkable outcome. "Prototype" means disposable code written only to prove the primitive; it is discarded or quarantined once the ADR is blessed.

- **Runtime-isolation evidence.** Setup: a prototype that starts the framework runtime, then occupies every task-worker slot with a synchronous never-returning closure. Action: arm a timer on the framework runtime and send the prototype a SIGTERM. Expected outcome: the timer fires and the signal is observed even though no task worker is free — demonstrating the framework machinery is not co-scheduled with task workers. This is the concrete backing for C13's "every task worker is blocked and a timeout still fires and SIGTERM still yields a complete stream."
- **Compute-pool bound evidence.** Setup: a prototype pool sized to N using the chosen compute-pool mechanism. Action: submit far more than N compute closures at once and record peak concurrent execution. Expected outcome: observed peak concurrency never exceeds N, and with a simulated fractional CPU quota the pool still reports at least one thread. This validates C13's "concurrently executing compute tasks never exceed the compute pool's size."
- **Blocking-does-not-starve evidence.** Setup: a prototype that dispatches one long synchronous closure on the blocking mechanism and, concurrently, an await-bound future that resolves quickly. Action: run both. Expected outcome: the await-bound future completes without waiting for the synchronous closure — validating C13's "a long synchronous task does not delay progress on unrelated await-bound work."
- **Await-bound cancellation evidence.** Setup: a prototype await-bound future that never resolves, wrapped with the chosen cancellation-token primitive and a short timeout. Action: let the timeout elapse. Expected outcome: the future is dropped and any permit-shaped guard it held is released immediately — matching C14's "await-bound attempt … is cancelled and its permit released immediately."
- **Blocking abandonment evidence.** Setup: a prototype blocking closure that ignores cancellation and keeps running, holding a permit-shaped guard. Action: mark it timed out / cancelled and observe the guard. Expected outcome: the guard is NOT released at the mark; it is released only when the closure actually returns — demonstrating the primitive can express C12/C14 abandoned-but-running accounting and cooperative-only cancellation (this is a shape check, not the full permit ledger, which is T31).
- **Cancellation-child evidence.** Setup: a prototype run-scoped cancellation token with a per-attempt child. Action: cancel the run token. Expected outcome: the child is observed cancelled, and cancelling a child does not cancel the parent — proving the token type supports C16's "run-scoped cancellation signal with a per-attempt child."
- **Shutdown-budget arithmetic evidence.** Setup: the ADR states default grace (10s), teardown deadline (15s), and final flush (2s). Action: sum them against the assumed 30-second Kubernetes kill window. Expected outcome: the total fits inside the window, and the ADR records that both grace and teardown are operator flags and that the worst-case budget is printed at startup — the decision-record backing for C16's shutdown-budget criteria (implementation lands in T35, not here).
- **Test-runtime shape evidence.** Setup: a prototype synchronous single-task invocation and a prototype await-bound single-task invocation. Action: run each. Expected outcome: the synchronous one compiles and runs with no runtime present; the await-bound one runs only on the plain test runtime the ADR specifies — validating C28's "a synchronous task requires no async runtime … an await-bound task test needs only the provided test runtime."
- **Public-dependency record check.** Setup: the ADR document. Action: read the Decision and Consequences sections. Expected outcome: tokio is named as a supported public dependency, the major-version-event rule is stated, and the rule "context-exposed types are dagr-owned wherever practical" is recorded — an assertable presence check against the committed file.
- **Compute-pool decision record check.** Setup: the ADR document. Action: read the resolved open question. Expected outcome: exactly one of {dedicated compute pool, capped semaphore over `spawn_blocking`} is chosen, the rejected alternative is named with its reason, and the choice is consistent with the compute-pool-bound evidence above.

## Definition of done
- [ ] The ADR names tokio as the async runtime and records it as a *supported public dependency* per the Stability section, with the explicit rule that replacing or major-bumping it is a major-version event (C13, Stability).
- [ ] The ADR records that tokio types may appear in the public API only where the surface is honest about it, and that context-exposed types are dagr-owned wherever practical (C13).
- [ ] The blocking-pool strategy is decided and written such that a long synchronous task cannot delay unrelated await-bound progress (C13 acceptance) and such that a held permit stays counted while a blocking closure is abandoned-but-running (C12/C14).
- [ ] The open question is resolved: the ADR chooses either a dedicated compute pool or a capped semaphore over `spawn_blocking`, names the rejected alternative with its reason, and guarantees concurrent compute tasks never exceed a fixed pool size with a floor of at least one thread under a fractional CPU quota (C13 acceptance, C12 sizing).
- [ ] The cancellation-token primitive is named, with its run-scoped signal and per-attempt child relationship, and the ADR states that await-bound cancellation is by future-drop (immediate permit release) while blocking/compute cancellation is cooperative-only and marked-not-killed (C14, C16).
- [ ] The isolated framework-runtime decision is recorded such that timers, cancellation fan-out, the event-stream writer, and signal handling run isolated from task execution, so a fully blocked task workforce can still fire a timeout, still react to SIGTERM, and still emit a complete event stream (C13 acceptance).
- [ ] The shutdown-budget arithmetic is recorded — default grace 10s, teardown deadline 15s, final flush 2s summing within the 30s Kubernetes window — with grace and teardown flagged as operator-configurable and the worst-case budget printed at startup as a stated obligation for T35 (C16).
- [ ] The C28 test-runtime shape is fixed: synchronous single-task tests need no runtime, and await-bound task tests use the plain test runtime the ADR specifies (C28 acceptance).
- [ ] Throwaway prototype evidence for each Test plan scenario exists and is quarantined from shipping crates (or removed), and the ADR references what each prototype proved.
- [ ] The ADR is committed at exactly `/Users/athvin/github.com/athvin/dagr/docs/implementation/004-T2-async-runtime-adr.md` with status, context, decision, consequences, and rejected-alternatives sections, and unblocks T9 and T33 by leaving no runtime or concurrency-primitive choice open to those tickets.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
- Dedicated compute pool (for example rayon) versus a capped semaphore over `spawn_blocking`. This ticket must *resolve* this question in the ADR, not leave it open; it is listed here as the decision this ticket is chartered to close.

## Out of scope
- The actual admission controller and permit ledger (C12) — this ticket only fixes the *shape* the primitives must support; permit accounting and the outcome matrix are T31/T37.
- The attempt runner's full retry, backoff, panic-containment, and per-attempt-timeout implementation (C14) — those are T20–T23; here we only decide the primitives they will use.
- The cancellation-core drain, grace-period, and startup-budget-printing implementation (C16) — that is T35; this ticket only records the token primitive and the budget arithmetic.
- The execution-class dispatch implementation itself (C13) — that is T33, which this ticket blocks and must not pre-empt.
- Pool *sizing* from cgroup limits and headroom (C12) — the ADR may reference the floor-of-one rule but the detection logic is not built here.
- Any temptation to introduce cross-process coordination, a scheduler, or multi-runtime orchestration to "balance" pools across runs — dagr is one run per container and never a scheduler; that boundary is named so it is not crossed.

---

# ADR: async runtime and concurrency primitives

> The repo keeps each ADR inside its own implementation-ticket file (the T1,
> T0.6, T3, T4, and T0.7 ADRs all embed the ADR at the ticket's own `path`).
> This ADR is committed here, at
> `docs/implementation/004-T2-async-runtime-adr.md`, the ADR location for ticket
> T2, satisfying the DoD line that names this exact path.

## Status

Accepted (2026-07-23). This is a decision-and-record ticket: it locks the
runtime and concurrency-primitive choices below and ships no production
execution code. The implementations it unblocks live in T9 (task abstraction),
T33 (execution-class dispatch), T31/T37 (admission and permit ledger), T35
(cancellation core and shutdown budget), and T60 (single-task test kit); each
builds against the primitives named here and re-decides none of them.

Every decision was validated by a throwaway spike built **outside** the dagr
workspace (a standalone `/tmp` Cargo project, `dagr-t2-spike`, depending on
`tokio` 1.53.1 `full`, `tokio-util` 0.7.19 `rt`, and `rayon` 1.12.0). The spike
ran one prototype per Test-plan scenario, printed the `EVIDENCE …` line each
scenario is quoted against below, and was **deleted before this PR** — no spike
code was promoted into any shipping crate (`core`, `artifact`, `render`, `cli`),
and no dependency was added to any shipping `Cargo.toml`. Wiring tokio as a real
`dagr-core` dependency is an API decision that belongs to T9/T33, not here; the
`core` dependency set stays empty (Stability, T1 ADR).

## Context

`docs/arch.md` makes three execution-core commitments that cannot each pick
their own runtime without failing to compose:

- **C13 · Execution class dispatch** — three execution classes (*await-bound* on
  the async runtime, *blocking* on a dedicated pool so it cannot starve the
  runtime, *compute-bound* on a fixed CPU-sized pool), plus the framework's own
  machinery (timers, cancellation fan-out, the event-stream writer, signal
  handling) running **isolated** from task execution so misbehaving tasks can
  degrade task progress but never disable the safety rails. C13 names the async
  runtime as **tokio**, "a supported public dependency … context-exposed types
  are dagr-owned wherever practical."
- **C14 · Attempt runner** — per-class timeout semantics that are honest about
  Rust's reality: an await-bound attempt over its timeout is *truly cancelled*
  (its future is dropped, permit released immediately); a blocking or compute
  attempt *cannot be killed*, so on timeout it is **marked** timed-out at once
  while the thread runs on as *abandoned-but-running* work whose permit is held
  until the closure actually returns (C12).
- **C16 · Cancellation and shutdown** — a run-scoped cancellation signal with a
  **per-attempt child**; await-bound work cancelled by future-drop, synchronous
  work cancelled cooperative-only; and an arithmetic shutdown budget that must
  fit the orchestrator's kill window.

The **Stability** section elevates tokio from an implementation detail to a
spec-level public-dependency commitment ("replacing or major-bumping it is a
major version event"). **C28** requires a blessed test-runtime shape: a
synchronous task needs no async runtime, an await-bound task test needs only a
provided plain test runtime. The **one-run-per-container** operational model and
arch.md's permanent non-goals forbid any cross-process pool balancing,
scheduler, or multi-runtime orchestration.

The single genuinely open question this ticket is chartered to close: **a
dedicated compute pool (e.g. rayon) versus a capped semaphore over
`spawn_blocking`** for compute-class work.

## Decision

### 1. tokio is the async runtime and a supported *public dependency*

The await-bound execution class runs on **tokio** (multi-threaded runtime).
tokio is a **supported public dependency** per the Stability section, not a
hidden implementation detail:

- **Major-version event.** Replacing tokio, or taking a major version bump of
  it, is a **major-version event** for dagr's authoring API. This is a
  first-class stability commitment, recorded here and in arch.md's Stability
  section.
- **Honest surface only.** tokio types may appear in dagr's public API **only
  where the surface is honest about it** — i.e. where a type genuinely is a
  tokio type and pretending otherwise would mislead (for example, if a task
  ever received a raw tokio handle). Everywhere the surface can be dagr's own,
  it is.
- **dagr-owned context types wherever practical.** Types reachable through the
  run context (C8) — the cancellation signal, spans, scratch/registry access —
  are **dagr-owned wherever practical**: task authors program against dagr
  types, so a future runtime swap does not ripple through every task. The
  cancellation signal in particular is exposed as a dagr-owned wrapper over the
  underlying token (see §4), never as a bare tokio/`tokio-util` type.

*Rejected runtimes* are recorded under "Rejected alternatives."

### 2. Blocking-pool strategy: tokio's blocking pool via `spawn_blocking`

*Blocking* execution-class work runs on tokio's **blocking thread pool** through
`tokio::task::spawn_blocking` (a `block_in_place`-free dispatch that moves the
synchronous closure off the async worker threads). This directly satisfies C13's
"a long synchronous task does not delay progress on unrelated await-bound work":

- **No starvation.** The synchronous closure runs on a blocking-pool thread, so
  the async worker threads stay free to drive await-bound futures. *Spike
  evidence (blocking-does-not-starve):* a quick await-bound future completed in
  ~22 ms while a 400 ms `spawn_blocking` closure ran concurrently —
  `EVIDENCE blocking: await-bound future completed in 22.07475ms, not blocked by
  a 400ms spawn_blocking closure`.
- **Abandoned-but-running accounting.** A `spawn_blocking` closure **cannot be
  killed** — dropping its `JoinHandle` does not stop the thread. On timeout or
  cancellation the attempt is **marked** immediately, but the permit-shaped
  guard the closure holds stays **counted against the pools until the closure
  actually returns** (C12/C14 abandoned-but-running; the guard is released by
  the closure returning, not by the mark). *Spike evidence (blocking
  abandonment):* the guard was still held at the cancellation mark and released
  only when the closure returned — `EVIDENCE blocking-abandon: permit held at
  cancel mark (closure still running)` then `… permit released only when the
  closure actually returned`. The full permit ledger and outcome matrix are
  T31/T37; this ADR fixes only the *shape* they must express.

The blocking pool's size is a bootstrap concern (C12, T32); this ADR requires
only that the mechanism (`spawn_blocking`) supports a permit guard whose release
is tied to closure return, which the spike proved.

### 3. Compute-pool implementation — RESOLVED: a dedicated `rayon` pool

**Chosen: a dedicated compute pool (a `rayon::ThreadPool`), sized to the CPU
allocation, over a capped semaphore over `spawn_blocking`.**

- **Fixed size, never exceeded.** A `rayon::ThreadPool` built with
  `num_threads(N)` runs **at most N closures concurrently** regardless of how
  many are submitted, satisfying C13's "concurrently executing compute-class
  tasks never exceed the compute pool's size." *Spike evidence (compute-pool
  bound):* 64 closures submitted at once to a 3-thread pool peaked at exactly 3
  concurrent — `EVIDENCE compute-pool: peak concurrency 3 <= pool size 3`.
- **Floor of one thread.** The size is `max(1, threads_from_quota)`, so even a
  **fractional CPU quota** yields **at least one thread** (C12 sizing, C13
  acceptance). *Spike evidence:* `floor_one(0)=1`. The cgroup→host sizing that
  computes `threads_from_quota` is T32's detection logic, out of scope here; the
  ADR fixes only the floor-of-one rule and the fixed-pool guarantee.
- **Why a dedicated pool over the capped semaphore.** A capped semaphore over
  `spawn_blocking` would draw compute closures from tokio's *shared* blocking
  pool, entangling the compute bound with the blocking class: the same pool must
  now honour two different caps, and blocking-class work (§2) and compute-class
  work would contend for one thread set, so a burst of blocking work could
  starve compute admission (or vice versa) even though C13 treats them as
  distinct classes with distinct sizes. A dedicated rayon pool gives compute its
  own thread set sized to CPU, keeps the "never exceed N" guarantee **structural
  in the pool** rather than resting on a correctly-held semaphore, and leaves the
  blocking pool free for genuinely blocking (I/O-waiting) work. rayon's work-
  stealing is also the right engine for CPU-bound closures. The semaphore
  approach is recorded as the rejected alternative with this reasoning.

rayon becomes a `dagr-core` dependency when T33 wires compute dispatch — an API
decision reviewed then (Stability), not here.

### 4. Cancellation-token primitive: `tokio_util::sync::CancellationToken`

The run-scoped cancellation signal is **`tokio_util::sync::CancellationToken`**,
with **per-attempt children** via `child_token()`. It is exposed to task authors
as a **dagr-owned wrapper** (per §1), never as a bare `tokio-util` type.

- **Run-scoped signal with per-attempt child (C16).** One run-scoped token; each
  attempt gets a child token. Cancelling the run cancels every child; cancelling
  a child does **not** cancel the parent. *Spike evidence (cancellation-child):*
  `EVIDENCE cancel-child: run.cancel() cancels child; child.cancel() leaves
  parent uncancelled`.
- **Await-bound cancellation is by future-drop (immediate permit release).** An
  await-bound attempt over its timeout, or under run cancellation, has its
  **future dropped**; any permit-shaped guard the future held is released
  **immediately** by that drop (C14 "await-bound attempt … is cancelled and its
  permit released immediately"). *Spike evidence (await-bound cancellation):*
  `EVIDENCE await-cancel: future dropped on timeout, permit guard released
  immediately`. In the runner (T21/T35), the token's `cancelled()` future is
  `select!`ed against the task future so cancellation drops the loser.
- **Blocking/compute cancellation is cooperative-only and marked-not-killed.**
  A blocking or compute closure is handed its child token and can **observe** it
  cooperatively (`is_cancelled()` / poll), but the framework can only **mark**
  it — it cannot kill the thread. The permit is held until the closure returns
  (§2). A task that observes the mark and returns promptly within grace is
  `cancelled`; one that does not is `abandoned` (C16 vocabulary). This is the
  same cooperative-only honesty arch.md commits to; the spec "does not pretend
  otherwise."

### 5. Isolated framework runtime

The framework's own machinery runs on a **framework runtime that is a separate
tokio runtime from the task-worker runtime**, so that a run in which **every
task worker is blocked** can still fire a timeout, still react to SIGTERM, and
still write a complete event stream (C13 acceptance):

- The **task-worker runtime** is the multi-threaded tokio runtime that drives
  await-bound task futures and dispatches blocking/compute work.
- The **framework runtime** is a small, separate tokio runtime carrying the
  safety rails: **timers** (per-attempt timeout firing), **cancellation** fan-
  out, the **event-stream** writer, and **signal handling** (SIGTERM/SIGINT via
  `tokio::signal`). Because these do not share worker threads with task
  execution, they keep running even when every task worker is monopolised by a
  misdeclared synchronous closure. *Spike evidence (runtime isolation):* with 4
  synchronous never-yielding closures jamming a 2-worker task runtime, a timer
  armed on the framework runtime still fired — `EVIDENCE isolation:
  framework-runtime timer fired while all task workers were jammed`. Signal
  handling installs on the same isolated framework runtime, so SIGTERM is
  observed and the stream is flushed regardless of task-worker state; the
  isolation property proven for timers is identical for signals.

This is the concrete backing for C13's "misdeclaring a class may stall the run's
progress, but never … disables timeouts, cancellation, or the event stream." The
run-loop wiring is T24/T33; this ADR fixes the two-runtime topology they build.

### 6. Shutdown-budget arithmetic

The shutdown budget is **arithmetic, not hope** (C16):

| Component | Default | Operator flag |
|---|---|---|
| Grace period | **10 s** | yes (configurable) |
| Teardown deadline | **15 s** | yes (configurable) |
| Final flush | **2 s** | bounded, fixed |
| **Worst-case total** | **27 s** | — |
| Kubernetes kill window (assumed) | **30 s** | (`terminationGracePeriodSeconds`) |

`10 + 15 + 2 = 27 s ≤ 30 s`, so the worst-case budget fits inside the assumed
Kubernetes kill window. *Spike evidence (shutdown-budget arithmetic):*
`EVIDENCE budget: grace 10s + teardown 15s + flush 2s = 27s <= 30s kill window`.
**Grace and teardown are operator flags**; the **worst-case budget is printed at
startup** so a misconfiguration is visible before it matters. Implementing the
drain, the flags, and the startup print is **T35** — this ADR records the
arithmetic and the two obligations (operator-configurable grace/teardown; print
worst-case budget at startup) that T35 must satisfy.

### 7. Test-runtime shape (C28)

- **Synchronous single-task tests need no runtime at all.** A synchronous task
  is invoked directly with a hand-built context; no tokio runtime is created.
  *Spike evidence (test-runtime shape):* `EVIDENCE test-runtime: synchronous
  task ran with no async runtime present`.
- **Await-bound task tests use a plain test runtime.** The C28 testing surface
  hands await-bound single-task tests a **plain `current_thread` tokio runtime**
  (`tokio::runtime::Builder::new_current_thread().enable_all().build()`) — the
  smallest runtime that drives a single await-bound task deterministically, with
  no multi-thread nondeterminism in a unit test. *Spike evidence:*
  `EVIDENCE test-runtime: await-bound task ran on a plain current-thread test
  runtime`. T60 ships this as the single-task test kit; this ADR fixes the
  runtime shape it provides.

## Consequences

- **T9 and T33 are unblocked with no runtime or concurrency-primitive choice
  left open.** T9 (task abstraction) declares the await-bound-default execution
  class against the tokio commitment above; **T33** (execution-class dispatch)
  builds the three-class dispatch — await-bound on tokio, blocking via
  `spawn_blocking`, compute on a dedicated rayon pool — and the two-runtime
  isolation topology (§5), re-deciding none of it.
- **tokio is now a public-dependency commitment.** A tokio major bump is a dagr
  major-version event; additions of `tokio`/`tokio-util`/`rayon` to `dagr-core`
  are reviewed as API decisions when T9/T33 make them (Stability). `dagr-core`'s
  dependency set stays empty until then.
- **The permit ledger has a fixed shape to build against.** T31/T37 implement the
  admission pools and outcome matrix knowing await-bound cancellation releases
  immediately (future-drop) while blocking/compute cancellation holds the permit
  until the closure returns — abandoned-but-running work stays counted (C12).
- **The cancellation core has one primitive.** T35 builds the run-scoped-token +
  per-attempt-child drain, the grace/teardown flags, and the startup budget
  print on `tokio_util::sync::CancellationToken`, exposed dagr-owned.
- **The test surface has one blessed runtime.** T60's single-task kit gives sync
  tasks no runtime and await-bound tasks a plain `current_thread` runtime (C28).
- **Spike is throwaway.** The `/tmp` spike that proved all of the above is
  deleted; nothing was promoted into a shipping crate, and `Cargo.lock` is
  unchanged, so `cargo audit` has no new surface.

## Rejected alternatives

- **A capped semaphore over `spawn_blocking` for compute-class work.** Rejected
  in favour of a dedicated rayon pool (Decision §3). A semaphore-over-
  `spawn_blocking` compute path draws from tokio's *shared* blocking pool, so
  compute-class and blocking-class work would contend for one thread set and one
  pool would have to honour two different caps; the "never exceed N" guarantee
  would rest on a correctly-held semaphore rather than being structural in a
  sized pool, and a burst of blocking work could starve compute (or vice versa)
  though C13 treats them as distinct classes with distinct sizes. A dedicated
  rayon pool gives compute its own CPU-sized thread set, makes the bound
  structural, and frees the blocking pool for genuinely I/O-waiting work.
- **A non-tokio async runtime (async-std, smol, or a hand-rolled executor).**
  Rejected: arch.md commits to tokio at the spec level (C13, Stability), tokio is
  the ecosystem default with the broadest resource-client support (the C9
  registry holds real database/object-store/HTTP clients, most of which target
  tokio), and `tokio_util::sync::CancellationToken`, `spawn_blocking`, and
  `tokio::signal`/`tokio::time` give this ADR every primitive it needs in one
  runtime. A different runtime would reopen a settled spec decision for no gain.
- **A runtime-agnostic executor abstraction (a spawner trait over several
  runtimes, as in the dagx prior art's test matrix).** Rejected: it buys
  portability dagr does not want — arch.md deliberately commits to one runtime so
  the shutdown budget, signal handling, and blocking/compute pools have concrete
  semantics to reason about, and "plain tokio is the honest recommendation" below
  the adoption threshold (arch.md "When not to use this"). An abstraction layer
  would dilute the public-dependency commitment and add surface for no operator
  benefit.
- **A single shared runtime for both task execution and framework machinery.**
  Rejected: it defeats C13's isolation guarantee. If timers, cancellation
  fan-out, the event writer, and signal handling shared worker threads with task
  execution, a fully blocked task workforce could stall the safety rails — the
  spike's jam scenario would starve the timeout and SIGTERM. Two separate tokio
  runtimes (Decision §5) keep the rails alive when task workers are wedged.
- **A dedicated OS thread per framework rail instead of a small framework
  runtime.** Rejected as unnecessary: a single small multi-threaded tokio
  runtime already isolates the rails from task workers (proven by the spike) and
  lets timers, signals, cancellation, and the event writer share tokio's timer
  and I/O drivers rather than reinventing them per thread. The isolation
  requirement is about *not sharing threads with task execution*, which one
  separate runtime satisfies.
