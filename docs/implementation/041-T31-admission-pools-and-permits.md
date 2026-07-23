# 041 · T31 — C12: admission pools and permit lifecycle

> **Milestone:** M2 · **Size:** M · **Type:** feature · **Components:** C12
> **Branch:** `feat/t31-admission-pools-and-permits` · **Depends on:** T24, T29, T0.3 · **Blocks:** T32, T37, T42

## Why / context
The admission controller is what turns a memory ceiling into a throughput limit instead of a crash — it is the primary lever on infrastructure cost, and it is the component that keeps the pipeline from exceeding the machine it runs on. This ticket builds the production permit system: weighted capacity pools, all-or-nothing multi-pool acquisition, oldest-ready-first admission with bounded bypass, and a ledger that stays honest while abandoned-but-running work still pins its cost. It implements against the model locked by the T0.3 ADR (timeout abandonment and permit accounting), draws declared-cost vectors from C5 node policy (T29), and is driven by the M1 run loop (T24). It is governed by arch.md **§C12 · Admission controller**, with the working-memory / output-residency split and the slot-lease charging rule coming from **§C5 · Node policy** and **§C10 · Output slots**. Pool *sizing* from container limits is deliberately deferred to T32; this ticket takes pool capacities as an input (pinned for tests) and owns everything from acquisition through release.

## Objective
Build the runtime admission controller that admits ready nodes against weighted capacity pools and holds each permit across its whole attempt, releasing it exactly when the underlying work is truly done.

- Implement **weighted capacity pools**, at minimum a memory pool (native unit: bytes) and thread pools (native unit: thread count), each holding a total capacity and a live remaining capacity.
- Implement **all-or-nothing multi-pool acquisition**: a node is admitted only when its declared cost fits the remaining capacity of *every* pool it needs, and no pool's capacity is held while waiting on another (the deadlock the atomicity prevents).
- Implement **oldest-ready-first admission with bounded bypass**: a small node may jump the queue only when admitting it cannot delay the current oldest waiter, so a large node behind a stream of small ones is never starved.
- Hold the **permit for the whole attempt** and release it on every terminal outcome — success, permanent failure, retry-eligible failure, and cooperative cancellation.
- Keep **zombie (abandoned-but-running) cost counted** against every pool it drew from until the closure actually returns, observing that return via the mechanism the T0.3 ADR named — never releasing what is still running.
- Implement the **working-memory vs output-residency split** from C5: working memory is held for the attempt and released at its terminal state; output residency **transfers** from the producing attempt to the output slot when the value is produced and is charged (the **slot lease**) until the slot *actually* releases — which, per C10, waits for zombie consumers to return.
- Record **permit-wait time separately from execution time**, so the waiting phase and the executing phase are distinguishable per attempt.
- Emit a **warning for undeclared-cost nodes in a memory-constrained run** (a node with no declared memory cost when the memory pool is a real constraint).
- Surface **declared cost** in a form the run artifact can juxtapose against measured cost (the reporting seam C23/T42 folds), including the per-pool zombie cost currently pinned.
- Expose the ledger operations the T0.3 ADR named as the seam for T37's outcome-matrix tests and T42's artifact folding, keeping the admission controller's machinery isolated from task execution (the safety-rail isolation C13 requires).

## Test plan (write these first — TDD)
Each scenario pins pool capacities so admission is deterministic (the T32 flag is not exercised here). "Ledger" means the live remaining-capacity accounting across all pools. Every test is independently checkable.

- **A node that fits every pool is admitted immediately.** *Setup:* pools pinned so their remaining capacity comfortably exceeds one node's declared per-pool cost. *Action:* present that ready node for admission. *Expected:* it is admitted at once, and each pool's remaining capacity drops by exactly that node's declared cost for that pool.

- **A node is admitted only when it fits *every* pool it needs.** *Setup:* two pools pinned so the node fits the first but its cost exceeds the second's remaining capacity. *Action:* present the node. *Expected:* it is not admitted; it waits; and — critically — the first pool's capacity is *not* consumed while it waits on the second (no partial hold).

- **Multi-pool acquisition is atomic and two contending nodes do not deadlock.** *Setup:* two ready nodes, each declaring cost on the same two pools, pinned so only one can be admitted at a time. *Action:* present both concurrently and let each complete when admitted. *Expected:* the run makes progress — one is admitted, runs, releases, then the other is admitted; at no point does one hold pool A while blocking on pool B while the other holds pool B while blocking on pool A. The test times out as a failure if either stalls.

- **Combined counted cost never exceeds capacity, including a live zombie.** *Setup:* the memory pool pinned to admit exactly one node of a given declared cost. *Action:* admit a blocking node, time it out so it becomes abandoned-but-running (a live zombie), then present a second ready node of the same cost while the zombie's closure has not yet returned. *Expected:* the second node is not admitted until the zombie's closure returns; at every instant the sum of counted cost across executing nodes *and* the zombie is at most the pinned capacity.

- **A large node behind a stream of small nodes is eventually admitted (no starvation).** *Setup:* pool capacity that fits several small nodes at once but only one large node; a large node made ready first, then a continuous stream of small ready nodes arriving behind it. *Action:* run the stream while the large node waits. *Expected:* small nodes bypass only while doing so cannot delay the large (oldest) waiter; the large node is admitted within a bounded number of admission decisions rather than being indefinitely postponed by the stream.

- **Bounded bypass never delays the oldest waiter.** *Setup:* one large node waiting (the oldest waiter) plus a small node that would fit in the currently free capacity. *Action:* offer the small node while the large one waits. *Expected:* the small node is admitted only if admitting it leaves enough capacity path for the large node to still be admitted no later than it otherwise would have been; when admitting the small node *would* push out the large node's admission, the small node is held instead.

- **Permit releases on success.** *Setup:* an admitted node whose attempt succeeds. *Action:* let it complete. *Expected:* its working-memory cost is returned to the pools when the attempt reaches its terminal success state; the ledger returns to its pre-admission level for working memory (output residency handled separately, below).

- **Permit releases on permanent failure.** *Setup:* an admitted node whose attempt is a permanent failure. *Action:* let it fail. *Expected:* the permit releases at the terminal failure; remaining capacity is restored.

- **Permit releases on retry-eligible failure.** *Setup:* an admitted node whose attempt is a retry-eligible failure. *Action:* let the attempt fail. *Expected:* the permit releases when that attempt reaches its (non-terminal-for-the-node but terminal-for-the-attempt) failure; the next attempt re-acquires admission fresh rather than inheriting a held permit.

- **Permit releases on cooperative cancellation.** *Setup:* an admitted await-bound node under a run that is cooperatively cancelled. *Action:* cancel. *Expected:* the future is dropped and the permit releases immediately; remaining capacity is restored at once.

- **Permit for a timed-out blocking attempt is held until the closure returns.** *Setup:* an admitted blocking node whose closure sleeps well past its timeout. *Action:* fire the timeout, observe the ledger, then let the closure return and observe again. *Expected:* immediately after timeout the node is marked timed out and the ledger still counts the full declared cost (one zombie present); only after the closure actually returns does the ledger release that cost and drop the zombie count to zero.

- **Working memory and output residency are charged separately.** *Setup:* a producing node declaring both a working-memory cost and an output-residency cost. *Action:* run it to success and observe the memory pool at production and at terminal state. *Expected:* working memory is charged on admission and released at the attempt's terminal state; output residency transfers from the attempt to the node's output slot when the value is produced and is *not* released at the attempt's terminal state — it remains charged as a slot lease.

- **Slot lease is held until the slot actually releases.** *Setup:* a produced value with two downstream consumers, one of which is timed out as a blocking zombie after reading the value. *Action:* let the healthy consumer finish, then hold at the zombie, then let the zombie's closure return. *Expected:* the output-residency cost is *not* reclaimed while any consumer's work has not returned; the memory pool regains that capacity only when the slot actually releases (per C10, after the zombie consumer returns), so bytes a leftover thread still pins are never counted as reclaimed.

- **A retained output is charged until run end.** *Setup:* a node marked retained (C5/C10) producing a value with a nonzero output residency. *Action:* run past all its consumers' terminal states to run end. *Expected:* the output-residency cost stays charged until the run ends, never reclaimed mid-run.

- **Permit-wait time is recorded separately from execution time.** *Setup:* a node made to wait a measurable interval for capacity before admission, then executing for a measurable interval. *Action:* run it and read the recorded phases. *Expected:* the recorded permit-wait duration and the recorded execution duration are distinct fields, each reflecting the correct interval; a node admitted immediately shows a near-zero wait.

- **An undeclared-cost node warns in a memory-constrained run.** *Setup:* a memory pool that is a genuine constraint and a node with no declared memory cost. *Action:* run it. *Expected:* a warning is emitted naming that node's missing cost declaration; the warning fires only when the memory pool is constrained, not for an unconstrained run.

- **Declared cost is exposed for the artifact juxtaposition.** *Setup:* a node with a known per-pool declared cost, plus a live zombie pinning a known per-pool cost. *Action:* query the admission controller's reporting seam. *Expected:* it reports each node's declared per-pool cost and the current per-pool zombie cost in the shape T42/C23 can fold side by side with measured cost — no measured-vs-declared comparison is computed here, only the declared side is surfaced.

## Definition of done
- [ ] Weighted capacity pools exist for memory (bytes) and threads (thread count) at minimum, each tracking total and live remaining capacity (C12).
- [ ] A ready node is admitted only when its declared cost fits the remaining capacity of *every* pool it needs (C12).
- [ ] Multi-pool acquisition is all-or-nothing: no pool's capacity is held while waiting on another, and a test with two contending multi-pool nodes proves no deadlock (C12 acceptance criterion).
- [ ] Admission order is oldest-ready-first with bounded bypass; a large-cost node ready behind a stream of small nodes is eventually admitted, verified by a starvation test (C12 acceptance criterion).
- [ ] The bounded bypass admits a small node only when doing so cannot delay the oldest waiter (C12).
- [ ] The permit is held for the whole attempt and released on success, permanent failure, retry-eligible failure, and cooperative cancellation — each verified by a test that induces that specific outcome (C12 acceptance criterion).
- [ ] For timeout and abandonment, the permit releases only when the underlying work has actually returned; abandoned-but-running cost stays counted against every pool it drew from until the closure returns, observed via the mechanism the T0.3 ADR named (C12 acceptance criterion).
- [ ] The combined declared cost of executing nodes — *including abandoned-but-running work* — never exceeds pool capacity, verified by a test that holds a live zombie against a pinned-to-one capacity (C12 acceptance criterion).
- [ ] Working memory is charged on admission and released at the attempt's terminal state; output residency transfers from the producing attempt to the output slot when the value is produced (C5, C10).
- [ ] The slot lease (output residency) is held until the slot *actually* releases — which waits for zombie consumers to return — so the pool never regains capacity for bytes a leftover thread still pins; a retained output stays charged until run end (C10).
- [ ] Time spent waiting for a permit is recorded separately from time spent executing (C12 acceptance criterion).
- [ ] A memory-constrained run warns about nodes with no declared cost, and the warning does not fire for an unconstrained run (C12).
- [ ] Declared per-pool cost (and current per-pool zombie cost) is surfaced through a reporting seam in the shape T42/C23 folds side by side with measured cost in the run artifact (C12 acceptance criterion).
- [ ] The admission controller's machinery is isolated from task execution so a misbehaving task cannot corrupt the ledger (consistent with C13's safety-rail isolation).
- [ ] The ledger operations relied on by T37 (outcome-matrix assertions) and T42 (artifact folding) are exposed as a stable seam, and the implementation matches the T0.3 ADR without re-litigating it.
- [ ] Public items carry rustdoc; the admission controller is driven by the T24 run loop and consumes C5 declared-cost vectors from T29 without duplicating their definitions.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
- Pools beyond memory and threads (memory and threads are the stated minimum) in scope for v1? Resolve toward the minimum: implement memory and thread pools now and keep the pool set open for extension, but do not add a third pool without a spec driver. Any decision must not open the door to a runtime-mutable pool set.

  **Resolved (T31, 041):** ships exactly the stated minimum — a single **memory** pool (bytes; both the working-memory and output-residency halves of the C5 cost draw from it) and the two **thread** pools from T2, **blocking** and **compute** (thread counts). These are the three `Pool` enum variants in `dagr_core::admission`. No fourth pool is added — there is no spec driver for one. The pool set is kept **open for extension** only in the compile-time sense: adding a pool is a source change to the `Pool` enum (a spec-driven decision), never a runtime knob — the enum is fixed at compile time and there is no API to add/remove a pool at runtime, so the door to a runtime-mutable pool set stays closed (a permanent non-goal). Capacities are taken as a pinned **input** (`PoolCapacities`); deriving them from container limits is deferred to T32.

- Where should a node whose declared cost **exceeds a pool's total capacity** (so it can *never* be admitted, even into an empty pool) be caught?

  **Resolved (T31, 041) — defensive driver-level guard only:** a can-never-fit node would otherwise wait in the driver's `pending` queue forever; when `in_flight` reaches 0 the run loop exits, leaving that node with **no** terminal state and reporting the run as complete — a silent violation of the "every reachable node reaches a terminal state" invariant. T31 closes that termination hole with the **minimum** guard: `AdmissionController::can_ever_fit` / `over_demand_reason` let the driver detect the over-demand and give the node a defined `Failed` terminal (carrying the honest reason "declared cost \<c\> exceeds \<pool\> capacity \<cap\>"), folding the run to a `Failed` outcome rather than a silent success. **The full bootstrap-time rejection of too-big nodes (with the resolved container-limit capacities, before any node runs) stays deferred to T32** — this ticket adds only the runtime driver-level guard so T31 never silently strands a node. (Also noted in code comments in `crates/cli/src/driver.rs` and `crates/core/src/admission.rs`.)

## Out of scope
- **Container limit detection and pool sizing (T32).** Deriving pool capacities from cgroup v2 → v1 → host, the 20% headroom default, the at-least-one-unit rule, unlimited-sentinel fallback, the pinning flag, and too-big-node rejection at bootstrap all belong to T32. This ticket takes capacities as an input and pins them for tests.
- **The permit-release outcome matrix suite (T37).** The exhaustive per-outcome permit-release matrix is that ticket's deliverable; here each outcome gets one representative test, and the ledger seam T37 asserts against is exposed.
- **Event-stream and run-artifact folding (T42/C22, C23).** This ticket surfaces the declared-cost reporting *shape*; folding declared against measured cost into the run artifact and emitting zombie-at-exit events (C19) is T42/C23's job.
- **Execution-class dispatch (T33/C13) and per-attempt timeout (T21/C14).** The controller consumes their outcomes (which class, whether an attempt timed out) but does not implement thread dispatch or the timeout mechanism; those are their own tickets and are already governed by the T0.3 ADR.
- **Cancellation-path abandonment (C16).** The cooperative-cancellation *release* is tested here; the `abandoned` terminal state and cancellation fan-out are C16's.
- **Any move toward a scheduler, cross-process capacity coordination, distributed execution, a metadata store, a runtime-mutable pool or graph set, backfill, or a DSL.** The honest response to an unkillable thread is to keep counting its cost, not to invent a reaper or coordinate capacity across processes — those cross dagr's permanent scope boundary.
