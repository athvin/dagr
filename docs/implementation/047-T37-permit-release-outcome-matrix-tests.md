# 047 · T37 — C12: permit-release outcome matrix tests

> **Milestone:** M2 · **Size:** M · **Type:** feature (tests) · **Components:** C12, C14
> **Branch:** `feat/t37-permit-release-outcome-matrix-tests` · **Depends on:** T21, T23, T31, T35 · **Blocks:** T38

## Why / context
The admission controller (C12) is the component that turns a memory ceiling into a throughput limit instead of a crash, and it is honest only if its permit ledger never lies — including the honest exception where timed-out or cancelled-but-still-running work stays counted against the pools until the closure actually returns. C12 and C14 each list per-outcome acceptance criteria ("permits are released on success, permanent failure, retry-eligible failure, and cooperative cancellation, and for timeout and abandonment only when the underlying work has actually returned — each verified by a test that induces that specific outcome"), but those criteria are only met once a test exists that *induces each specific outcome* and asserts the ledger and the C10 slot residency behave correctly. This ticket writes that outcome matrix. It builds on the permit lifecycle from T31, the per-class timeout from T21, panic containment from T23, and the cancellation/grace machinery from T35; it is a prerequisite for the M2 overcommit-and-clean-stop demo (T38).

## Objective
Write a test suite that drives one node (and, where needed, a producer/consumer pair) through **every** terminal outcome and asserts the admission ledger and slot accounting stay truthful throughout. Concretely:

- Build a deterministic test rig using the pinned-capacity mechanism (C12's operator pin flag, the CI determinism lever) so pool sizes are exact and reproducible, and a way to observe the live ledger of each pool (memory, threads) at the moment of each outcome.
- Induce each of these outcomes with a purpose-built task: **success**, **permanent failure**, **retry-eligible failure**, **timeout in the await-bound class**, **timeout in the blocking/compute class**, **panic**, **cooperative cancellation**, and **abandonment** (asked to cancel, never returns within grace).
- For every outcome, assert the invariant from C12: the combined declared cost of executing nodes — *including abandoned-but-running work* — never exceeds any pool's capacity, and the permit is released exactly according to the outcome's rule (immediately on success / permanent failure / retry-eligible failure / cooperative cancellation / await-bound timeout; held until closure return on blocking/compute timeout and on abandonment).
- Assert the C10 companion invariant: a slot whose value is pinned by a zombie (abandoned-but-running or blocking-timed-out) consumer stays counted against the memory pool until that consumer's closure returns, and only then is the residency reclaimed.
- Cover the "no double state" boundary from C14/Vocabulary: a blocking timeout stays `timed-out` (never flips to `abandoned`), and its permit accounting reflects that single decided state.

## Test plan (write these first — TDD)
Each scenario uses a rig with pinned pool capacities and a ledger observer. Unless stated, use a one-pool-unit-per-node cost so admission and release are individually observable.

- **Success releases immediately.** Setup: a single node whose task returns a value, pool capacity pinned to exactly its cost. Action: run to completion and sample the ledger right after the node reaches `succeeded`. Expected: the node's permit is released, the pool shows full capacity again, and the ledger never exceeded capacity at any sampled point.

- **Permanent failure releases immediately.** Setup: a node whose task returns a permanent (non-retry-eligible) error, no retries left to matter. Action: run and sample the ledger after the node reaches `failed`. Expected: permit released immediately, pool restored, invariant never violated, and the node is not retried.

- **Retry-eligible failure releases between attempts and re-acquires.** Setup: a node with a retry budget whose task returns a retry-eligible error on the first attempt and succeeds on the second. Action: sample the ledger after the first attempt's failure (before backoff), during backoff, and after the successful second attempt. Expected: the permit is released after the failed attempt (pool free during backoff), re-acquired for the retry, and released again on success; capacity is never exceeded, and the two attempts never hold two permits at once.

- **Await-bound timeout cancels and releases immediately.** Setup: an await-bound node whose future never completes, with a short per-attempt timeout, retries disabled so the outcome is terminal `timed-out`. Action: run and sample the ledger the instant the timeout is recorded. Expected: the future is dropped, the permit releases immediately (pool free), the node's state is `timed-out`, and the invariant held throughout.

- **Blocking/compute timeout keeps the permit until the closure returns.** Setup: a blocking-class node whose synchronous closure sleeps past its timeout on a controllable gate, timeout short, retries disabled. Action: sample the ledger (a) immediately after the timeout is recorded while the closure is still gated, then release the gate and sample again after the closure returns. Expected: at (a) the node is already decided `timed-out` and the event is emitted, but the permit is still counted (pool NOT free — abandoned-but-running cost held); after the closure returns the permit releases and the pool is restored. The node's state stays `timed-out` — it never becomes a second state.

- **Blocking timeout retry is deferred until the closure returns.** Setup: same blocking node but with one retry allowed; the first closure is gated past its timeout, the second would succeed. Action: keep the first closure gated and observe whether a second attempt starts; then release the gate and observe. Expected: no second attempt (and no second permit acquisition) starts while the first closure is still running; the retry begins only after the first closure returns, so the same task instance never runs concurrently with its own zombie, and the ledger never counts two live permits for the node at once.

- **Panic releases immediately as permanent failure.** Setup: a node whose task panics; the binary is built under the required (non-abort) unwind profile. Action: run and sample the ledger after the node reaches `failed`. Expected: the panic is caught and attributed, the node fails permanently, the permit releases immediately, the pool is restored, the rest of a co-scheduled unrelated node proceeds, and the invariant held.

- **Cooperative cancellation releases immediately.** Setup: a node that observes the cancellation signal and returns promptly, with cancellation triggered mid-run (e.g. by a sibling failure under stop-on-first-failure, or a direct token trip). Action: sample the ledger after the node is recorded `cancelled`. Expected: state is `cancelled` (distinct from `failed`), the permit releases immediately on return, and the pool is restored.

- **Abandonment holds the permit until the closure returns (or process exit).** Setup: a node whose synchronous closure ignores the cancellation signal and stays gated past the grace period; cancellation triggered mid-run. Action: sample the ledger (a) after grace expires while the closure is still gated, then release the gate and sample again. Expected: at (a) the node is recorded `abandoned` (distinct from `failed` and `cancelled`) yet its permit is still counted against the pool (the ledger counts zombies); after the closure returns, the permit is released and the pool is restored. Capacity is never exceeded at any sample.

- **Zombie consumer pins slot residency (C10 cross-check).** Setup: a producer node that fills a slot with a declared output residency, and a single consumer of that slot whose blocking closure reads the value and then is timed out (or abandoned) while still gated. Action: sample the memory pool / slot residency (a) after the consumer is decided and the producer's final consumer count would otherwise allow release, while the consumer closure is still gated, then release the gate and sample again. Expected: at (a) the slot's value residency stays counted against the memory pool because the zombie consumer still holds read access; only after the consumer closure returns is the value unreachable and its residency reclaimed to the allocator. The memory pool never regains capacity for bytes a leftover thread still pins.

- **Capacity invariant across the whole matrix.** Setup: a run that schedules several of the above nodes together against a pinned capacity that is exactly saturated by the intended concurrent set. Action: continuously sample the summed declared cost of all executing-and-abandoned nodes across the run. Expected: at no sampled instant does the summed cost — including any abandoned-but-running or blocking-timed-out node still counted — exceed any pool's capacity.

- **Every outcome emits exactly one attempt-outcome record.** Setup: reuse each induced-outcome node above and walk the event stream. Action: count `attempt-outcome` records per attempt, including the timed-out, panicked, and abandoned cases. Expected: exactly one attempt-outcome record per attempt, and (for zombie cases) a zombie/leftover-thread event where the taxonomy requires it — with the outcome state matching the ledger-release behaviour asserted above.

## Definition of done
- [ ] A test exists that induces **success** and asserts the permit releases immediately and the pool is restored.
- [ ] A test exists that induces **permanent failure** and asserts the permit releases immediately, with no retry.
- [ ] A test exists that induces **retry-eligible failure** and asserts the permit is released between attempts, re-acquired for the retry, and never doubly held.
- [ ] A test exists that induces an **await-bound timeout** and asserts the future is dropped and the permit releases immediately.
- [ ] A test exists that induces a **blocking/compute timeout** and asserts the node is recorded `timed-out` immediately while the permit is held until the closure actually returns, then released.
- [ ] A test asserts a **blocking-timeout retry is deferred** until the previous attempt's closure returns, so the task instance never runs concurrently with its own zombie.
- [ ] A test exists that induces a **panic** and asserts it becomes a permanent failure of only that node, the permit releases immediately, and the rest of the run proceeds.
- [ ] A test exists that induces **cooperative cancellation** and asserts the node is `cancelled` (distinct from `failed`) and the permit releases on return.
- [ ] A test exists that induces **abandonment** and asserts the node is `abandoned`, its permit stays counted while the closure runs past grace, and releases only when the closure returns.
- [ ] Across every induced outcome, a test asserts the combined declared cost of executing nodes — **including abandoned-but-running work** — never exceeds any pool's capacity (C12 capacity invariant, honest zombie accounting).
- [ ] A test asserts that a **slot value pinned by a zombie consumer** stays counted against the memory pool until that consumer's closure returns, and its residency is reclaimed to the allocator only afterward (C10 cross-check).
- [ ] A test asserts a blocking timeout's terminal state stays `timed-out` and never flips to a second terminal state (`abandoned` arises only on the cancellation path).
- [ ] A test asserts **exactly one attempt-outcome record** per attempt across the matrix, including timed-out, panicked, and abandoned attempts, with the zombie/leftover-thread event present where required.
- [ ] Pool capacities in the suite are made deterministic via the C12 pin flag/mechanism, and the ledger is observable at each outcome without racing the outcome it measures (deterministic gates, not sleeps).
- [ ] The suite lives with the framework's own tests (per C28's self-testing convention) and needs no live network, database, or infrastructure.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
None.

## Out of scope
- Implementing or changing the permit lifecycle, the per-class timeout, panic containment, or the cancellation machinery — those land in T31, T21, T23, and T35 respectively; this ticket only *tests* their composed behaviour and may only fix defects the tests surface in the honesty invariant.
- Container limit detection and pool sizing from cgroups/host (T32); this suite pins capacity outright rather than probing it.
- The M2 overcommit-and-clean-stop demo (T38) and the run-artifact juxtaposition of declared-vs-measured cost (C12's reporting, exercised elsewhere) beyond what an outcome assertion needs.
- Trigger-rule propagation and failure-mode selection (C15/T34) except as a means to trigger cancellation for the cooperative-cancellation and abandonment cases.
- Any temptation to make the ledger "smarter" about reclaiming zombie capacity early, to kill blocking closures, or to coordinate capacity across processes — dagr is not a scheduler or a distributed execution system, and the ledger's honesty about un-killable work is the whole point being tested here.
