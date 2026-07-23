# 027 · T17 — C10: output slots

> **Milestone:** M1 · **Size:** M · **Type:** feature · **Components:** C10
> **Branch:** `feat/t17-output-slots` · **Depends on:** T14, T0.2 · **Blocks:** T20, T26

## Why / context
Every node needs somewhere to put the value it produces and a disciplined way for downstream nodes to read it. C10 (arch.md `### C10 · Output slot`, plus the residency/zombie clauses at the "Declared cost" paragraph and C12/C14 zombie-accounting rules) defines that place: one typed, once-writable slot per node, wired to its consumers at assembly time so reads need no lookup and no runtime type check. This ticket builds on the assembled graph and consumer-count facts from T14 (C7) and on the ownership/sharing model locked by the T0.2 ADR (sole-consumer-owns / shared-read / per-edge clone-on-read). It is the storage substrate that the attempt runner (T20) fills and that the bounded-memory chain test (T26) exercises, so it must be correct before either lands. The load-bearing subtlety amended into C10: the slot releases only when every consumer is terminal **and** every consumer closure has actually returned, so a zombie (abandoned-but-running) consumer pins both the value and its counted residency lease.

## Objective
Build the output-slot substrate for C10.

- A typed, once-writable slot per node, empty until the producing node succeeds, holding exactly that node's declared output type.
- Assembly-time consumer references: each downstream consumer holds a direct reference to its upstream slot, established during assembly (from T14), so a read is a direct access with no map lookup and no runtime type check.
- A resolved type-erasure strategy for storing heterogeneous slots together while keeping reads lookup-free and type-check-free (answer the Open question and record the decision in the module rustdoc).
- Consumer accounting: each slot tracks remaining consumers and releases the value only when **every** consumer has reached a terminal state **and** every consumer closure has returned. A shared-access consumer that reads and then retries still finds the value present.
- Zombie pinning: an abandoned-but-running consumer keeps its read access, so the value stays reachable and its residency stays counted until that closure returns.
- A `retained` flag: retained nodes keep their value until run end, and the value is redeemable by the embedding program via a post-run redemption API (a handle exchanged for the value once the run has ended); non-retained released values are not redeemable.
- Slot-lease accounting hooks: the producer's declared output residency transfers into the slot when the value is produced and is released (once) only at actual slot release, so a zombie never lets the pool regain capacity for bytes it still pins. Expose the hooks the memory pool (C12) and run artifact (C23) consume, including peak measured residency.
- A loud read-before-fill defect: reading an unfilled slot is a framework defect that fails loudly and names the offending node.

## Test plan (write these first — TDD)

- **Read before fill is a loud, node-named defect.** Setup: a slot for a named node, never filled. Action: attempt to read it. Expected: the read fails loudly (framework-defect path, not a task error), and the failure message contains the exact node identity.
- **Fill once, read the value.** Setup: an empty slot typed to a node's output. Action: fill it with a produced value, then read through a consumer reference. Expected: the read returns the produced value.
- **Second fill is rejected.** Setup: a slot already filled with a value. Action: attempt to fill it again. Expected: the second fill is refused (once-writable invariant); the original value is unchanged.
- **Shared consumer reads, then retries, and still finds its input.** Setup: a slot filled with a value and a single shared-access consumer whose first attempt reads the value and then fails retry-eligibly. Action: run the consumer's next attempt. Expected: the value is still present and readable on the retry; the slot is not released between attempts.
- **Release fires only after every consumer is terminal AND has returned.** Setup: a slot with two consumers. Action: drive one consumer to a terminal state while the other is still in flight, then read residency/release status; then drive the second consumer terminal and let its closure return. Expected: the value stays reachable and residency stays counted while any consumer is not yet terminal-and-returned; only after the last consumer is terminal and its closure has returned does the slot release and its memory return to the allocator.
- **Release waits on last read, not last terminal-signal.** Setup: a slot whose consumers reach terminal states but where one consumer closure returns strictly later than its terminal decision. Action: observe release timing relative to the two events. Expected: release is gated on the closure return, not on the terminal-state decision — the value is not reclaimed in the window between them.
- **Zombie consumer pins value and residency.** Setup: a slot with a consumer marked timed-out/abandoned whose closure has not returned. Action: check whether the value is reachable and whether its residency is counted. Expected: the value remains reachable and its residency remains counted against the memory pool while the closure runs; both release only once the closure returns.
- **Retained value survives to run end and is redeemable.** Setup: a node marked `retained` with all consumers terminal-and-returned. Action: after the run has ended, exchange the handle for the value via the redemption API. Expected: the value is still present and is returned by redemption; its residency was counted through run end.
- **Released value is not redeemable.** Setup: a non-retained node whose value was released after its last consumer returned. Action: attempt post-run redemption for that node. Expected: redemption reports no value available (released values are not redeemable), distinct from the read-before-fill defect.
- **Residency is counted once, not per consumer.** Setup: a filled slot with several consumers. Action: read the counted residency for that value. Expected: the declared output residency is counted exactly once for the value regardless of consumer count, transferred from the producing attempt at fill time.
- **Accounting hooks expose peak residency.** Setup: a run (or harness) that fills and releases several slots over time with an instrumentable residency probe. Action: read the peak-residency hook after activity. Expected: peak measured slot residency is reported and matches the maximum concurrent counted residency observed.
- **No lookup, no runtime type check on the read path.** Setup: two slots of different output types wired to their consumers at assembly. Action: exercise both consumer reads. Expected: each read yields the correctly typed value with no key/map lookup and no runtime type-tag check on the hot read path (verified by construction/design test asserting the read goes through a direct reference; a mismatched-type wiring is impossible to construct, not caught at read time).
- **Chain peak does not grow with chain length (bounded-memory smoke).** Setup: a synthetic long linear chain where each node's value is consumed by exactly one downstream node and nothing is retained. Action: run it and sample allocator-level residency. Expected: peak allocator-level residency stays bounded and does not scale with chain length. (The authoritative hundred-node assertion lives in T26; this ticket carries a smaller smoke version to protect the release logic.)

## Definition of done
- [ ] Each node owns exactly one slot, typed to that node's output, empty until the node succeeds; a second fill of a filled slot is refused (once-writable).
- [ ] Downstream consumers hold direct references to their upstream slots, established at assembly time (from T14); a read requires no lookup and no runtime type check.
- [ ] A type-erasure strategy for heterogeneous slot storage is chosen and documented in module rustdoc, and it keeps reads lookup-free and type-check-free (Open question answered).
- [ ] Reading a slot before it is filled is a framework defect that fails loudly and names the node.
- [ ] A shared-access consumer that reads a value and then retries finds the value still available on its next attempt.
- [ ] A slot releases its value only after **every** consumer has reached a terminal state **and** every consumer closure has returned; release is gated on closure return, not on the terminal-state decision.
- [ ] While an abandoned-but-running (zombie) consumer's closure is still running, the value stays reachable and its residency stays counted against the memory pool; both release only when that closure returns.
- [ ] After the final consumer reaches a terminal state and every consumer closure has returned, a non-retained value is unreachable and its memory is returned to the allocator.
- [ ] The producer's declared output residency transfers from the producing attempt into the slot when the value is produced, is counted exactly once per value (not once per consumer), and is released exactly once at actual slot release.
- [ ] Slot-lease accounting hooks are exposed for the memory pool (C12) and the run artifact (C23), including peak measured slot residency.
- [ ] Nodes marked `retained` keep their value until run end; the value is redeemable after the run has ended via the post-run redemption API (handle exchanged for value), and its residency is counted through run end.
- [ ] A released (non-retained) value is not redeemable, and this is distinguishable from a read-before-fill defect.
- [ ] Peak allocator-level residency across a linear chain with nothing retained does not grow with chain length (smoke-level here; hundred-node authority is T26).
- [ ] Every scenario in the Test plan is implemented and passing.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
- Type-erasure strategy for heterogeneous slot storage that keeps reads lookup-free and type-check-free. (Resolve within this ticket; record the decision and its safety argument in module rustdoc.)

## Out of scope
- Filling the slot from a real attempt outcome and emitting attempt records — that is the attempt runner (T20, C14); this ticket exposes the fill/read/release surface it drives.
- The authoritative hundred-node bounded-memory assertion with the instrumented allocator — that is T26; only a smaller smoke test lives here.
- Timeout classification, abandonment decisions, and permit release timing — those are C14/C12 (T21, and the T0.3 spike); this ticket only honours the residency/reachability pinning that a zombie consumer imposes.
- Deriving or enforcing memory-pool capacity or admission ordering — that is the admission controller (C12); this ticket only exposes the residency accounting hooks it consumes.
- Assembling the graph, exact-type dependency binding, and consumer-count/ownership-mode validation — those belong to C3/C7 (T11, T14, T0.2); this ticket consumes the already-validated wiring and counts.
- Rendering the run artifact and its summary numbers — that is C23; this ticket only surfaces peak-residency and retained-value data for it to fold.
- Durable/addressable outputs and rehydration — that is C27; in-memory slots deliberately cannot be rehydrated, and this ticket does not add durability.
- No metadata store, scheduler, or runtime graph reshaping: slots are per-node and fixed at assembly; nothing here lets the graph shape change at runtime.
