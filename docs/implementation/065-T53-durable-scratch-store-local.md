# 065 · T53 — C18: durable scratch store (local)

> **Milestone:** M4 · **Size:** M · **Type:** feature · **Components:** C18
> **Branch:** `feat/t53-durable-scratch-store-local` · **Depends on:** T16, T0.6 · **Blocks:** T54a

## Why / context
Tasks need to remember something across their own retries (and, later, across a resume): a cursor, a high-water mark, an "I already finished the first half" checkpoint. C18 · Durable scratch store (arch.md "C18 · Durable scratch store") supplies exactly that — a per-run, per-node key-value store of opaque bytes reached through the run context (C8, delivered by T16), physically living under the run store whose contract T0.6 fixed (`<base>/<pipeline>/<run-id>/`). This ticket builds the local scratch store and its lifecycle: namespaced writes, enforced cross-node isolation, deletion on node success, retention otherwise, and I/O-failure classification as retry-eligible. It deliberately stops short of the cross-restart durability proof (T54a) and resume carry-forward (T54b), which build on top of it.

## Objective
Build the durable scratch store as a per-run, per-node namespaced key-value store of opaque bytes, exposed through the run context and backed by the run store on local disk.

Concrete pieces of work:
- A scratch API on the run context: write a value for a key, read a value for a key (returning "absent" distinctly from an error), and remove a value — all keyed by opaque `byte`-string keys with opaque `byte`-string values.
- Physical layout under the run store: scratch for a node lives inside that run's directory (`<base>/<pipeline>/<run-id>/`), in a location namespaced by both run identity and node identity, derived only from the identities the context already carries.
- Namespacing that makes two nodes' key spaces disjoint, so identical keys written by different nodes never collide.
- Enforced (not conventional) cross-node isolation: the scratch handle a task receives can address only its own node's namespace; there is no API path by which a task names, reaches, or reads another node's scratch.
- Lifecycle hook invoked when a node reaches terminal success: that node's scratch is deleted. Scratch of nodes that did not succeed is left in place — no implicit deletion at run end.
- Failure classification: any read or write failure caused by the underlying store surfaces to the caller as a retry-eligible task failure (per C4 classification), never as a permanent failure or a panic.
- Documentation on the scratch API stating the intended-use guidance: opaque bytes with task-owned serialization; sized for kilobyte-scale values (no hard bound); for cursors/high-water-marks/checkpoints and explicitly not a channel for passing data between nodes (that is what data edges are for).

## Test plan (write these first — TDD)

- **Write-then-read within a namespace.** Setup: construct a scratch store for one node under a temp run-store base. Action: write a value under a key, then read that key back. Expected: the read returns exactly the bytes written, byte-for-byte.

- **Absent key is distinct from failure.** Setup: a scratch store for a node with nothing written. Action: read a key that was never written. Expected: the result is the well-defined "absent" outcome, distinguishable from an I/O error and not a task failure.

- **Value survives across attempts (the attempt-1-write / attempt-2-read case).** Setup: a node whose scratch store is reached through a hand-built run context (C8), configured to report attempt number 1. Action: on attempt 1 write a value under a key; then, simulating a retry, obtain the scratch store for the same node/run at attempt number 2 and read the key. Expected: attempt 2 reads exactly the value attempt 1 wrote.

- **Keys are namespaced by run and node — no collision between nodes.** Setup: two distinct node identities within the same run. Action: each node writes a different value under the *same* key string, then each reads that key back. Expected: each node reads its own value; neither sees the other's; the two writes did not overwrite one another.

- **Same key in different runs does not collide.** Setup: the same node identity under two different run identities sharing one run-store base. Action: each run writes a different value under the same key, then each reads it back. Expected: each run reads its own value; the two are isolated.

- **Cross-node read is impossible by construction (enforced isolation).** Setup: node A writes a value under a key; obtain node B's scratch handle. Action: attempt, through every API surface node B is given, to read A's value. Expected: there is no API by which B can name or reach A's namespace; a review-checkable test asserts B's handle can address only B's namespace and that reading A's key through B's handle yields "absent," never A's bytes.

- **Scratch of a succeeded node is deleted.** Setup: a node with scratch written under a temp run-store base. Action: invoke the on-success lifecycle hook for that node. Expected: subsequent reads of that node's keys return "absent," and the node's scratch storage location no longer exists on disk.

- **Scratch of a non-succeeded node is retained (nothing deleted implicitly).** Setup: two nodes with scratch written; node X reaches success, node Y reaches a non-success terminal state. Action: run the success hook for X only, then simulate run end (drop/close the store). Expected: X's scratch is gone; Y's scratch remains readable on disk after run end; no implicit end-of-run deletion touched Y.

- **Write failure classifies as retry-eligible.** Setup: a scratch store whose backing location is made unwritable (for example, an injected sink/store that returns an I/O error, or a path made read-only). Action: attempt a write. Expected: the caller receives a retry-eligible task failure (per C4), not a permanent failure and not a panic; the error carries enough context to identify the failing operation.

- **Read failure classifies as retry-eligible.** Setup: a scratch store whose backing location returns an I/O error on read (injected fault) for an existing key. Action: attempt a read of that key. Expected: the caller receives a retry-eligible task failure — distinct from the "absent" outcome, which is not a failure.

- **Physical layout is inside the run directory and namespaced.** Setup: a scratch store for a known run identity, pipeline, and node identity under a known base. Action: write a value and inspect the on-disk location used. Expected: the storage path is under `<base>/<pipeline>/<run-id>/`, is derived from run and node identity, and two distinct nodes resolve to distinct locations.

- **Hand-constructed context reaches scratch with no runtime running (C8 alignment).** Setup: build a run context by hand in a unit test with a temp run-store base and no runtime, admission, or event stream present. Action: obtain the scratch store from that context and perform a write then read. Expected: the round-trip succeeds, demonstrating scratch is exercisable in a single-task unit test.

## Definition of done
- [ ] A value written on attempt one is readable on attempt two through the run context's scratch API.
- [ ] Keys are namespaced by run and node; two nodes writing the same key do not collide, and two runs writing the same key do not collide.
- [ ] A node cannot read another node's scratch values, and this is enforced by construction (no API path to another node's namespace), not by convention — asserted by a review-checkable test.
- [ ] Scratch values are opaque bytes with keys as opaque byte strings; serialization is the task's affair and the store imposes no schema.
- [ ] A node's scratch is deleted when the node reaches terminal success (lifecycle hook wired).
- [ ] Scratch of nodes that did not succeed remains in the run's directory; nothing is deleted implicitly at run end (removal is deferred to prune, C26 — out of scope here).
- [ ] A scratch read or write failure surfaces to the caller as a retry-eligible task failure (per C4), distinct from the "absent-key" outcome, and never as a permanent failure or a panic.
- [ ] Scratch physically lives under the run store at `<base>/<pipeline>/<run-id>/`, in a per-run/per-node namespaced location derived from identities already carried by the run context (T0.6 layout, T16 context).
- [ ] The scratch API is reachable from a hand-constructed run context in a unit test with no runtime running (C8 acceptance).
- [ ] Rustdoc on the scratch API states: opaque bytes, task-owned serialization, no hard size bound but kilobyte-scale intent, and that scratch is not a channel for inter-node data (use data edges).
- [ ] The full Test plan above is implemented as automated tests and passes.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
None.

## Out of scope
- **Cross-process-restart durability proof** — that scratch under a durable run-store base survives a full process restart is T54a; this ticket implements the store but does not prove restart survival.
- **Resume carry-forward** — copying a re-executing node's scratch forward from a linked prior run into the new run's namespace is C27/T54b; not touched here.
- **Prune-driven removal** — reclaiming retained scratch of non-succeeded nodes is the prune verb (C26); this ticket only guarantees retention, not eventual cleanup.
- **Durable-store selection and the run-store contract itself** — the base location, sink, and layout are fixed by T0.6; this ticket consumes that contract and does not redefine it.
- **Any use of scratch to move data between nodes** — that is the job of typed data edges (C10); the scratch API must not become a side channel, and its docs say so.
- **Scope-boundary temptations:** scratch is a per-node checkpoint store, not a metadata store, not a database, not a cross-run cache, and not shared mutable state; it never influences graph shape, scheduling, or ordering, and offers no route back to a scheduler.
