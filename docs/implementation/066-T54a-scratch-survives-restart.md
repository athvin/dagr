# 066 · T54a — C18: scratch survives process restart under the run store

> **Milestone:** M4 · **Size:** S · **Type:** feature · **Components:** C18
> **Branch:** `feat/t54a-scratch-survives-restart` · **Depends on:** T53 · **Blocks:** T54b, T58

## Why / context
The whole point of durable scratch is that a checkpoint written before a crash is still there after the crash — a task that finished the first half of its work must be able to say so across a full process restart, not just across an in-process retry. This ticket builds on T53, which stands up the per-run, per-node scratch key-value store under the run directory (C18, arch.md `### C18 · Durable scratch store`, lines 385–401), and hardens the *lifecycle* half of that component: scratch of nodes that did not succeed is retained on disk under the run-store base, nothing is deleted implicitly at run end, and only prune (C26) reclaims it. It is governed by the amended C18 lifecycle (arch.md line 393: "Scratch of nodes that did not succeed stays in the run's directory … and is removed by prune; nothing is deleted implicitly at run end") and inherits the placement, base-location, and no-implicit-deletion contract locked by T0.6 (arch.md "The shape of a run," lines 67, 391). It is the durability foundation that T54b (resume scratch carry-forward) and T58 (resume core) both stand on — carry-forward is meaningless if the prior run's scratch did not survive the process ending.

## Objective
Prove and enforce that per-node scratch persists on the run-store medium across a full process exit and restart, that non-succeeded nodes' scratch is retained (never deleted implicitly at run end), and that prune is the only path that removes it.

Concrete pieces of work:
- Confirm the T53 scratch store writes through the run-store base — `<base>/<pipeline>/<run-id>/` (arch.md line 67) — so that when the operator points the base at storage that outlives the container, scratch written by one process is readable by a *later, separate* process addressing the same run directory.
- Enforce the amended end-of-run lifecycle: at run end, delete only the scratch of nodes that reached the succeeded terminal state; retain on disk the scratch of every node that did not succeed (failed, timed-out, cancelled, skipped, or never reached terminal). Nothing about run end deletes non-succeeded scratch.
- Ensure a fresh process that opens an existing run directory (the situation a resume faces) sees exactly the retained non-succeeded scratch, byte-for-byte, under the original per-run/per-node namespacing, with cross-node isolation still enforced.
- Make prune (C26) the sole mechanism that removes a retained non-succeeded node's scratch, by removing the whole per-run directory; verify a run directory that prune has not touched still carries its non-succeeded scratch.
- Document the retention guarantee at the scratch API and in the run-store/prune operator docs: succeeded scratch is gone, non-succeeded scratch survives restart and is the operator's to prune.

## Test plan (write these first — TDD)
- **Scratch survives a full process restart.** Setup: point the run-store base at a real filesystem path that outlives a single process; run a pipeline whose single node writes a known key/value to scratch on attempt one and then fails in a way that leaves the node non-succeeded, and let the process exit. Action: start a *separate* process that opens the same run directory and reads that node's scratch namespace. Expected: the key is present and its bytes equal what the first process wrote — the value crossed a process boundary via the run-store medium, not via in-process state.
- **Non-succeeded scratch is retained at run end.** Setup: a run with one node that writes scratch and ends non-succeeded (e.g. exhausts retries and is `failed`). Action: after the run process has finished normally (not killed), inspect the on-disk run directory. Expected: that node's scratch is still present on disk; run end deleted nothing belonging to a non-succeeded node.
- **Succeeded scratch is gone after restart too.** Setup: a run with one node that writes scratch and then succeeds. Action: after the process exits, open the run directory from a fresh process and look for that node's scratch. Expected: the succeeded node's scratch namespace is absent — success-triggered deletion (T53) is durable and is not resurrected by the retention path.
- **A non-succeeded run has no implicit end-of-run deletion.** Setup: a multi-node run where some nodes succeed and some do not, all writing scratch. Action: after the process finishes, list the scratch present in the run directory. Expected: exactly the non-succeeded nodes' scratch remains; the succeeded nodes' scratch is gone; the run-finished path itself performed no deletion beyond the per-node success deletions.
- **A fresh process sees retained scratch under the original namespacing.** Setup: a completed run that left node A and node B both non-succeeded, each having written a distinct value under a shared key name. Action: from a new process, open the run directory and read that key for A and for B. Expected: each reads back its own node's value; the per-run/per-node namespace kept them disjoint across the restart, and neither node's process could reach the other's (isolation from T53/C18 acceptance holds across the boundary).
- **Prune removes retained non-succeeded scratch.** Setup: a completed run directory that retains a non-succeeded node's scratch, plus enough other runs to make it a prune candidate by count or by age. Action: run the prune verb (C26) so this run is selected. Expected: the whole per-run directory — including the retained scratch — is gone afterward; prune is the mechanism that reclaimed it.
- **Prune is the only remover.** Setup: a completed run directory retaining non-succeeded scratch, with prune *not* run against it. Action: open the run directory from a fresh process at any later time. Expected: the non-succeeded scratch is still present — no timer, no next run, and no run-end path ever removed it; only an explicit prune would.
- **Retention holds on a durable-style base.** Setup: point the base at a mounted/synced directory (the "survives the container" configuration, arch.md line 67) and run a pipeline leaving a node non-succeeded. Action: simulate the container going away by discarding the process and re-opening the run directory from the persisted base with a new process. Expected: the non-succeeded node's scratch is intact and readable — matching the operational promise that a run whose store survives is the resumable case (arch.md line 67, 688).

## Definition of done
- [ ] Per-node scratch is written through the run-store base under `<base>/<pipeline>/<run-id>/` (T53/T0.6), so a value written by one process is readable by a later, separate process that opens the same run directory.
- [ ] A value written to a non-succeeded node's scratch survives a full process exit and restart and is readable, byte-for-byte, by a fresh process (C18: durable across restart, the foundation resume relies on).
- [ ] At run end, only the scratch of succeeded nodes is deleted; the scratch of every non-succeeded node (failed, timed-out, cancelled, skipped, or never-terminal) is retained on disk (arch.md line 393).
- [ ] Nothing is deleted implicitly at run end beyond the per-node success deletions — the run-finished path performs no blanket scratch cleanup (arch.md line 393).
- [ ] A fresh process opening an existing run directory sees the retained non-succeeded scratch under its original per-run/per-node namespacing, with cross-node isolation still enforced (C18: keys namespaced by run and node; one node cannot read another's).
- [ ] Prune (C26) is the sole mechanism that removes a retained non-succeeded node's scratch, and it does so by removing the whole per-run directory.
- [ ] A run directory that prune has not selected still carries its non-succeeded scratch at any later time — no other path reclaims it.
- [ ] The retention guarantee is documented at the scratch API and in the run-store/prune operator docs (succeeded scratch gone; non-succeeded scratch survives restart and is prune's to remove), and any machine-classed C18 lifecycle criteria are registered in the criteria matrix.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
None.

## Out of scope
- Copying scratch *forward* into a resumed run's namespace — that is the resume carry-forward behavior owned by T54b/T58 (arch.md line 391); this ticket only guarantees the prior run's scratch survives so there is something to copy.
- The resume verb itself and its seed/closure/demand algorithm (C27, T58); this ticket does not read or interpret a prior run, it only proves persistence and retention.
- Defining or changing the run-store base-location surface, directory layout, sink, or run-identity scheme — those are locked by T0.6 and consumed here, not re-decided.
- The prune verb's own selection semantics (count/age policy, CLI surface) — owned by C26/T55/T56; this ticket only asserts prune is the path that removes retained scratch and touches the whole per-run directory.
- Any hard size bound, quota, or eviction policy on scratch (C18 states no hard bound; kilobyte-scale is documentation, not enforcement), and any use of scratch to pass data between nodes — data edges own that (C18 line 389).
- A metadata store, index, or cross-run registry of scratch — dagr is not and will not become a metadata store; retention is per-run-directory files reclaimed only by prune.
