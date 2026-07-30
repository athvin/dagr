# 124 · T109 — orphan adoption, tombstones, and ownership revocation

> **Milestone:** M10 · **Size:** M · **Type:** feature · **Components:** C16, C27
> **Branch:** `feat/t109-orphan-adoption-and-ownership` · **Depends on:** T108 · **Blocks:** T112

## Why / context

T108's submission is idempotent within one live orchestrator process. This ticket
handles the case that process **dies**: the orchestrator is killed, evicted, or
redeployed while pods are still running. Those pods keep working — they are separate
processes on separate machines, unaware their submitter is gone — and the next
invocation must decide what they are.

Getting this wrong is expensive in exactly the way dagr exists to prevent. The naive
behaviour is to resubmit every unfinished node, which duplicates work that is already
running (and, for a task with side effects, duplicates the side effects). The other
naive behaviour is to ignore the pods, which leaks them and double-counts cluster
capacity.

Airflow's answer is the one ADR 115 §5 adopts, and its mechanics are specific:
ownership lives in a **mutable pod label** that the orchestrator **patches in place** —
adoption is a labels-only patch rewriting the owner key, *never* a pod recreation.
Terminal pods are deleted or **tombstoned** with a completion key, and that key is
precisely what the adoption selector filters on, so a finished pod is not adopted
twice. Revocation is a deliberate **two-step patch-then-delete**, so the delete is not
misread as an external deletion by whatever is still watching.

This composes with resume rather than competing with it. dagr's resume already decides
which nodes need to re-run from the prior run's artifact and durable references
(`plan_resume`); adoption answers a narrower question — for a node resume says must
run, is there **already a pod running this exact attempt**? An adopted pod's shard,
when it lands, replays into the new run's stream through T108's existing path.

## Objective

Make an orchestrator restart safe, and pod ownership explicit.

- On startup under the `k8s` executor, **discover** pods for the run by label selector,
  **excluding** those carrying the completion tombstone key.
- **Adopt** a discovered pod whose annotations match the current build — same
  structural fingerprint, same tool version, same image digest — by **patching only the
  owner label**. Never recreate, never delete-and-resubmit, never mutate anything else.
- Wire an adopted pod into T107's observer and T108's runner so its terminal transition
  and shard are consumed exactly as a freshly submitted pod's would be — one code path
  after adoption, not two.
- **Refuse to adopt** a pod whose annotated fingerprint, tool version, or image digest
  differs from the current build: it belongs to a different program. Report it, leave it
  alone, and fail the node with a classified error rather than guessing.
- **Tombstone** a pod whose outcome has been consumed, using the same key the discovery
  selector excludes — so a completed attempt is never adopted again, even if pod
  deletion is deferred or fails.
- Implement **revocation as patch-then-delete**: clear the owner label, then delete, so
  a watcher distinguishes an orchestrator-initiated teardown from an external deletion.
- Handle the **ambiguous cases** explicitly: two pods for the same attempt key (adopt
  one deterministically, revoke the other), a pod that is terminal with a readable
  shard (consume it, do not re-run), and a pod that is terminal with no shard (T108's
  classified failure).

## Test plan (write these first — TDD)

**Adoption happens instead of duplication**
- Given a live pod for an attempt key and a fresh orchestrator process, when it starts,
  then the pod is **adopted** — its owner label is patched, no second pod is created,
  and the pod object is otherwise unmodified (asserted field-by-field).
- Given that adopted pod completing, then its shard is replayed into the new run's
  stream and the node reaches the shard's terminal state — the same path a submitted
  pod takes.
- Given an adopted pod, then the run's `events.jsonl` still has gapless `seq` and folds
  cleanly.

**Tombstones prevent double adoption**
- Given a pod whose outcome has been consumed, then it carries the completion key and a
  subsequent discovery **excludes** it.
- Given a tombstoned pod that was never deleted (deletion deferred or failed), then a
  restart does not adopt it and does not re-run its node.

**Refusal on mismatch**
- Given a pod whose annotated structural fingerprint differs, then adoption is refused,
  the pod is left untouched, and the node fails with an error naming both fingerprints.
- Given a mismatched tool version or image digest, then likewise — a different program's
  pod is never adopted.

**Revocation ordering**
- Given a revocation, then the owner label is cleared **before** the delete is issued,
  and the ordering is asserted (not merely the end state).
- Given a revoked pod, then a watcher can distinguish it from an externally deleted
  pod.

**Ambiguity**
- Given two live pods for the same attempt key, then exactly one is adopted
  deterministically and the other is revoked; the node produces exactly one terminal
  state.
- Given a pod terminal with a readable shard at discovery time, then its outcome is
  consumed without re-running the node.

**Composition with resume**
- Given a prior run and a resume where a node resume marks must-run **and** a live pod
  exists for it, then the pod is adopted rather than the node resubmitted.
- Given a resume where a node is `satisfied-from-prior`, then no pod is sought or
  adopted for it (it has no runner at all).
- Given resume refusing (structural mismatch, dangling reference), then no adoption is
  attempted — the refusal path is unchanged.

**Kill-and-restart, for real**
- Given an orchestrator killed mid-run with pods live, when a new process starts, then
  the run completes with **each node executed once** — the end-to-end property this
  ticket exists for, asserted via the artifact's attempt records.

## Definition of done

- [ ] Startup discovers the run's pods by label, excluding tombstoned ones.
- [ ] Adoption patches **only** the owner label; the pod is otherwise unmodified and is
      never recreated.
- [ ] An adopted pod's terminal transition and shard flow through the same path as a
      submitted pod's; `seq` stays gapless and the stream folds.
- [ ] A pod whose annotated fingerprint, tool version, or image digest differs is
      refused, left untouched, and reported with both values named.
- [ ] Consumed outcomes are tombstoned with the key discovery excludes; a tombstoned pod
      is never adopted or re-run.
- [ ] Revocation clears the owner label **then** deletes, with the ordering asserted.
- [ ] Two pods for one attempt key resolve deterministically to one adoption and one
      revocation, with exactly one terminal state.
- [ ] Adoption composes with resume: must-run nodes with live pods are adopted;
      `satisfied-from-prior` nodes seek no pod; resume refusals are unchanged.
- [ ] A kill-and-restart test shows every node executed exactly once.
- [ ] Tests pass on `ubuntu-latest` and `macos-latest` (fake API surface; the real
      cluster kill-restart is **T112**).
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, the
      rustdoc lint, and cargo-audit/deny where configured).

## Open questions

- **Does adoption require the same run id, or can a new run adopt a prior run's pods?**
  Same run id, on the reasoning that the attempt key is scoped to a run and dagr's
  resume already mints a **new** run id — so a resumed run adopting the prior run's pods
  would blur two runs' event streams. The consequence is explicit and recorded in-PR: a
  resumed run does **not** adopt the killed run's pods; it revokes them and resubmits,
  and the kill-restart guarantee above applies to a restart of the *same* run id (an
  operator-supplied id, which `RunId::from_operator` already supports). If the operator
  wants cross-run adoption, that is a decision, not an implementation detail.
- **Should an un-adoptable foreign pod be revoked or left running?** Left running and
  reported: deleting another program's pod is not dagr's call. Recorded in-PR.

## Out of scope

- The observer and its reconnect discipline — **T107** (this ticket consumes it).
- Pod submission, retry budgets, and shard replay — **T108** (this ticket reuses the
  post-adoption path unchanged).
- Cross-run adoption, and any change to `plan_resume` or the resume refusal gates.
- Garbage-collecting blobs left by an abandoned run — **T110**.
- A reaper for pods belonging to runs that will never restart. Operators have
  `kubectl` and the labels are documented; a background reaper would be a process that
  outlives a run, which ADR 115 explicitly does not permit.
- Scope boundary restated: adoption is one process reclaiming work it started, by
  patching a label on a pod it owns — no second coordinating process, no queue, no
  control plane outliving the run. dagr remains not a scheduler, a distributed
  execution system, a coordinating metadata store, a web interface, a DSL, or a
  backfill orchestrator, and the graph's shape never changes at runtime.
