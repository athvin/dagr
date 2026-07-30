# 123 · T108 — the Kubernetes node runner and the two retry budgets

> **Milestone:** M10 · **Size:** L · **Type:** feature · **Components:** C13, C14, C15, C19
> **Branch:** `feat/t108-kubernetes-node-runner` · **Depends on:** T102, T106, T107 · **Blocks:** T109, T110

## Why / context

This is the ticket that makes remote execution work, and it is deliberately last of
the mechanism tickets because **everything hard has already landed somewhere else**:
the pod-side attempt (T106), the watch (T107), the codec and blob store (T103/T104),
the placement surface (T105), and the timeout that bounds a pod that never reports
(T102).

What remains is one trait implementation. ADR 115's central claim is that
`NodeRunner` (`crates/cli/src/driver.rs:630`) is already the "where does this node
run" seam: it is type-erased, it emits through an **injected** `AttemptEventSink`, it
returns only a `TerminalState`, and it already has `durable_reference` /
`durable_reference_meta` hooks the driver reads after a success. So the run loop,
`ReadinessTracker`, `AdmissionController`, teardown phase, resume path, sink-fault
handling, and exit-code precedence are **untouched** by this ticket. The ~30
hand-written `NodeRunner` implementations in the existing test suite are the proof
that this is true; this ticket adds the thirty-first, and it happens to submit a pod.

Two behaviours are genuinely new and are the reason this is sized L.

**The two retry budgets.** A pod that never *started* — unschedulable, image pull
failure, quota rejection — is an infrastructure failure, and charging it against
`NodePolicy::retries` means a cluster at capacity burns a node's entire retry budget
without executing anything. Airflow reached the same conclusion: a pod reporting
FAILED while the task is still QUEUED is requeued against a separate
`pod_launch_failure_retries` budget *without writing to the event buffer*, so the
scheduler never sees a task failure. dagr needs the same split, expressed in its own
terms.

**⚠ How a pre-start failure is detected — corrected by T101's measurements.** Airflow's
signal ("the pod reported FAILED while the task was still QUEUED") **does not
translate.** T101 ran an unpullable image against a real k3s cluster and the pod
**never reached a terminal phase at all**: it sat in `Pending` with
`waiting.reason=ImagePullBackOff` and **no `terminated` state**, because the platform
retries the pull indefinitely rather than failing the pod. A runner that awaits a
terminal phase would therefore **wait forever**. So this ticket must detect a pre-start
failure as **`Pending` plus a `waiting.reason` in a known-fatal set** —
`ImagePullBackOff`, `ErrImagePull`, `CreateContainerConfigError`, `InvalidImageName` —
and apply **its own bound**, since no platform-side terminal event is coming. An
unschedulable pod is the same shape (`Pending`, no container status), bounded the same
way. See T101's `## Spike findings`, Bet 3(a).

**Terminal state comes from pod status, not exit codes.** Prefect determines a crash
from Job status counters and treats container reasons — `OOMKilled`,
`ImagePullBackOff`, `Unschedulable`, `Evicted` — as **diagnostic strings only**. dagr
follows: the nine-state terminal taxonomy (`arch.md` "Vocabulary") is **closed** and
gains no member; an OOM kill is a `failed` attempt carrying a diagnostic, not a new
state. And per ADR 115 §2, **Kubernetes must not retry** — bare Pods, or Jobs pinned
to `backoffLimit=0` — because two retry loops duplicate an attempt.

## Objective

Implement the executor as a `NodeRunner`, with no change to the driver.

- Implement **`K8sNodeRunner: NodeRunner`**: build the pod spec from the node's
  `Placement` plus the orchestrator's own image digest, **emit the write-ahead
  `attempt-submitted` record**, submit the pod, await its terminal transition through
  T107's observer, read its shard, **replay the shard's records into the injected
  `AttemptEventSink`**, and return the `TerminalState`. Report the shard's output
  reference and metadata through `durable_reference` / `durable_reference_meta` so the
  driver stamps them exactly as it does for a local durable node.
- Add the **`attempt-submitted` writer** to `dagr-artifact` for the `@1.3` kind ADR 115
  §9 fixes in the schema, plus its record type, and add the **`@1.3` fixture-corpus
  artifact** so the "one artifact per released schema version is parsed in CI forever
  after" commitment (`arch.md` "Stability") starts holding for it.
- **Write-ahead ordering is the point**: the record is emitted and durably flushed
  **before** the pod-create call, carrying the identity triple `(run_id, node,
  attempt)`, the **ordered** `{ uri, content_hash }` inputs (empty array for a
  consume-nothing source — never null, and length equal to the node's declared arity),
  both fingerprints, tool version, image digest, and the **intended** pod name. The
  **observed** name, UID, and host are recorded additively once creation returns.
- **No reference recorded here may carry a credential.** A presigned or otherwise
  secret-bearing URL must never reach an event record, a label, or an annotation.
- Enforce **remote-eligibility at registration**: a placed node whose input or output
  types are not `Payload` is a **compile error**, not a runtime failure.
- **Two retry budgets.** Pre-start failures retry against `--dagr.pod-launch-retries`
  (flag > env > default) and emit **no user-visible attempt** — the driver must not see
  a failed attempt, so no retry is consumed and the artifact does not show a phantom
  try. Post-start task failures consume `NodePolicy::retries` exactly as a local node
  does, with T102's real backoff between them.
- **Never let Kubernetes retry**: bare Pods, or Jobs with `backoffLimit=0`; refuse a
  configuration in which cluster-side retry is enabled, with an error naming why.
- **Terminal classification from pod status**, with `OOMKilled` / `Evicted` /
  `Unschedulable` / `ImagePullBackOff` carried as **diagnostic strings** on the attempt
  record. No new terminal state.
- **Idempotent submission** on the attempt key `(run_id, node, attempt)`: a
  resubmission for a key that already has a live pod adopts it rather than creating a
  second.
- **Cancellation**: the per-attempt cancellation signal the driver already supplies
  deletes the pod; a run-level cancel deletes all outstanding pods inside the existing
  grace-plus-teardown budget.
- **Missing or unreadable shard** on a terminal pod is handled per T101's finding: a
  classified failure that names the pod and its status, never a silent success and
  never a hang.

## Test plan (write these first — TDD)

**The seam holds — assert this first**
- Given a placed node, when the run executes, then the driver's loop, admission ledger,
  readiness cascade, and exit-code selection behave identically to a local run with the
  same outcomes — no driver code path is special-cased for remoteness.
- Given a replayed shard, then the run's `events.jsonl` has **gapless, strictly
  increasing `seq`** and folds cleanly into a `RunArtifact`; the orchestrator remains
  the single writer.
- Given a mixed pipeline (some nodes placed, some not), then it runs end to end and the
  artifact does not distinguish them except by policy.

**The submission record is write-ahead — assert the ordering, not just the content**
- Given a placed node, when its attempt is submitted, then an `attempt-submitted`
  record is durably flushed **before** the pod-create call is issued (asserted by
  ordering, e.g. a create hook that reads the stream and finds the record already
  present) — a test that fails if the record is written after creation.
- Given the orchestrator killed **between** the record and the create, then the stream
  contains an `attempt-submitted` with no corresponding pod, and a restart can read the
  intent — the crash window this record exists to cover.
- Given a submitted attempt, then the record carries `(run_id, node, attempt)`, both
  fingerprints, tool version, image digest, and the intended pod name; once creation
  returns, the observed name, UID, and host are recorded.
- Given a node with N inputs, then `inputs` has exactly N entries **in declared
  positional order**, each with its `content_hash` when the producer supplied one.
- Given a consume-nothing source, then `inputs` is an **empty array**, not null or
  absent.
- Given a node whose declared arity disagrees with the reference count assembled for
  it, then submission fails with a classified error rather than launching — the
  detectability the empty-array-plus-known-arity encoding buys.
- Given a reference that would carry a credential, then it is rejected before being
  recorded (asserted by scanning the emitted record for the credential pattern).

**Local runs stay byte-identical**
- Given `--dagr.executor=local`, then **no** `attempt-submitted` record is emitted and
  the stream is byte-identical to a pre-M10 run.
- Given a stream containing `attempt-submitted` records, then `fold_stream` produces a
  `RunArtifact` unchanged from one folded without them (unknown/new kinds do not
  perturb the fold), and the `@1.3` fixture artifact parses.

**Two retry budgets**
- Given a pod that cannot be scheduled and `--dagr.pod-launch-retries=2`, then
  submission is retried twice and the node's `NodePolicy::retries` is **untouched** —
  the artifact shows no extra attempt.
- Given a pod stuck in `Pending` with `waiting.reason=ImagePullBackOff`, then it is
  classified as a **pre-start failure within the runner's own bound** and never waits
  for a terminal phase — the T101-corrected mechanism. A test that waits on a terminal
  phase here must hang, proving the bound is what ends it.
- Given each known-fatal `waiting.reason`, then it is treated as pre-start; given a
  transient waiting reason (e.g. `ContainerCreating`), then it is **not**.
- Given launch retries exhausted, then the node fails with a classified error naming
  the infrastructure cause.
- Given a pod that started and whose task failed retry-eligibly, then
  `NodePolicy::retries` is consumed, T102's backoff elapses between attempts, and each
  attempt is a distinct try number in the artifact.
- Given a pre-start failure followed by a successful launch, then the successful
  attempt is try number 1 — the failed launches did not advance it.

**Classification, not invention**
- Given a pod OOM-killed, then the attempt is `failed` with an `OOMKilled` diagnostic;
  the terminal taxonomy still has exactly nine members.
- Given a pod evicted or preempted, then the attempt is classified per its status with
  the reason carried as a diagnostic.
- Given a configuration enabling cluster-side retry, then the executor **refuses** at
  bootstrap with an error naming the duplicate-execution hazard.

**Timeout and hang**
- Given a pod that starts and never reports, then the node's `NodePolicy::timeout`
  fires (T102) and the pod is deleted — the run does not hang.
- Given a terminal pod with no readable shard, then the node fails with a classified
  error naming the pod and its status.
- Given a pod whose shard reports a different structural fingerprint, then the shard is
  refused and the node fails naming both fingerprints.

**Idempotency and cancellation**
- Given a submission for an attempt key that already has a live pod, then the existing
  pod is adopted and no second pod is created.
- Given a per-attempt cancellation, then the pod is deleted and the attempt records
  `cancelled`.
- Given SIGTERM to the orchestrator mid-run, then all outstanding pods are deleted
  inside the grace-plus-teardown budget and the run reports the cancellation origin.

**Compile-time eligibility**
- Given a placed node whose payload types are not `Payload`, then a `trybuild`
  compile-fail fixture shows an actionable error naming the type and the missing bound.

**Boundaries**
- Given `--dagr.executor=local` on the same pipeline, then it runs in-process and the
  stream is byte-identical to a pre-M10 run.
- Given `scripts/check-metastore-acceptance-boundary.sh`, then it passes; the executor
  adds no listener and links no metastore into the pod path.
- Tests use T107's fake API surface; the real-cluster run is **T112**.

## Definition of done

- [ ] `K8sNodeRunner` implements `NodeRunner` and drives record → submit → await → read
      shard → replay → `TerminalState`, reporting the output reference and metadata
      through the existing hooks; **no driver change**.
- [ ] The `attempt-submitted` writer and record type land in `dagr-artifact` for the
      `@1.3` kind, with a fixture-corpus artifact parsed in CI.
- [ ] The record is durably flushed **before** pod creation (ordering asserted), and
      carries the identity triple, ordered `{ uri, content_hash }` inputs, both
      fingerprints, tool version, image digest, and intended pod name; observed name,
      UID, and host are recorded after creation.
- [ ] `inputs` is an empty array for a consume-nothing source and has exactly the
      declared arity in positional order; an arity mismatch fails before launching.
- [ ] No credential-bearing reference is ever recorded.
- [ ] A local run emits no `attempt-submitted` record and stays byte-identical; the fold
      is unperturbed by streams that contain them.
- [ ] Replayed streams have gapless `seq` and fold cleanly; the orchestrator is the
      single writer.
- [ ] Pre-start failures use `--dagr.pod-launch-retries` and consume no user-visible
      attempt; post-start failures consume `NodePolicy::retries` with real backoff.
- [ ] A pre-start failure is detected as `Pending` + a known-fatal `waiting.reason`
      under the runner's own bound — **not** by awaiting a terminal phase (T101 proved
      none arrives for an unpullable image).
- [ ] Terminal state comes from pod status; `OOMKilled`/`Evicted`/`Unschedulable`/
      `ImagePullBackOff` are diagnostics; the terminal taxonomy still has nine members.
- [ ] Cluster-side retry is refused at bootstrap with the duplicate-execution rationale.
- [ ] A hung pod is bounded by the node timeout and deleted; a terminal pod with no
      readable shard, or a fingerprint-mismatched shard, is a classified failure.
- [ ] Submission is idempotent on `(run_id, node, attempt)`; cancellation deletes pods
      inside the shutdown budget.
- [ ] A placed node with non-`Payload` payloads is a compile error (`trybuild`).
- [ ] `--dagr.executor=local` remains byte-identical to a pre-M10 run.
- [ ] `scripts/check-metastore-acceptance-boundary.sh` passes.
- [ ] Tests pass on `ubuntu-latest` and `macos-latest`.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, the
      rustdoc lint, and cargo-audit/deny where configured).

## Open questions

- **Bare Pod or Job with `backoffLimit=0`?** ADR 115 permits either and forbids
  cluster-side retry. A bare Pod is simpler and makes the invariant unbreakable by
  construction; a Job brings the cluster's own cleanup and eviction handling. Decided
  in-PR against T101's findings, with the refusal test above holding either way.
- **How is a pre-start failure detected?** Airflow's signal is "pod FAILED while the
  task is still QUEUED." dagr's equivalent is that no attempt record has been observed
  from the shard yet, which is available without extra bookkeeping. Confirmed against
  T101's kill-mode findings and recorded in-PR.

## Out of scope

- Orphan adoption after an **orchestrator restart**, tombstoning, and ownership
  revocation — **T109**. This ticket's idempotent submission handles a duplicate
  submission within one live process; surviving a restart is T109's, and it *reads* the
  `attempt-submitted` records this ticket writes.
- The **metastore projection** of `attempt-submitted` and the audit queries over it —
  **T111**. This ticket emits the event; nothing writes SQL here.
- The S3 backend and blob GC — **T110**.
- The real-cluster end-to-end demo, RBAC beyond T107's watch permissions, and the
  acceptance gate — **T112**.
- Warm pods, pod reuse, or batching several nodes into one pod. Named as future work
  only if T101's latency numbers make per-node pods untenable — in which case ADR 115
  §2 reopens rather than this ticket growing.
- Log streaming from pods — the cluster's job, per ADR 115.
- Foreign-image tasks — future work behind the pod-spec seam.
- Scope boundary restated: one orchestrator process renting compute per node attempt
  for one run adds no coordination and no server — no peer, no election, no queue, no
  inbound API, and pods never touch the run index. dagr remains not a scheduler, a
  coordinating metadata store, a web interface, a DSL, or a backfill orchestrator, and
  the graph's shape never changes at runtime — a placed node is one node.
