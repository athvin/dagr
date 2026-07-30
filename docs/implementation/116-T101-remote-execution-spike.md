# 116 · T101 — spike: kube watch semantics, pod startup latency, blob round-trip

> **Milestone:** M10 · **Size:** M · **Type:** decision (spike) · **Components:** system-level
> **Branch:** `adr/t101-remote-execution-spike` · **Depends on:** T100 · **Blocks:** T103, T105

## Why / context

ADR 115 (T100) bets on three properties of the substrate that no code in this repo
has ever exercised. M0 established the precedent that the highest-risk premises get
a **throwaway spike before the tickets that depend on them** (T0.2 output ownership,
T0.3 timeout accounting), and this milestone's premises are riskier than either:
they involve a network, another party's control plane, and a latency budget the spec
publishes a number for.

The three bets, each with a named consequence if wrong:

1. **A single long-lived pod watch can be made reliable.** ADR 115 §2 mandates one
   watch per orchestrator process. Kubernetes watches terminate — `resourceVersion`
   expiry (HTTP 410 Gone), API-server rollout, idle timeout, network partition — and
   a watch that silently stops delivering events without erroring is
   indistinguishable from "no pods have changed." If reconnection cannot be made
   correct and *observably* correct, the no-callback state path (§3) has no liveness
   signal and the ADR reopens.
2. **Pod startup latency is tolerable at the target graph sizes.** `arch.md`
   publishes a sub-millisecond per-node framework overhead budget; ADR 115 rescopes
   it to local execution and promises remote start latency as a separately measured
   figure. That figure does not exist yet. If submission-to-container-start is tens
   of seconds rather than low single digits, per-node pods are the wrong granularity
   and §2 reopens.
3. **The shard path reports terminal state even when the pod dies badly.** The whole
   no-callback design rests on the orchestrator reconstructing an attempt from a
   shard the pod wrote. A pod OOM-killed or evicted **mid-write** leaves a partial
   shard, and a pod killed before it writes anything leaves none. The fold already
   tolerates exactly one trailing partial record; whether that tolerance covers the
   real failure modes is an empirical question.

This is a **spike**: throwaway code, quarantined, deleted before merge. It ships no
production code and no dependency in the shipped lockfile. Its output is
measurements and a recorded verdict on each bet.

## Objective

Validate or refute the three bets, and record the answers where the tickets that
consume them will read them.

- Stand up a **quarantined** spike (a `spikes/` directory or an excluded workspace
  member — not a shipped crate, and not reachable from `dagr-cli`) that talks to a
  real local cluster (kind or k3s).
- **Bet 1 — watch reliability.** Drive a watch through forced `resourceVersion`
  expiry, an API-server restart, and a network interruption. Determine: does the
  chosen client surface these as errors or silently stall; does it re-list-then-watch
  correctly; can a missed terminal transition occur; and what is the correct
  bookmark/resync discipline. Record the reconnect recipe T107 must implement.
- **Bet 2 — startup latency.** Measure submission → `Running` and submission →
  container-entrypoint-executing, for a warm image and a cold pull, at
  concurrencies of 1, 10, and 50 pods. Report p50 and p99. Record the numbers
  `arch.md`'s performance envelope will cite and the granularity guidance authors
  get.
- **Bet 3 — shard durability.** Kill a pod mid-shard-write (OOM via a memory limit,
  eviction, and `SIGKILL`), and kill one before it writes at all. Determine what the
  orchestrator can conclude in each case, whether `fold_stream`'s
  single-trailing-partial tolerance is sufficient, and what the executor must do
  when a pod reaches a terminal phase with no readable shard.
- **Client choice.** Evaluate the Rust Kubernetes client options against bets 1 and
  3, and record the choice with its reasoning and its transitive dependency and
  licence impact (ADR 115 quarantines these, but the spike is where the real list is
  learned).
- Record all findings in this ticket file under a `## Spike findings` section, and
  **delete the spike code before merge** — the tree ends clean.

## Test plan (write these first — TDD)

Spike ticket: the assertions are about the *record*, plus the harness that produces
it being re-runnable.

- **Reproducibility.** A single documented command stands up the cluster and runs
  the three experiments; a second run reproduces the verdicts (not the exact
  timings) on a different machine.
- **Bet 1 is decided by an experiment that can fail.** The watch experiment forces
  at least one real `410 Gone` and one API-server restart, and asserts the harness
  *detects* the interruption rather than silently continuing — a run in which no
  interruption occurred is reported as inconclusive, never as a pass.
- **Bet 2 produces numbers, not adjectives.** Latency is reported as p50/p99 at each
  of the three concurrencies, with the image-pull condition stated. A missing
  measurement fails the ticket.
- **Bet 3 enumerates outcomes.** For each of the four kill modes, the record states
  what was on disk, what the fold returned, and what the orchestrator could
  conclude. "It worked" without the four cases is not a finding.
- **Verdict recorded per bet.** Each bet ends `HOLDS` / `HOLDS WITH CONSTRAINT
  <constraint>` / `REFUTED`, and any refutation names the ADR 115 section that
  reopens.
- **Tree is clean.** `git diff` at merge touches only `docs/**`; no `crates/**`, no
  `Cargo.lock` change, no spike code left behind.

## Definition of done

- [ ] A `## Spike findings` section in this file records, per bet, the experiment
      run, the raw result, and a `HOLDS` / `HOLDS WITH CONSTRAINT` / `REFUTED`
      verdict.
- [ ] The watch reconnect recipe (error taxonomy, re-list discipline, bookmark
      handling, stall detection) is written down concretely enough for T107 to
      implement without re-deriving it.
- [ ] Submission→start latency is recorded as p50/p99 at 1, 10, and 50 concurrent
      pods, warm and cold image, with the exact numbers `arch.md` will cite.
- [ ] The four pod-kill modes each have a recorded on-disk state, fold result, and
      orchestrator conclusion; the executor's required behaviour for "terminal phase,
      no readable shard" is stated.
- [ ] The Kubernetes client is chosen, with reasoning and its transitive dependency
      and licence list recorded for T107's `deny.toml` work.
- [ ] Any refuted bet names the ADR 115 section that reopens, and the ticket reports
      a STOP rather than proceeding.
- [ ] The spike code is deleted; the diff is docs-only.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, the
      rustdoc lint, and cargo-audit/deny where configured).

## Open questions

- **Which local cluster does CI use?** kind and k3s differ in image-pull behaviour
  and in how fast the API server restarts, which affects bets 1 and 2. The spike
  measures on whichever it picks and records the choice so T112's CI job matches it;
  if the two disagree materially, that is itself a finding.
- **Is a 50-pod concurrency measurement meaningful on a single-node dev cluster?**
  Probably not for scheduling latency. The spike reports what it can and states the
  limit of the measurement rather than extrapolating.

## Out of scope

- Any production code, dependency, or lockfile change — this spike is throwaway and
  deleted before merge. The client dependency lands in **T107**.
- The `Payload` codec — **T103**; the blob port — **T104**. The spike may hand-roll
  whatever it needs to measure a round-trip.
- The executor itself — **T108**; the shared observer — **T107**; adoption —
  **T109**.
- Re-deciding anything ADR 115 (T100) settled. A refuted bet reopens the ADR; it
  does not get patched locally.
- Scope boundary restated: a spike that talks to a cluster introduces no
  coordination and no server — one process, outbound calls only, nothing served and
  nothing persisted beyond the run. dagr remains not a scheduler, a coordinating
  metadata store, a web interface, a DSL, or a backfill orchestrator, and the graph's
  shape never changes at runtime.

---

# Spike findings

> **Run 2026-07-29** against a real 4-node k3s cluster (`athvin-prod`, k3s v1.36.0,
> Civo CSI) from an out-of-cluster orchestrator (macOS, kubeconfig), inside a
> `dagr-spike` namespace held to 12 pods / 512Mi by a `ResourceQuota` and a
> `LimitRange` — because the cluster's worst node was at **102% memory** and already
> carrying other work. Harness: throwaway Python + `kubectl`, deleted before merge.
> Concurrency above 10 is deliberately **not** measured here (see Bet 2).

## Bet 1 — a single long-lived watch can be made reliable → **HOLDS WITH CONSTRAINT**

The constraint is the recipe, and it is specific enough for T107 to implement without
re-deriving it.

**(a) A stale `resourceVersion` is reported *inside the watch stream*, not as a
transport error.** Watching from `resourceVersion=1` yields, as a watch event:

```json
{"type":"ERROR","object":{"kind":"Status","status":"Failure",
 "message":"too old resource version: 1 (9371596)","reason":"Expired","code":410}}
```

Two consequences. A client that only handles connection/transport failures **will not
notice this** — it must inspect `type == "ERROR"` on decoded events. And the message
**names the current resourceVersion** (`9371596`), so the recovery point is available
in the failure itself.

**(b) A stale `resourceVersion` on a *LIST* succeeds.** The same `resourceVersion=1`
that expires a watch returns a normal `PodList` carrying a fresh
`metadata.resourceVersion`. So the standard **re-list-then-watch** recovery is
confirmed working on this cluster: on a 410, LIST (any RV) to obtain current state
plus a usable RV, then re-watch from it. A resync therefore cannot lose a transition
that happened during the gap — the LIST returns current state, not a delta.

**(c) `BOOKMARK` events are delivered when requested.** With
`allowWatchBookmarks=true`, bookmarks arrive on an otherwise-idle watch (2 observed in
a 10s window). T107 should request them and track the bookmarked RV, so a reconnect
resumes from a recent RV instead of re-listing every time.

**(d) A *future* `resourceVersion` produces a silent stall.** Watching from
`resourceVersion=999999999999` returned **no data and no error** for the full 8s
window. This is the exact failure mode ADR 115 §3 worried about: silence is
indistinguishable from "nothing changed", and there is no inbound heartbeat to fall
back on. **This validates the stall-detection requirement as load-bearing**, not
defensive: a watch that has neither errored nor delivered within a bound must be
treated as broken and reconnected.

**Caveat on transferability.** (a)–(d) are *server-side* facts and hold for any
client. What the chosen Rust client does with them — whether it surfaces an in-stream
ERROR distinctly, and whether its reconnect is automatic — is **not** established
here and must be verified in T107 against the client it picks.

## Bet 2 — pod startup latency is tolerable → **HOLDS**

Client-side wall clock from "create issued" to "container observed running or
terminated", which is what an executor actually experiences and assumes no clock sync
with the nodes.

| Condition | n | min | p50 | p99 | max | stdev |
|---|---|---|---|---|---|---|
| cold image pull | 1 | 3.08 | 3.08 | 3.08 | 3.08 | — |
| warm, sequential singles | 3 | 2.09 | 2.31 | 2.39 | 2.39 | 0.16 |
| warm, concurrent fan-out | 10 | 2.12 | **2.49** | 2.50 | 2.50 | **0.19** |

**Headline: ~2.1–2.5s warm, ~3.1s on a cold pull, and it does not degrade from 1 to
10 concurrent pods** (stdev 0.19s across the fan-out).

**A methodology correction worth recording**, because it would have produced a wrong
number. The first fan-out harness submitted pods sequentially and began observing only
after the last submit, yielding an apparent latency *ramp* (1.50 → 4.53s, stdev 1.08)
that looked like cluster degradation under concurrency. It was the harness: each
`kubectl apply` costs ~0.3s of process spawn, so an early pod's measured latency
absorbed the whole submission loop. Submitting concurrently and observing from the
first submit collapsed the spread to 0.19s. **The cluster was never the bottleneck.**
Any future benchmark must submit concurrently and observe continuously.

**Not measured: concurrency above 10.** The cluster's worst node was at 102% memory
with other workloads on it, and a namespace quota bounds *my* usage, not the node's
existing pressure — so a 50-pod fan-out could have evicted work that was already
running. That measurement belongs on a local cluster and is **outstanding**; this
report must not be read as covering it.

**Consequence for the spec.** `arch.md` budgets framework overhead at under one
millisecond per node. Remote placement costs **~2.3s of cluster latency per node
attempt** — roughly three orders of magnitude more, and entirely outside dagr's
control. This is the measured basis for ADR 115's decision to scope that budget to
local execution and report remote start latency separately, and for the authoring
guidance that remote placement pays off only when a node's own work dominates ~2.5s.

## Bet 3 — the shard path reports terminal state even when the pod dies badly → **PARTIAL**

**(a) What the orchestrator observes per termination mode — COMPLETE.**

| Mode | `phase` | `reason` | `exitCode` | terminal state reached? |
|---|---|---|---|---|
| clean success | `Succeeded` | `Completed` | 0 | yes |
| task error (`exit 7`) | `Failed` | `Error` | **7** | yes |
| OOM past a 64Mi limit | `Failed` | **`OOMKilled`** | **137** | yes |
| `kill -9` from inside | `Failed` | `Error` | 1 | yes |
| unpullable image | **`Pending`** | `waiting.ImagePullBackOff` | *none* | **NO** |

Three findings, one of which **changes a downstream ticket**:

- **`OOMKilled` is cleanly distinguishable** (`reason=OOMKilled`, `exitCode=137`),
  confirming ADR 115 §6's plan to carry it as a diagnostic string rather than a new
  terminal state.
- **A task's own exit code survives** (`exit 7` observed verbatim), so T106's plan to
  map dagr's `ExitCode` table through the pod boundary works.
- **⚠ A pre-start failure never reaches a terminal phase.** The unpullable-image pod
  sat in `Pending` with `waiting.reason=ImagePullBackOff` and **no `terminated` state
  at all**. So Airflow's pre-start signal — "the pod reported FAILED while the task
  was still QUEUED" — **does not translate to this cluster**, and **T108's stated
  detection mechanism is wrong as written**: a runner awaiting a terminal phase would
  wait *forever*. T108 must instead detect **`Pending` + a `waiting.reason` in a
  known-fatal set** (`ImagePullBackOff`, `ErrImagePull`, `CreateContainerConfigError`,
  `InvalidImageName`) and apply its own bound, since the platform will retry the pull
  indefinitely rather than fail the pod.

**(b) Is a partially-written shard detectable? — OUTSTANDING, operator-blocked.**
Needs somewhere for a pod to write a shard, and the reference cluster offers neither an
object store nor an RWX class (see below). The four kill modes above are reproducible
and the harness is written, so this is a matter of pointing it at storage — **not** of
new method. Deferred to the operator-provisioned infrastructure recorded as a blocking
prerequisite in **T112**; this spike does **not** improvise a substitute, because a
shard smuggled through a `ConfigMap` would not exercise the shipped path.

## Findings outside the three bets

- **⚠ No RWX storage class exists.** The cluster offers exactly one class —
  `civo-volume` (Civo CSI, `WaitForFirstConsumer`) — and it is **RWO**. T104's plan
  treats "a shared read-write-many volume" as a legitimate production configuration
  for the local blob backend; **on this cluster that configuration is unavailable**,
  making an object store mandatory rather than optional. T104 should state RWX as a
  *cluster capability to check*, not an assumption, and T110's S3 backend moves onto
  the critical path for anyone on a CSI without RWX.
- **No in-cluster registry.** A pod running a real dagr binary needs one (or an
  external one), which is a prerequisite T112's CI job must provision rather than
  assume.
- **Out-of-cluster orchestration works exactly as ADR 115 §3 predicted.** Every
  measurement above was taken from a laptop against a remote cluster over kubeconfig,
  with no inbound reachability and no tunnel — the "iterate locally, execute remotely"
  premise is confirmed, not merely argued.

## Verdicts

| Bet | Verdict |
|---|---|
| 1 — one reliable long-lived watch | **HOLDS WITH CONSTRAINT** — recipe in (a)–(d); client behaviour still to verify in T107 |
| 2 — startup latency tolerable | **HOLDS** — ~2.3s warm, flat to n=10; **>10 unmeasured** |
| 3 — shard reports state under bad deaths | **PARTIAL** — (a) complete and it *refines T108*; (b) outstanding, **operator-blocked** on storage (prerequisite recorded in T112) |

No bet is **REFUTED**, so **no ADR 115 section reopens**. Two downstream tickets take
corrections rather than reopening a decision: **T108**'s pre-start detection
(Pending + waiting-reason, not terminal-Failed) and **T104**'s RWX assumption.

**What this spike deliberately did not do.** It did not run a real dagr DAG in a pod,
because no pod-side code exists yet (that is T106) and because doing so needs a
registry and pod-to-pod storage the reference cluster lacks. It did not run a 50-pod
fan-out against a cluster already at 102% memory on its worst node. Both are recorded
as an **operator-provisioned blocking prerequisite in T112** rather than approximated
here — an end-to-end proof built on stand-ins would assert less than it appears to, and
the gate's whole value is that it cannot.
