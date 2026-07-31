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


## Spike findings

The three bets were exercised against **two substrates**, deliberately, because the
ticket's first open question asks which cluster CI should use and warns that "if the
two disagree materially, that is itself a finding". They do disagree — twice — and
both disagreements are operational rather than semantic.

- **Run A — 2026-07-29, remote 4-node k3s** (`athvin-prod`, k3s v1.36.0, Civo CSI),
  orchestrated from macOS over kubeconfig inside a `dagr-spike` namespace held to
  12 pods / 512Mi by a `ResourceQuota` and a `LimitRange` — the cluster's worst node
  was at **102% memory** and carrying other work. Harness: throwaway Python +
  `kubectl`. This run could not measure concurrency above 10, could not restart the
  API server, and had **no writable shared storage**, so bet 3 was left open.
- **Run B — 2026-07-31, local single-node kind** (kind v0.31.0, `kindest/node`
  v1.35.0, containerd 2.2.0, 8 CPU / 7.6 GiB Docker Desktop VM). Harness: a
  throwaway Rust binary built on **kube-rs**, plus `kubectl`/`crictl`. This run owns
  the cluster, so it could restart the API server, blackhole it, fan out to 50 pods,
  and mount a host directory as the shared blob volume. It closes bet 3 and
  **supersedes Run A wherever the two conflict** — every conflict is called out
  inline.

Run B's harness talks to the cluster through **kube-rs**, on purpose: bets 1 and 3
are about what *a client* observes, and Run A could only establish what the *server*
sends. Run A's own "caveat on transferability" is discharged below.

### How to reproduce

A throwaway Rust probe (`spikes/t101/kube-probe`, kube-rs) and two shell harnesses
were used, and are deleted with the rest of the spike; the commands are recorded here
because the *recipe* is the reproducible artefact, not the code. From a machine with
Docker, `kind`, `kubectl` and a Rust toolchain:

```sh
# 1. cluster + shared blob volume
mkdir -p /tmp/t101-blobs && chmod 777 /tmp/t101-blobs
cat > kind.yaml <<YAML
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
    extraMounts: [{ hostPath: /tmp/t101-blobs, containerPath: /dagr-blobs }]
YAML
kind create cluster --name dagr-t101 --config kind.yaml --wait 120s   # 2m46s cold
export KUBECONFIG=$(mktemp); kind export kubeconfig --name dagr-t101 --kubeconfig "$KUBECONFIG"
kubectl create namespace t101

# 2. bet 1 — watch semantics. Server truth first, then the client.
kubectl get --raw '/api/v1/namespaces/t101/pods?watch=true&resourceVersion=1&timeoutSeconds=8'
kubectl get --raw '/api/v1/namespaces/t101/pods?watch=true&allowWatchBookmarks=true&resourceVersion=<rv>&timeoutSeconds=100'
probe watch --mode expired|future|live|runtime --seconds 130   # kube-rs, low-level and runtime::watcher
#    ...while, from another shell, forcing the two real interruptions:
docker exec dagr-t101-control-plane sh -c 'crictl ps -q --name kube-apiserver | xargs -r crictl stop'  # API-server restart
docker pause dagr-t101-control-plane; sleep 25; docker unpause dagr-t101-control-plane                 # network interruption

# 3. bet 2 — latency. Submit CONCURRENTLY, observe from before the first submit.
for n in 1 10 50; do
  docker exec dagr-t101-control-plane crictl rmi docker.io/library/busybox:1.36   # cold
  probe latency --n $n --image busybox:1.36 --tag cold$n
done
for n in 1 10 50; do probe latency --n $n --image busybox:1.36 --tag warm$n; done  # warm

# 4. bet 3 — the four kill modes and the blob round-trip
bash bet3.sh /tmp/t101-blobs      # OOM / eviction / SIGKILL / killed-before-write
./target/debug/dagr fold < /tmp/t101-blobs/shard-oom.ndjson
```

A second run reproduces the **verdicts**, not the timings. Two reproducibility
hazards are load-bearing and are stated as findings rather than footnotes: a **fresh
cluster cannot produce a `410`** (bet 1(a)) and a **sequential submission loop
fabricates a latency ramp** (bet 2).

### Bet 1 — a single long-lived watch can be made reliable → **HOLDS WITH CONSTRAINT**

The constraint is the recipe, and the recipe is below, concrete enough for T107 to
implement without re-deriving it.

**(a) A stale `resourceVersion` is reported *inside the watch stream* — but only
once the watch cache has aged past it.** On the 42-day-old k3s cluster, watching
from `resourceVersion=1` returned, as a watch event:

```json
{"type":"ERROR","object":{"kind":"Status","status":"Failure",
 "message":"too old resource version: 1 (9371596)","reason":"Expired","code":410}}
```

On a **five-minute-old kind cluster the identical request produced no error and no
data** — the API server accepted the watch and sat silent, because the watch cache's
ring buffer had not yet wrapped. After ~600 pod events in the namespace the same
request produced the 410. Two consequences: a client that only handles
connection/transport failures **will not notice a 410**, because it arrives as a
decoded event with `type == "ERROR"`; and **an interruption test that has not first
aged the watch cache proves nothing** — such a run must be reported *inconclusive*,
never as a pass. T112's CI job must generate the events before asserting the 410.

**(b) The `resourceVersion` inside the 410 message is not a resume point.** Run A
read `too old resource version: 1 (9371596)` as naming "the current
resourceVersion", and that reading is **wrong**. On kind, a `LIST` issued in the same
second returned `resourceVersion=2534` while the 410 said `(972)`, and `(972)` was
still the number minutes later after the cluster had advanced past 2800. It is the
watch cache's **oldest retained** bound, not the head. **Resuming a watch from it
would skip or replay transitions.** Re-list instead.

**(c) A stale `resourceVersion` on a *LIST* succeeds.** Confirmed on both substrates:
the `rv=1` that expires a watch returns a normal `PodList` carrying a fresh
`metadata.resourceVersion`. **Re-list-then-watch cannot lose a transition**, because
the LIST returns current state, not a delta.

**(d) `BOOKMARK` events arrive, but far too slowly to be a heartbeat.** With
`allowWatchBookmarks=true` on an idle kind namespace, exactly **2 bookmarks in a
100 s window** (at 59.4 s and 98.5 s) — roughly one per 40–60 s. Run A saw 2 in a
10 s window on a busy cluster, which is the same mechanism reading differently: the
API server emits a bookmark when the resource version advances, so cadence tracks
*cluster activity*, not wall time. A bookmark is therefore a useful **cheap resume
point** and a **useless liveness signal**.

**(e) A *future* `resourceVersion` produces a silent stall**, on both substrates:
`resourceVersion=999999999999` returned no data and no error for the whole window.

**(f) A network interruption produces a silent stall too.** Blackholing the control
plane with `docker pause` for 25 s produced **no error of any kind** in either the
low-level stream or `runtime::watcher` — pure silence, then normal delivery on
unpause with no re-list. This is the failure mode ADR 115 §3 named, observed
directly: silence from a broken watch is indistinguishable from silence from an idle
cluster, and (d) says bookmarks will not close the gap inside a useful bound.
**Stall detection is load-bearing, not defensive.**

**(g) An API-server restart is surfaced, and kube-rs's automatic recovery is correct
but violently impolite.** Stopping the `kube-apiserver` container produced, at the
low level, exactly one error item — `Error reading events stream: ServiceError: error
reading a body from connection` — followed by a clean end of stream.
`kube::runtime::watcher` recovered on its own and **missed nothing** (pods created
during and after the outage were both delivered), but it did so by issuing **1120
reconnect attempts in 1.37 seconds** — 922 of them inside a single second — all
`failed to start watching object: ServiceError: client error (Connect)`. The bare
`watcher()` stream has **no backoff**. Against a real API server that is recovering,
this is the client hammering the thing it is waiting for.

**(h) A transient `403` occurs during API-server startup.** While the API server was
coming back, two watch attempts failed with `pods is forbidden: User
"kubernetes-admin" cannot watch resource "pods" ... Forbidden (403)` before
authorization was fully loaded. An executor that treats `403` as a permanent
authorization failure will abort a run over a five-millisecond window.

**(i) Recovery ended in a 410 and an automatic re-list.** The last error before
normal service resumed was `too old resource version: 2589 (2597): Expired`,
immediately followed by a fresh list-then-watch. The 410 path is not exotic — it is
the *ordinary* consequence of an outage longer than the watch cache's coverage.

#### The reconnect recipe T107 must implement

An **error taxonomy** with four classes, because they need different handling:

| Class | How it appears | Action |
|---|---|---|
| **Expired (410)** | a decoded `WatchEvent::Error` with `code: 410`, `reason: "Expired"` — *not* a transport error | **re-LIST** (ignore the RV in the message, per (b)), reconcile against current state, watch from the LIST's RV |
| **Transport / stream end** | one `Err` item then `Ok(None)`; `ServiceError: error reading a body from connection`, `client error (Connect)` | reconnect from the last **bookmarked** RV; fall back to re-LIST if that RV is also expired |
| **Transient authz (403) / 5xx** | an `ApiError` during API-server startup | retry with backoff; **never** fatal without a repeat count and an elapsed bound |
| **Silence** | no event, no error | indistinguishable from idle — only a **client-side bound** detects it |

And five rules:

1. **Inspect decoded events for `type == "ERROR"`.** A transport-only error handler
   misses every 410.
2. **Recover by re-listing, not by trusting the 410's resourceVersion** — (b).
3. **Request bookmarks and track the bookmarked RV** as the cheap reconnect point,
   but do not schedule anything off their arrival — (d).
4. **Wrap the watch in backoff.** `kube::runtime::watcher` alone will issue ~800
   reconnects per second at an unavailable API server. T107 must apply kube-rs's
   `WatchStreamExt::backoff` (or its own) — the same defect class T102 owns for
   retries, at the observer instead of the runner.
5. **Bound silence and reconcile on expiry.** With bookmarks at ~1/minute the bound
   cannot be short, so the reliable liveness signal is a **periodic LIST
   reconciliation** the observer runs regardless of the watch, which also repairs any
   transition lost to a bug. Because dagr's terminal state is reconstructed from the
   shard rather than from the event, a late reconciliation is a *latency* cost and not
   a *correctness* one — which is exactly why ADR 115 §3's no-callback design
   tolerates this and a callback design would not.

**Client behaviour is now established, not assumed.** Run A's caveat — that (a)–(e)
are server-side facts and the client's behaviour "must be verified in T107" — is
discharged here for kube-rs: it surfaces the 410 in-stream as `WatchEvent::Error`, it
ends the low-level stream on a transport error, and `runtime::watcher`
re-lists-then-watches correctly and loses nothing. Only its **backoff** is missing.

### Bet 2 — pod startup latency is tolerable → **HOLDS WITH CONSTRAINT**

Client-side wall clock from "create issued" to "the pod is observed non-`Pending`",
which is what an executor actually experiences and assumes no clock sync with the
node. Every pod submitted **concurrently**, observed by **one shared watch** started
**before the first submit** — the ADR 115 §2 shape, and the only methodology that
does not fabricate a ramp (below). Seconds. Run B, kind, `busybox:1.36`.

| Condition | n | min | p50 | p99 | max | stdev |
|---|---|---|---|---|---|---|
| warm, n=1 | 1 | 0.759 | 0.759 | 0.759 | 0.759 | — |
| warm, n=10 | 10 | 0.899 | 0.911 | 0.923 | 0.923 | 0.008 |
| warm, n=50 | 50 | 1.125 | 2.297 | 3.437 | 3.437 | 0.765 |
| cold pull, n=1 | 1 | 2.619 | 2.619 | 2.619 | 2.619 | — |
| cold pull, n=10 | 10 | 2.856 | 4.865 | 6.887 | 6.887 | 1.308 |
| cold pull, n=50 | 50 | 2.936 | 13.028 | 24.186 | 24.186 | 6.239 |

Run A's figures, from a *remote* cluster with real network latency, measure the same
quantity and agree on the shape:

| Condition (Run A, k3s) | n | min | p50 | p99 | max | stdev |
|---|---|---|---|---|---|---|
| cold image pull | 1 | 3.08 | 3.08 | 3.08 | 3.08 | — |
| warm, sequential singles | 3 | 2.09 | 2.31 | 2.39 | 2.39 | 0.16 |
| warm, concurrent fan-out | 10 | 2.12 | 2.49 | 2.50 | 2.50 | 0.19 |

**Headline.** Warm placement costs **~0.8–0.9 s co-located and ~2.3 s across a
network**, and is essentially flat to n=10. It degrades gently to n=50 warm (p50
2.30 s, p99 3.44 s) and **badly** to n=50 cold (p50 13.03 s, p99 24.19 s).

**The constraint: the image pull, not the scheduler, is the tail.** Cold and warm
differ by 1.9 s at n=1 and by **20.7 s at p99 for n=50**, even though containerd
de-duplicates the concurrent pull of a single image — so the cost is the pull plus
serialised container creation on one node, and it does not amortise across a fan-out.
Pre-pulled images and pinned digests are therefore a **requirement** of the remote
executor's operational story, not a tuning tip: T112's CI job must load the image
into the node (`kind load docker-image`) before it measures anything, and the
authoring guidance must say that a wide remote fan-out onto a cold node pays a
double-digit-second p99.

**A methodology correction worth keeping**, recorded by Run A and honoured by
construction in Run B: Run A's first fan-out harness submitted pods sequentially and
began observing only after the last submit, producing an apparent latency *ramp*
(1.50 → 4.53 s, stdev 1.08) that looked like cluster degradation under concurrency.
It was the harness — each `kubectl apply` costs ~0.3 s of process spawn, so an early
pod's measured latency absorbed the whole submission loop. Run B submits through the
API concurrently and observes continuously, and warm n=10 came out at stdev
**0.008 s**. **The cluster was never the bottleneck.** Any future benchmark must
submit concurrently and observe from before the first submit.

**"Running" and "the entrypoint is executing" are the same observation.** The ticket
asks for both. From an out-of-cluster orchestrator they are **not separable**: in all
122 measured pods the first non-`Pending` status update already carried a
`containerStatuses[].state.running`, so the two series are identical to the
microsecond. The only distinct signal is the node-clock `state.running.startedAt`,
which is 1-second-granular and agreed with the client-side figure (creation
`15:14:34Z` → `startedAt` `15:14:35Z` against a 0.9 s warm client measurement). T107
should not build a separate "entrypoint started" waiter; there is nothing extra to
wait for.

**Consequence for the spec.** `arch.md` budgets framework overhead at under one
millisecond per node. Remote placement costs **~0.9 s (co-located) to ~2.3 s (across
a network) per node attempt** — three orders of magnitude more, and entirely outside
dagr's control. This is the measured basis for ADR 115's decision to scope that
budget to local execution and report remote start latency separately, and for the
authoring guidance that remote placement pays off only when a node's own work
dominates ~1–2.5 s.

### Bet 3 — the shard path reports terminal state even when the pod dies badly → **HOLDS WITH CONSTRAINT**

Run A left this **PARTIAL** and operator-blocked: the reference cluster offered no
object store and no RWX class, so no pod could write a shard. Run B resolves it. A
`kind` `extraMounts` host directory mounted into the pod at `/dagr-blobs` is a real
shared read-write volume — the configuration ADR 115 §8 calls the local blob backend
— so the pod writes and the orchestrator reads the same bytes.

Each pod ran a writer that emits one event-stream record at a time and **splits each
record across a gap**, so a kill lands *mid-record* rather than tidily between
records. All four kills did land mid-record (final byte `,` in every surviving
shard). The fold is the shipped one: `dagr fold` over `dagr-artifact`'s
`fold_stream`.

| Mode | On disk | Fold result | Orchestrator conclusion |
|---|---|---|---|
| **OOM** past a 64Mi limit | 3454 B; 13 whole records (1 `run-started` + 12 `attempt-outcome`); a 13th truncated mid-key | **exit 0**, `interrupted: true`, 12 attempts recovered, trailing partial discarded | pod `phase=Failed`, **container** `reason=OOMKilled`, `exitCode=137`; every completed attempt readable — terminal state reported |
| **Evicted** past a 32Mi ephemeral-storage limit | 13706 B; 57 whole records; 58th truncated | **exit 0**, `interrupted: true`, 56 attempts recovered | pod `phase=Failed`, **pod** `reason=Evicted` + `message="Pod ephemeral local storage usage exceeds the total limit of containers 32Mi"`, container `reason=Error`, `exitCode=137` — terminal state reported |
| **SIGKILL** (`crictl stop -t 0`) | 5784 B; 23 whole records; 24th truncated | **exit 0**, `interrupted: true`, 22 attempts recovered | pod `phase=Failed`, container `reason=Error`, `exitCode=137` — terminal state reported |
| **killed before it writes at all** | **file absent** | n/a — nothing to read | pod `phase=Failed`, container `reason=Error`, `exitCode=137`; the *platform* reports terminal state, the shard reports nothing — see the rule below |

**The single-trailing-partial tolerance is exactly sufficient, and exactly at its
limit.** All three mid-write kills left precisely **one** truncated trailing record,
which `fold_stream` discards while marking the artifact `interrupted`. Appending a
second truncated record turns it into a hard `FoldError::CorruptRecord` (exit 2). A
badly-killed pod produces one partial because a kill happens once; the tolerance is
not a coincidence, and it does not need widening.

**⚠ The replay path can destroy that tolerance — a constraint on T106 and T107.**
The tolerance is for a *trailing* partial. Concatenating a partial shard into the
orchestrator's stream and then writing **one more record** makes the partial a
*non-final* corruption, and the fold refuses the whole stream:

```
cannot fold event stream: corrupt event-stream record at line 13 (not the tolerated trailing partial)
```

exit code 2 — an entire run's artifact lost to one truncated line. **The orchestrator
must never byte-concatenate a shard.** It must parse the shard, drop an incomplete
trailing record, and re-emit whole records through the existing `AttemptEventSink`,
which re-numbers `seq` and keeps the stream single-writer — what ADR 115 §3 already
says, now with a demonstrated cost for doing otherwise.

**The rule for "terminal phase, no readable shard".** The executor **must** treat a
pod that reached a terminal phase with no readable shard as a **failed attempt with a
diagnostic**, never as a hang and never as a success: synthesise the attempt outcome
from the pod status alone (`phase`, pod `reason`, container `reason`, `exitCode`),
attach the platform's reason string as the diagnostic ADR 115 §6 already provides
for, and charge the attempt to the correct budget — infrastructure if the container
never ran, `NodePolicy::retries` if it did. It must **not** wait for a shard that will
never appear, and it must **not** infer success from a `Succeeded` phase without one,
because the shard is the only record of what the task actually produced.

**Both `reason` fields must be read.** `OOMKilled` appears on the **container** status
with an empty pod `reason`; `Evicted` appears on the **pod** with the container
reporting a generic `Error`. An executor reading only one of the two loses half the
diagnostics — a concrete refinement of ADR 115 §6's "terminal classification comes
from pod/Job status, not container exit codes".

**Blob round-trip — measured, both directions, content-identical.** The ticket's
third named subject, hand-rolled through the same shared volume (T103's `Payload` and
T104's port are out of scope here):

| Direction | Size | Time | Integrity |
|---|---|---|---|
| orchestrator → pod (`local → remote` edge) | 1 MiB / 64 MiB | pod-side read 0.00 s / **0.02 s** | md5 identical in the pod |
| pod → orchestrator (`remote → local` edge) | 1 MiB / 64 MiB | pod-side write + `sync` 0.00 s / **0.10 s** | md5 identical on the host |

64 MiB makes the full round trip in **~0.12 s**, against a ~0.9 s warm pod start. The
data path is **not** the cost of remote execution; the pod start is. ADR 115 §8's
"place contiguous subgraphs remotely" guidance is therefore about *pod count*, not
about bytes.

### Findings outside the three bets

- **⚠ A pre-start failure never reaches a terminal phase — confirmed on both
  substrates, and there are *three* detection surfaces, not one.** Run A found an
  unpullable image sitting in `Pending` with `waiting.reason=ImagePullBackOff` and
  **no `terminated` state at all**; kind reproduces this exactly. Run B adds a second
  class: an unschedulable pod reports **`status.conditions[PodScheduled].reason =
  Unschedulable`** with a message (`0/1 nodes are available: 1 Insufficient memory`)
  and has **no `containerStatuses` entry whatsoever**, so a waiting-reason check alone
  will not see it. **T108's stated detection mechanism is wrong as written** — a
  runner awaiting a terminal phase waits forever. It must poll for:
  1. `status.phase == Failed` with pod `reason` (e.g. `Evicted`) — post-start;
  2. `containerStatuses[].state.waiting.reason` in a known-fatal set
     (`ImagePullBackOff`, `ErrImagePull`, `CreateContainerConfigError`,
     `InvalidImageName`) — pre-start, image;
  3. `conditions[PodScheduled].status == False` with `reason == Unschedulable` —
     pre-start, placement;

  and apply **its own bound** to 2 and 3, because the platform retries indefinitely
  rather than failing the pod. All of these are the *infrastructure* budget of
  ADR 115 §6, not `NodePolicy::retries`.
- **A task's own exit code survives the pod boundary** (Run A observed `exit 7`
  verbatim), so T106's plan to map dagr's `ExitCode` table through the pod boundary
  works. Note the collision it must handle: **`137` is produced by both an OOM kill
  and an external SIGKILL**, and only the `reason` field separates them.
- **⚠ No RWX storage class exists on the k3s reference cluster.** It offers exactly
  one class — `civo-volume` (Civo CSI, `WaitForFirstConsumer`) — and it is **RWO**.
  T104's plan treats "a shared read-write-many volume" as a legitimate production
  configuration for the local blob backend; on that cluster the configuration is
  **unavailable**, making an object store mandatory rather than optional. T104 should
  state RWX as a *cluster capability to check*, not an assumption, and T110's S3
  backend moves onto the critical path for anyone on a CSI without RWX. (kind's
  default class is `rancher.io/local-path`, also not RWX — the host mount used here is
  a single-node convenience, not a production pattern.)
- **No in-cluster registry**, on either substrate. A pod running a real dagr binary
  needs one (or an external one) — a prerequisite T112's CI job must provision rather
  than assume. On kind, `kind load docker-image` covers it, and is also what the
  cold-pull constraint above requires.
- **Out-of-cluster orchestration works exactly as ADR 115 §3 predicted.** Run A's
  measurements were all taken from a laptop against a remote cluster over kubeconfig,
  with no inbound reachability and no tunnel — the "iterate locally, execute remotely"
  premise is confirmed, not merely argued.

### Client choice — kube-rs

**Chosen: `kube` 1.1.0 + `k8s-openapi` 0.25 with `rustls-tls`**, which is what Run B's
probe was built on, so the choice is made against measured behaviour rather than a
survey. It is the only Rust client with a maintained `runtime::watcher`, and bet 1
shows that watcher's re-list-then-watch recovery is **correct** (it lost no transition
across an API-server restart) — leaving T107 to add backoff and stall detection rather
than to write reconnection from scratch. Hand-rolling the watch loop over a plain HTTP
client would mean re-deriving (a)–(i) in dagr's own code, which is the outcome this
ticket exists to avoid.

**Dependency and licence impact, for T107's `deny.toml` work:**

- **144 transitive crates** are reachable from `kube` alone (145 for the whole probe;
  181 lockfile entries including build and dev edges) on
  `--filter-platform aarch64-apple-darwin`. dagr's current lockfile is **203
  packages**, so the quarantined tree roughly **doubles** it — which is why ADR 115's
  containment (opt-in crate, default-off feature, `dagr-core` untouched, `cargo build
  --all` and `--no-default-features` reaching neither) is the whole story.
- **Exactly one new SPDX id is required: `Zlib`.** Running dagr's *current*
  `deny.toml` against the probe's lockfile rejects precisely one crate:

  ```
  rejected: license is not explicitly allowed — Zlib
    foldhash v0.1.5 └── hashbrown v0.15.5 └── kube-runtime v1.1.0 └── kube v1.1.0
  ```

  Everything else in the 144-crate tree resolves into the existing allow-list
  `["MIT", "Unicode-3.0", "Apache-2.0", "BSD-3-Clause", "ISC"]`. The expressions
  encountered are `MIT OR Apache-2.0` (74), `MIT` (37), `Apache-2.0 OR MIT` (12),
  `Apache-2.0` (7), `Apache-2.0 OR ISC OR MIT` (3), `ISC` (2), `Unlicense OR MIT` (2),
  `MIT/Apache-2.0`, `Apache-2.0 OR BSL-1.0`, `(MIT OR Apache-2.0) AND Unicode-3.0`,
  `BSD-2-Clause OR Apache-2.0 OR MIT`, `Apache-2.0 AND ISC` (`ring`), `BSD-3-Clause`,
  and `Zlib`. **No copyleft anywhere.** T107 adds one id, with `foldhash` named as its
  sole justification, in the one-line-per-id style the file already uses for `libsql`.
- **`cargo audit` is clean** — 0 advisories across the tree, so no advisory ignores are
  needed. `cargo deny check bans` reports **4 duplicate-version warnings**, which the
  repo's `multiple-versions = "warn"` already tolerates.

**Three build traps T107 will hit, all found the hard way here:**

1. **`k8s-openapi` must be pinned to the exact version `kube` depends on.** Declaring a
   different major (`0.26` against kube's `0.25`) resolves *both* into the graph; the
   feature that selects a Kubernetes version applies only to the copy you named, and
   the other copy's build script **panics**: `None of the v1_* features are enabled on
   the k8s-openapi crate`.
2. **`kube`'s `rustls-tls` feature alone does not install a crypto provider.** The
   first TLS handshake panics inside `rustls` 0.23 with `Could not automatically
   determine the process-level CryptoProvider`. A direct `rustls` dependency with
   exactly one provider feature plus an explicit `install_default()` at start-up is
   required — and in a *library* crate that is a real API-design question, because a
   panic on first use is not an acceptable failure mode for an opt-in feature.
3. **`WatchParams::timeout` is validated client-side at `< 295s`.** Anything larger
   fails the request before it leaves the process.

### Open questions — resolved

- **Which local cluster does CI use? → `kind`,** and T112's job must match. Reasons: it
  is what Run B measured, so the recorded numbers and the CI numbers are the same
  measurement; `kind create cluster` took **2m46s** on this machine including the cold
  node-image pull; `kind load docker-image` solves the registry gap the cold-pull
  constraint makes mandatory; and `extraMounts` supplies the shared blob volume the k3s
  reference cluster could not (no RWX class). **kind and k3s agree on every server-side
  semantic** tested — 410 shape, LIST-with-stale-rv, bookmarks, future-rv stall,
  `ImagePullBackOff` never reaching a terminal phase — and **disagree on two
  operational facts**, both of which change how a CI assertion must be written: a
  **fresh** cluster cannot produce a 410 until its watch cache has aged (bet 1(a)), and
  **bookmark cadence tracks cluster activity**, so an idle single-node cluster emits
  them ~15× less often than a busy multi-node one (bet 1(d)). A CI test that waits for
  a bookmark, or that asserts a 410 without first generating events, will be flaky on
  kind and green on k3s.
- **Is a 50-pod concurrency measurement meaningful on a single-node dev cluster? →
  Partly, and the limit is stated rather than extrapolated.** It was measured (the
  table above) and it is meaningful for exactly one thing: **kubelet and containerd
  serialisation on one node**, which is what produces the warm p99 of 3.44 s and the
  cold p99 of 24.19 s. It measures **nothing** about multi-node scheduling latency,
  bin-packing, or scheduler throughput, because there is one node and no bin-packing
  decision to make. Those remain **unmeasured**; a graph that fans out to 50 remote
  nodes against a real multi-node cluster should be re-measured before `arch.md` quotes
  a number for that shape. Run A's independent limit — it declined the 50-pod fan-out
  because the reference cluster's worst node was at 102% memory and the measurement
  could have evicted other people's work — stands as the reason the figure was not
  taken there.

### Verdicts

| Bet | Verdict |
|---|---|
| 1 — one reliable long-lived watch | **HOLDS WITH CONSTRAINT** — the recipe in (a)–(i); kube-rs recovers correctly but needs backoff, a silence bound, and a periodic LIST reconciliation |
| 2 — startup latency tolerable | **HOLDS WITH CONSTRAINT** — ~0.9 s warm co-located, ~2.3 s warm remote, flat to n=10; a **cold** 50-wide fan-out costs a p99 of 24.2 s, so pre-pulled images are a requirement, and multi-node scheduling latency is still unmeasured |
| 3 — shard reports state under bad deaths | **HOLDS WITH CONSTRAINT** — all four kill modes report terminal state and leave at most one trailing partial, which the shipped fold tolerates; the constraints are the replay discipline, the terminal-phase-no-shard rule, reading both `reason` fields, and the three pre-start surfaces that never reach a terminal phase |

**No bet is REFUTED, so no ADR 115 section reopens.** §2 (per-node pods, one shared
watch), §3 (no callback), §6 (two retry budgets, diagnostics as strings) and §8 (one
blob backend) all survive contact with a real cluster. Four downstream tickets take
**corrections**, none of which re-decides anything the ADR settled:

- **T107** — add backoff to `runtime::watcher`, bound silence, reconcile by periodic
  LIST, never resume from a 410's resourceVersion, and allow `Zlib` in `deny.toml`.
- **T108** — detect pre-start failure on three surfaces (waiting-reason, `PodScheduled`
  condition, pod `reason`), not on a terminal phase; read both `reason` fields; do not
  disambiguate an OOM kill from a SIGKILL by exit code 137 alone.
- **T106** — make the shard self-describing and **re-emit parsed records**; never
  byte-concatenate a shard into the stream.
- **T104** — treat RWX as a cluster capability to check, not an assumption.

**What this spike deliberately did not do.** It did not run a real dagr DAG in a pod:
no pod-side code exists yet (that is T106), and doing so needs an image published to a
registry the cluster can reach. It did not measure multi-node scheduling latency. Both
are recorded as prerequisites for **T112** rather than approximated here — an
end-to-end proof built on stand-ins asserts less than it appears to, and the gate's
whole value is that it cannot.
