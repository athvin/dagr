# 122 · T107 — the shared pod observer: one watch, reconnect, identity

> **Milestone:** M10 · **Size:** L · **Type:** feature · **Components:** C16, C19
> **Branch:** `feat/t107-shared-pod-observer` · **Depends on:** T105 · **Blocks:** T108

## Why / context

ADR 115 §2 mandates **one watch per orchestrator process, never one per pod**, and
this ticket is where that lands. It is separated from the executor (T108) because the
observer is the part with genuinely hard failure modes, and it is testable against a
fake API surface without any pod submission logic in the way.

The rule is not an optimisation, it is what both reviewed implementations converged on
after shipping the alternative. Prefect's worker had per-job watching and log
streaming **removed** in favour of a single observer per process — its
`job_watch_timeout_seconds`, `pod_watch_timeout_seconds`, and `stream_output` fields
survive as *dead declarations*, which is its own cautionary tale about config
outliving mechanism. Airflow likewise runs one cluster watch. A watch per pod
multiplies API-server connections by graph width and gives every node its own
reconnect bug.

The hard part is that **a Kubernetes watch is not a reliable stream**. It terminates
on `resourceVersion` expiry (410 Gone), API-server rollout, idle timeout, and network
partition — and worse, it can stop delivering events *without* erroring, which is
indistinguishable from "nothing has changed." Since ADR 115 §3 has no inbound
heartbeat, **the watch is the only liveness signal**, so a silently stalled watch is a
silently hung run. T101's spike produces the concrete reconnect recipe this ticket
implements; the ticket does not re-derive it.

Identity is the other trap, and it is a hard platform limit rather than a design
choice. Kubernetes label values cap at **63 characters**, and dagr's run ids are
36-character UUIDv7 values *before* any node name is appended — so a label cannot
carry identity without truncating. Airflow truncates to 53 characters plus a 9-character
md5 and therefore reads identity from **annotations** on every reconciliation path.
dagr does the same: labels are forward selectors, annotations are authoritative.

## Objective

Add the observer, its reconnect discipline, and the identity encoding — with the
Kubernetes client dependency quarantined.

- Add the **Kubernetes client dependency** chosen by T101, in a **new opt-in crate**
  reachable from `dagr-cli` only behind a **default-off feature**, with no edge onto
  `dagr-core`. Extend `deny.toml` for the new licences. `cargo build --all` and
  `--no-default-features` must pull neither the crate nor an HTTP/TLS stack, and
  `dagr-core`'s runtime dependency set must stay empty.
- Implement a **`PodObserver`**: one watch per orchestrator process over the run's
  pods, selected by label, demultiplexing phase transitions to per-attempt waiters.
- Implement the **reconnect discipline from T101's findings**: classify termination
  causes, re-list-then-watch on `resourceVersion` expiry, honour bookmarks, and
  **detect a stalled watch** (a watch that has neither errored nor delivered within a
  bound) rather than trusting silence. A resync must not lose or duplicate a terminal
  transition.
- **Reconcile on resync, not just on events**: after any reconnect, the observer
  re-reads current pod state so a terminal transition that happened during the gap is
  still delivered exactly once to its waiter.
- Encode identity per ADR 115 §4: **labels** (≤63 chars, selectors only) carry run id,
  a node-name *fingerprint*, attempt number, an owner key, and a completion tombstone
  key; **annotations** carry the authoritative full node name, pipeline name, both
  fingerprints, tool version, and image digest. Provide the label-safe encoder and its
  inverse-by-annotation lookup.
- Support **out-of-cluster and in-cluster** configuration (kubeconfig for a developer's
  machine, in-cluster service account when the orchestrator itself is a pod), because
  the laptop path is the point of the feature.
- Ship the **RBAC** the observer needs — get/list/watch on pods in one namespace — as
  a reviewed manifest, least privilege, no cluster-wide read.

## Test plan (write these first — TDD)

**Reconnect and resync — the load-bearing tests, against a fake API surface**
- Given a watch terminated by `resourceVersion` expiry, when the observer reconnects,
  then it re-lists and resumes, and a terminal transition that occurred during the gap
  is delivered exactly once.
- Given a watch that stops delivering without erroring, then the observer **detects the
  stall** within its bound and reconnects — a test that fails if silence is trusted.
- Given a watch terminated repeatedly, then reconnection backs off, is bounded, and a
  permanent failure surfaces as a classified error rather than an infinite quiet
  retry.
- Given a terminal transition delivered twice by the API, then the waiter is notified
  **once** (idempotent delivery on the attempt key).
- Given two pods for different attempts of the same node, then each waiter receives
  only its own.

**Identity**
- Given a 36-character run id and a long node name, then every emitted label value is
  ≤63 characters and syntactically valid.
- Given two distinct node names that collide under label truncation, then they remain
  **distinguishable** via annotations, and the observer attributes each pod correctly —
  the test that justifies annotations existing.
- Given a pod, then its annotations round-trip the full node name, pipeline name, both
  fingerprints, tool version, and image digest.
- Given a pod whose annotations name a different structural fingerprint, then the
  observer reports it as foreign rather than attributing it to a waiter.

**Configuration and boundaries**
- Given a kubeconfig, then the client configures out-of-cluster; given in-cluster
  service-account files, then it configures in-cluster; given neither, then it fails
  with an actionable error naming what it looked for.
- Given `cargo tree -i dagr-core`, then the new crate is absent from core's
  reverse-dependency tree; given `--no-default-features` and `cargo build --all`, then
  no HTTP/TLS stack is compiled.
- Given `cargo deny check licenses`, then it passes with the new transitive
  dependencies.
- Given `scripts/check-metastore-acceptance-boundary.sh`, then it **passes** — the
  observer adds a client, never a listener, and no `*Scheduler` type.

**Shutdown**
- Given the run ending, then the observer's watch is torn down and its task does not
  outlive the run; given SIGTERM, then teardown completes inside the shutdown budget.

## Definition of done

- [ ] One `PodObserver` per orchestrator process owns a single watch and demultiplexes
      to per-attempt waiters; no per-pod watch exists.
- [ ] The reconnect discipline from T101 is implemented: cause classification,
      re-list-then-watch, bookmark handling, and **stall detection**; a resync neither
      loses nor duplicates a terminal transition.
- [ ] Labels are ≤63 characters and selector-only; annotations carry authoritative
      identity; a truncation collision is still attributed correctly.
- [ ] A pod whose annotated fingerprint does not match is reported foreign, not
      attributed.
- [ ] Out-of-cluster (kubeconfig) and in-cluster configuration both work; neither
      present is an actionable error.
- [ ] The client lives in a new opt-in crate behind a default-off feature with no edge
      onto `dagr-core`; `--no-default-features` and `cargo build --all` pull no
      HTTP/TLS stack; `deny.toml` covers the new licences; core's runtime dependency
      set is still empty.
- [ ] `scripts/check-metastore-acceptance-boundary.sh` passes unchanged.
- [ ] Least-privilege RBAC (get/list/watch pods, one namespace) ships as a reviewed
      manifest.
- [ ] The observer is torn down with the run and respects the shutdown budget.
- [ ] Tests pass on `ubuntu-latest` and `macos-latest` (fake API surface; the real
      cluster test is T112).
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, the
      rustdoc lint, and cargo-audit/deny where configured).

## Open questions

- **How is the fake API surface built?** Either the chosen client's own test facility
  or a local HTTP fixture serving watch frames. The requirement is that
  `resourceVersion` expiry, a silent stall, and duplicate delivery are all *inducible*
  — a fake that cannot produce those does not test this ticket. Decided in-PR against
  T101's client choice.
- **Stall-detection bound.** A concrete value needs T101's latency numbers; it must be
  long enough not to churn on an idle graph and short enough that a hung run is
  noticed well inside a human's patience. Recorded in-PR with the reasoning, and
  operator-overridable only if T112's demo shows it needs to be.

## Out of scope

- Submitting pods, building pod specs, retry budgets, or shard replay — **T108**.
- Orphan adoption, tombstoning, and ownership revocation — **T109** (this ticket ships
  the *keys* those mechanisms use, not the mechanisms).
- Log streaming from pods. ADR 115 leaves aggregation to the cluster, consistent with
  arch.md's refusal of push-export; the observer watches status, not logs.
- Multi-namespace watching. One namespace, least privilege; revisit only on a real
  requirement.
- Re-deriving the reconnect recipe or re-deciding one-watch-per-process (ADR 115 §2).
- Scope boundary restated: a client that watches an API it does not own adds no
  coordination and no server — the orchestrator opens no listener and nothing hands
  off to it. dagr remains not a scheduler, a distributed execution system, a
  coordinating metadata store, a web interface, a DSL, or a backfill orchestrator, and
  the graph's shape never changes at runtime.

---

## Open questions — resolved

`docs/tasks.md` carries **no `T107` entry** (it enumerates M0–M4 only), so there
are no `Q:` items beyond this file's own two. Both are answered below, together
with the decisions the implementation had to make that the ticket did not name.

### 1. How is the fake API surface built? → **A port the observer watches through, faked in-process — plus recorded frames through the real client's deserializer.**

The ticket's bar is that a `resourceVersion` expiry, a **silent stall** and a
**duplicate delivery** are all *inducible*. Two candidates were considered.

A **local HTTP fixture serving watch frames** to a real client was rejected on two
counts. It would stand up a listener inside a project whose boundary claim is that
it opens no inbound port — even in a test — and it would test the *client's*
recovery rather than dagr's, since the self-healing watcher it would have to drive
performs the re-list itself and re-emits none of the classification the ticket
requires dagr to implement.

What ships instead is a **port**: `dagr_k8s::api::PodApi`, the two primitive calls
`list` and `watch`, which is exactly the level the spike's error taxonomy is
written in terms of (an expiry as a decoded `type: ERROR` frame; a transport
failure as one error item then the end of the stream). `dagr_k8s::fake` is an
in-process implementation whose failures are scripted, so every scenario is a
function call: `expire()` for the 410, delivering nothing for the stall, the same
terminal three times for the duplicate, and a queue of failures for the bounded
retry. It is deterministic, runs in microseconds under a paused clock, and needs
no cluster on either CI platform.

The fake is kept honest about the wire by the second half: `crates/k8s/src/client.rs`'s
tests push **recorded** frames — including the verbatim 410 body the spike captured
off a real API server — through kube's own `WatchEvent<Pod>` deserializer and then
through this crate's classifier. The fake proves the discipline; the recorded frames
prove the classification is aimed at the bytes a server actually sends. The real
cluster remains **T112**'s.

### 2. Stall-detection bound. → **90 seconds**, a settable field and deliberately not an operator knob.

`DEFAULT_STALL_BOUND` is 90 s, from the spike's two measurements rather than from
a round number:

- **Long enough not to churn on an idle graph.** Bookmarks are the only thing an
  idle watch emits, and were measured at roughly one per 40–60 s on an idle
  namespace (2 in a 100 s window, at 59.4 s and 98.5 s — a worst observed gap just
  under a minute). 90 s clears that with margin.
- **Short enough that a hung run is noticed well inside a human's patience.** Warm
  placement costs ~0.9 s co-located and ~2.3 s across a network; the worst measured
  tail is a 24.2 s p99 for a cold 50-wide fan-out. 90 s is ~3.7× that worst tail, so
  real work always produces something inside the bound, and dagr acts on silence in
  under two minutes.

The asymmetry decides the rounding: being too high costs a late reconciliation
(and a late reconciliation is a *latency* cost, not a correctness one, because a
remote attempt's outcome is reconstructed from what it wrote rather than from the
event), while being too low costs one extra `list` of one namespace per bound.
Both are cheap, which is why it is `ObserverLimits::stall_bound` — settable by a
caller, exercised by the suite — and **not** a `--dagr.*` flag. It becomes one only
if T112's demo shows it needs to be, exactly as the ticket instructs.

### 3. Over the client's primitive calls, or over its self-healing watcher? → **The primitive calls**, and the licence consequence.

The spike's corrections list says *"add backoff to `runtime::watcher`"* and *"allow
`Zlib` in `deny.toml`"*. Neither happens, and the reasoning is recorded in full in
`docs/implementation/DEVIATIONS.md` and at the site (`crates/k8s/src/client.rs`).
In short: the DoD requires dagr to implement cause classification, re-list-then-watch
and bookmark handling, and to have all of it exercised against a fake — none of which
is possible with a watcher that consumes those observations and re-emits none of
them. Turning `kube`'s `runtime` feature off removes the `kube-runtime → hashbrown →
foldhash` path that was the **sole** justification for `Zlib`, so `deny.toml` gains
the audit (every crate in the new ~140-crate tree resolves into the existing
allow-list; `cargo audit` clean) rather than an id. The next ticket that reaches for
`runtime` will find the cost written down where it will look.

### 4. Where does the client quarantine live? → **Inside `dagr-k8s`, behind a second default-off feature.**

The ticket asks that *"`cargo build --all` and `--no-default-features` must pull
neither the crate nor an HTTP/TLS stack"*. A crate is a workspace member, and
`cargo build --all` builds members — so an unconditional `kube` dependency would
make that sentence false however the cli gated its edge. (This is the shape the
`metastore` feature has: `cargo build --all` does compile `libsql`.)

So the containment is doubled: `dagr-cli`'s `k8s` feature is default-off, **and**
`dagr-k8s`'s own `client` feature is default-off, with `kube`, `k8s-openapi`,
`rustls` and `futures-util` all optional behind it. The default surface of the
crate — the observer, the discipline, the port, the identity encoding, the
cluster-access resolution — compiles with `tokio` and nothing else, and
`scripts/check-k8s-feature-gating.sh` asserts the **whole-workspace** default and
`--no-default-features` resolutions contain no HTTP/TLS crate at all. That is the
literal reading, and it buys something real: a contributor who never touches remote
execution never compiles a ~140-crate tree with a C toolchain behind its TLS backend.

The consequence is that the client is *not* built by `cargo test --workspace`, so
CI compiles it two other ways: a dedicated `-p dagr-k8s --features client` test and
clippy leg, and the existing `--all-features` build on the feature-matrix job.

### 5. Is the node label a digest or a truncation? → **A truncation, and the ticket's own test is why.**

ADR 115 §4 says "a node-name *fingerprint*", and the obvious reading is a hash.
It is the wrong one here. The Test plan requires *"two distinct node names that
collide under label truncation"* to remain distinguishable via annotations — the
test that justifies annotations existing — and under a digest that collision is
not constructible, so the test would assert nothing.

The deeper reason is the same one: a digest is *also* irreversible, so it would not
let a reader recover the name either, but it would look unique, and code that reads
a unique-looking label eventually trusts it. A visibly lossy label forces every path
through the annotation, which is what ADR 115 §4 requires on every reconciliation
path. The stated consequence is that two nodes whose names share a 63-character
sanitized prefix carry the same node label — which is fine, because the label is a
selector and the selector narrows by run id anyway.

Unlike Airflow, dagr therefore appends **no** hash suffix. Airflow's md5 exists to
keep pod *names* unique; pod naming is the submitting executor's concern, not the
observer's.

### 6. Cluster-access precedence, and the explicit-but-missing case. → **`KUBECONFIG` > in-cluster > `$HOME/.kube/config`; a named file that is absent is an error.**

Explicit operator intent outranks everything. Being a pod (the three mounted
service-account files **and** `KUBERNETES_SERVICE_HOST`/`_PORT` — a token with no
service address is a leftover mount, not an environment) outranks a home file that
merely happens to be there.

`KUBECONFIG` naming a file that does not exist is a **refusal that names the file**,
not a fall-through. Silently using a different cluster than the one an operator
asked for is the same class of failure as silently running locally when `--dagr.executor
k8s` was requested, which this repo already refuses loudly. A `KUBECONFIG` that is
*set but empty* is treated as unset, because a shell that exports an empty variable
has named nothing.

### 7. What retires a waiter? → **A terminal phase or a disappearance, once, on the attempt key.**

Idempotency is on `(run, node, attempt)` and not on the event, which is what makes a
duplicate from the API, a repeat across a reconnect, and a re-read during a resync
all collapse to one notification. A pod that a full reconciliation no longer reports —
having previously been seen — is delivered as a `vanished` observation and also
retires its waiter: a pod that no longer exists will never reach a phase, and a
waiter that hangs on one is a hung run. Adoption and tombstoning, which are what
would *re*-create such a pod, are **T109**'s; this ticket ships the owner and
completion **keys** they use and reads them, and implements neither mechanism.

Observations that arrive before their waiter registers are buffered and replayed on
registration, because registering and submitting are two acts and a pod already
terminal on the first reconciliation is a real ordering.

### 8. The label/annotation key prefix. → **`dagr.io/`.**

A namespace, not a claim of DNS ownership: prefixing is what stops dagr's keys
colliding with an operator's own, and it is what one of the two reviewed
implementations does. It is one constant (`identity::KEY_PREFIX`) if it ever needs
to change, and a unit test asserts every key lives under it.
