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
