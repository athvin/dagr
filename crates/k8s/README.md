# dagr-k8s

The **shared pod observer** — **one** Kubernetes watch per orchestrator process
over one run's pods, demultiplexed to per-attempt waiters, with the reconnect
discipline that makes a long-lived watch survivable and the label/annotation
identity encoding a pod carries.

This crate is **opt-in** and **quarantined**. It has **no dependency edge onto
`dagr-core`** — the same boundary shape `dagr-render`, `dagr-metastore` and
`dagr-blob` keep — and the Kubernetes client itself sits behind a **default-off
`client` feature**, so a plain `cargo build --all` and a
`--no-default-features` build compile **no HTTP or TLS stack at all**.
`dagr-cli` reaches this crate only behind its own default-off `k8s` feature.

## What it is

- **`PodObserver`** — one spawned task owning **one** watch, selected by label,
  routing phase transitions to per-attempt waiters keyed by
  `(run id, node, attempt)`. A terminal transition reaches its waiter **exactly
  once**, however many times the API server reports it and however many
  reconnects happen in between. One watch per process is a rule, not a tuning
  choice: a watch per pod multiplies API-server connections by graph width and
  gives every node its own reconnect bug.
- **The reconnect discipline** — a four-class termination taxonomy (`Expired`,
  transport, transient API, **silence**), re-**list**-then-watch on a `410 Gone`,
  resume-from-the-last-known-`resourceVersion` on a transport end, exponential
  bounded backoff, and a **stall bound**: a watch that has neither errored nor
  delivered inside the bound is reconnected rather than trusted. A Kubernetes
  watch is not a reliable stream, and silence from a broken one is
  indistinguishable from silence from an idle cluster.
- **A resync that neither loses nor duplicates** — every reconnect re-reads
  current pod state, so a terminal transition that happened during the gap is
  still delivered, once.
- **Identity** — **labels** are ≤63-character **selectors only** (run id, a lossy
  node fingerprint, attempt, an owner key, a completion tombstone key);
  **annotations** are **authoritative** (full node name, pipeline, both
  fingerprints, tool version, image digest). A 36-character run id leaves no room
  for a node name inside 63 characters, so identity is read from annotations on
  every path — and a pod whose annotated structural fingerprint does not match is
  reported **foreign**, never attributed to a waiter.
- **Cluster access** — out-of-cluster (kubeconfig, for a developer's machine) and
  in-cluster (service account, when the orchestrator is itself a pod), resolved
  by one explicit precedence; neither present is an actionable error naming every
  path and variable it looked for.

## What it is not

It is **not** a server, a controller, or a coordinator. dagr opens no inbound
port under any executor: this crate makes **outbound** calls to an API it does
not own, and nothing hands off to it. It does not submit pods, build pod specs,
own a retry budget, adopt orphans, or stream logs — those are separate,
later concerns. It watches status.

## RBAC

`manifests/pod-observer-rbac.yaml` ships the least privilege the observer needs:
`get` / `list` / `watch` on `pods`, in **one** namespace, as a namespaced `Role`.
No `ClusterRole`, no cluster-wide read, and no verb that changes anything.
