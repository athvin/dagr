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

It also names **no async runtime**. ADR 004 places tokio in the crate that owns
the run loop and requires every other crate to justify a runtime edge or not have
one; this crate does not have one. The port's two calls hand back anonymous
futures, the discipline is a state machine over a monotonic offset its caller
supplies, and the fake is a queue behind a `std` mutex. The **task** that owns the
one watch, its stall/backoff timer and its per-attempt waiters is
`dagr_cli::pod_observer` — "one watch per orchestrator *process*" is a property of
that process.

## What it is

- **`ObserverCore`** — the reconnect discipline, the demultiplexing and the
  exactly-once bookkeeping as a **deterministic state machine**: an input and a
  monotonic offset in, the deliveries to route and the next action out. No I/O, no
  clock, nothing spawned, so every reconnect scenario is a sequence of function
  calls. Driven by one task over one watch, it routes phase transitions to
  per-attempt waiters keyed by `(run id, node, attempt)`, and a terminal
  transition reaches its waiter **exactly once**, however many times the API
  server reports it and however many reconnects happen in between. One watch per
  process is a rule, not a tuning choice: a watch per pod multiplies API-server
  connections by graph width and gives every node its own reconnect bug.
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
- **The executor's pure half** — what pod to ask for, when a pod has already
  failed *before it started*, and what a terminal pod's status means. The pod
  never restarts itself, so the platform can never duplicate an attempt, and a
  configuration asking it to retry is refused naming the hazard. A pre-start
  failure is read off **two** surfaces — a known-fatal container waiting reason,
  and an unschedulable `PodScheduled` condition that has no container status at
  all — because a pod that never starts reaches **no terminal phase**: an
  unpullable image sits in `Pending` indefinitely while the platform retries the
  pull, so a caller awaiting a terminal phase waits forever. Terminal state comes
  from pod status with **both** `reason` fields read (an out-of-memory kill
  appears on the container, an eviction on the pod), and every reason travels as
  a **diagnostic string**: dagr's nine-state terminal taxonomy is closed and
  gains no member from anything the platform says.

## What it is not

It is **not** a server, a controller, or a coordinator. dagr opens no inbound
port under any executor: this crate makes **outbound** calls to an API it does
not own, and nothing hands off to it.

It **describes** a pod and **classifies** one, and it does neither of the things
that need a process: it never submits, waits, retries, reads a shard or replays
anything. Those belong to `dagr_cli::k8s_runner`, which owns the attempt loop and
the two retry budgets, for the same reason the observer's task lives there —
that is where the orchestrator process and its runtime are. Orphan adoption
across a restart, and log streaming, are separate later concerns and are not
here either.

It is also **not** a runtime, and does not bring one. Nothing here spawns, sleeps
or reads a clock; a caller that owns a runtime drives it.

## RBAC

`manifests/pod-observer-rbac.yaml` ships the least privilege the observer needs:
`get` / `list` / `watch` on `pods`, in **one** namespace, as a namespaced `Role`.
No `ClusterRole`, no cluster-wide read, and no verb that changes anything.
