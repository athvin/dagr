# ADR 0001 · Critical-path definition for the run summary (C22 · T43)

> **Status:** Accepted (T43, ticket 054) · **Components:** C22 · **Governs:**
> `dagr_artifact::fold` critical-path computation and its rustdoc.

## Context

The C22 run summary carries two headline numbers — **total elapsed time** and
**critical-path time** — whose *relationship* answers a single operational
question: was the run limited by its dependency **structure** or by its
**resources** (arch.md `### C22 · Run artifact`: "the first two numbers together
answer whether the run was limited by its dependency structure or by its
resources"). The number is only meaningful if what it counts is fixed, because
critical-path time is ambiguous under retries and permit waits: if queueing time
(ready-wait, permit-wait) is counted *on the path*, a run serialized behind a
small permit pool looks structure-limited even though its dependency graph is
wide and shallow — defeating the discrimination the two numbers exist to make.

The fold is a **pure function of the event stream** (arch.md C19/C22: "folded by
a function that needs no access to the original run"): it receives the stream
bytes and the graph's node roster, and **no explicit dependency-edge list**. The
stream nonetheless encodes the dependency partial order *in its timing*: a node's
`node-ready` record is emitted the instant all of its upstreams reach a terminal
state and its trigger rule fires (arch.md C11 / `dagr_core::readiness`), so a
node's `node-ready` offset is exactly the monotonic instant its slowest upstream
completed. The critical path is therefore reconstructable from monotonic offsets
alone, with no store, no live graph, and no edge list.

## Decision

**Total elapsed time** is the authoritative monotonic wall of the run: the
maximum `offset_ns` seen in the stream minus the run-start offset (0). It is
computed from monotonic offsets only, never from the informational `wall`
stamps (which the fixtures deliberately skew).

**Critical-path time** is the longest **dependency-respecting** chain of node
**executing** contributions through the executed graph, computed as a
longest-path forward pass over the timing-derived DAG:

1. **A node's contribution** `c(v)` is the **sum of the `executing` phase across
   all of that node's attempts** (retries collapse by *summing executing*). A
   never-ran (propagated-terminal) node contributes `0` but is still traversed.

2. **Ready-wait, permit-wait, and backoff are EXCLUDED** from every node's
   contribution. Only `executing` time lies on the path.

3. **Dependency edges are reconstructed from timing**, not guessed: a node `u`
   is a critical-path predecessor of `v` iff `u` reached a terminal state at an
   offset `≤ v`'s `node-ready` offset — i.e. `u` was already complete when `v`
   became ready, the necessary condition for `v` to depend on `u`. The
   critical-path value *to complete* `v` is
   `cp(v) = base(v) + c(v)`, where `base(v) = max over qualifying predecessors u
   of cp(u)` (`0` when `v` has none — a source node). Critical-path time is
   `max over executed nodes v of cp(v)`.

4. **Zombie (abandoned-but-running) time is EXCLUDED** from the path. A node's
   contribution ends at its terminal offset; the pinned overrun of a
   `timed-out`/abandoned thread is reported **only** in the summary's
   `abandoned_pinned_time_ns`/`abandoned_pinned_capacity` fields (from T42),
   never merged into critical-path time.

**Tie / parallelism handling (determinism).** When several predecessors qualify
for `base(v)`, the **maximum** `cp(u)` wins; equal `cp(u)` values are
interchangeable, so ties never affect the result. Nodes with no qualifying
predecessor (independent siblings, sources) start a fresh chain at `cp = c(v)`.
Attempts are processed in stream (`seq`) order and node contributions are
accumulated in a sorted (`BTreeMap`) order, so the computation is a deterministic
pure function of the artifact bytes — folding the same stream twice yields the
identical number.

## Why this discriminates structure- from resource-limited runs

- **Structure-limited** (a long dependency chain, admitted immediately, no
  retries): each node's `node-ready` offset follows its predecessor's terminal,
  the chain of `executing` contributions accumulates end to end, and
  critical-path time ≈ total elapsed.
- **Resource-limited** (many independent siblings serialized behind a small
  permit pool): every sibling has no qualifying predecessor, so each `cp` is its
  own lone `executing` contribution and critical-path time is the **single
  longest** node — while total elapsed, which *does* include the permit-wait the
  siblings accrued, greatly exceeds it. The gap is the resource limitation, and
  because permit-wait is excluded from the path, the gap equals the added
  permit-wait exactly.

## Rejected alternatives

- **Count ready-wait and permit-wait on the path** (queueing time is "on the
  path"). Rejected: it collapses the structure-vs-resource discrimination — a
  run serialized entirely by a one-permit pool would report critical-path ≈ total
  elapsed and read "structure-limited," which is precisely the wrong answer for
  deciding what machine to run it on. The two numbers would stop being
  independent signals.
- **Require the graph artifact's explicit edge list.** Rejected: it would make
  the fold depend on a second artifact and break C22's "needs no access to the
  original run" — the timing-derived predecessor relation is sufficient and keeps
  the fold a pure function of one stream.
- **Collapse retries to the final attempt's executing time only.** Rejected:
  retries are real re-work that lengthened the run and arch.md keeps one record
  per attempt precisely because "retries are the interesting signal, not to be
  thrown away" (C22); summing executing across attempts keeps the path honest.
  Backoff is still excluded because it is idle waiting, not work.

## Consequences

- Critical-path time is a lower-effort upper bound on the true dependency chain:
  the timing-predecessor relation admits any earlier-completed node as a possible
  predecessor, so on a graph where an unrelated node happens to finish before a
  source becomes ready the estimate can only *over*-attribute, never under. For
  the fixtures (chains, diamonds, independent siblings) it is exact. This is
  stated in the rustdoc at the critical-path function.
- The number is populated using only offsets already in the folded artifact, so
  it is recomputable from the artifact bytes with no I/O.
- The summary schema is unchanged: `critical_path_ns` already exists in
  `schemas/run/v1.schema.json`; T43 only makes it correct.
