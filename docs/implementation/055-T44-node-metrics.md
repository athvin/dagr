# 055 · T44 — C23: node metrics

> **Milestone:** M3 · **Size:** M · **Type:** feature · **Components:** C23
> **Branch:** `feat/t44-node-metrics` · **Depends on:** T16, T42 · **Blocks:** T49

## Why / context
Every attempt has to be able to report what it measured — rows read, bytes spilled, and the like — while the framework simultaneously contributes what only it can know: allocator-attributed peak memory, permit sizes, and phase timings. This ticket implements C23 (arch.md `C23 · Node metrics`), building on T16's run context (the attach surface hangs off the per-attempt context) and T42's fold function (metrics ride into the attempt record and reach the run artifact unmodified — arch.md `C22 · Run artifact`). The measured peak memory produced here is the honest per-node number that arch.md juxtaposes against declared cost (arch.md lines on declared-vs-measured cost, C12/C22). It gates T49, the M3 demo that explains a run from artifacts.

## Objective
Provide an open, unschematized per-attempt metrics facility that is lawful at its edges: numeric-only values, units carried in the metric name, a reserved framework prefix that tasks cannot write under, hard caps with deterministic recorded truncation, and framework-contributed measurements (peak memory, permit sizes, phase timings) present on every attempt regardless of what the task attaches.

Concrete pieces of work:
- An attach API on the attempt's context (T16) that lets a task record a named numeric measurement with no framework change required.
- Enforcement that the `dagr.` prefix is reserved: a task attempting to attach under it fails loudly at attach time, naming the offending metric.
- Caps of 128 entries and 16 KiB encoded per attempt, with deterministic truncation whose occurrence and extent are recorded as a framework metric under the reserved prefix.
- Framework-contributed measurements populated for every attempt: allocator-attributed peak memory, admission-permit sizes, and phase timings — present even when the task attaches nothing.
- An instrumented global allocator that attributes allocations to the running attempt via task-local state, yielding a per-node peak (what the attempt allocated, not process-wide usage), correct under concurrent nodes in one process.
- A documented naming and units convention (units as a name suffix, e.g. `rows_read`, `bytes_spilled`), followed by every built-in measurement, with the framework's own metric names all under `dagr.`.
- The collected metric set threaded into the attempt record so T42's fold carries it to the run artifact unmodified.

## Test plan (write these first — TDD)

- **Task attaches a novel metric, no framework change.** Setup: a hand-constructed attempt context. Action: the task attaches a measurement whose name is not known to the framework and never was. Expected: the metric is present in the collected set with its exact name and numeric value, and no framework enum or registry needed editing to make it accepted.

- **Numeric-only values.** Setup: an attempt context. Action: attach measurements with the supported numeric value shapes. Expected: each is accepted and stored as a number; the API surface offers no way to attach a non-numeric value (verified by the type surface, exercised by the tests actually written against it).

- **Framework metrics present with no task metrics.** Setup: an attempt that attaches nothing. Action: complete the attempt and collect its metrics. Expected: peak memory, permit sizes, and phase timings are all present under `dagr.`-prefixed names, following the documented units convention.

- **Reserved prefix rejected at attach time.** Setup: an attempt context. Action: the task attaches a metric whose name begins with the reserved `dagr.` prefix. Expected: the attach call fails loudly and immediately, the error names the offending metric, and the reserved-prefixed value is not present in the collected set. A boundary name that merely contains but does not start with the prefix is accepted.

- **Entry-count cap with recorded truncation.** Setup: an attempt context. Action: the task attaches more than 128 distinct measurements. Expected: exactly the cap's worth survive, the survivors are chosen by the documented deterministic rule (same inputs always yield the same survivors and the same dropped set — asserted by attaching in two different orders and comparing), and a framework truncation metric under `dagr.` records that truncation occurred and by how much.

- **Byte-size cap with recorded truncation.** Setup: an attempt context. Action: the task attaches measurements whose encoded size exceeds 16 KiB while staying under 128 entries. Expected: the encoded set is held at or under 16 KiB, truncation is deterministic, and the same framework truncation metric records it.

- **Truncation metric is itself accounted.** Setup: an attempt already at the caps. Action: force truncation. Expected: adding the framework truncation record does not itself push the set back over the caps (no cap-violation feedback loop), and the recorded truncation figures are consistent with what was dropped.

- **Peak memory is per-attempt, not process-wide, under concurrency.** Setup: two attempts run concurrently in one process; attempt A allocates a large, known amount and holds it, attempt B allocates a small known amount. Action: collect each attempt's peak-memory metric. Expected: A's reported peak reflects roughly A's allocation and B's reflects roughly B's — neither attempt's number is inflated by the other's live allocation, demonstrating task-local attribution rather than process RSS.

- **Peak memory tracks the high-water mark.** Setup: one attempt allocates then frees then allocates a smaller amount. Action: collect the peak. Expected: the reported peak reflects the highest point reached during the attempt, not the residual at the end.

- **Allocations outside any attempt are unattributed.** Setup: allocate on a thread with no attempt in task-local state. Action: run an unrelated attempt afterward. Expected: the outside allocations do not appear in any attempt's peak, and the allocator behaves correctly (no panic, correct memory) when no attempt is current.

- **Units-in-name convention holds for built-ins.** Setup: run an attempt and collect the framework metrics. Action: check every built-in metric name against the documented convention. Expected: each built-in name carries its unit per the convention and lives under `dagr.`; a documented convention reference exists and the test asserts each built-in conforms to it.

- **Metrics reach the artifact unmodified.** Setup: an attempt with a mix of task and framework metrics; fold its event stream with T42's function. Action: read the resulting run artifact's attempt record. Expected: every collected metric (names and numeric values) appears in the artifact byte-for-value identical to what was collected, with the task and framework entries both present and unaltered.

## Definition of done
- [ ] A task can attach a new measurement with no framework change (open, unschematized set).
- [ ] Metric values are numeric only; the API affords no non-numeric value.
- [ ] Units are carried in the metric name per a documented naming/units convention, and every built-in measurement follows it.
- [ ] Framework-contributed measurements — allocator-attributed peak memory, permit sizes, phase timings — are present on every attempt even when the task attaches none.
- [ ] The `dagr.` prefix is reserved; a task attaching under it fails loudly at attach time, the error names the metric, and the value is not recorded.
- [ ] Per-attempt caps of 128 entries and 16 KiB encoded are enforced.
- [ ] Truncation past a cap is deterministic (order-independent survivor/drop sets) and the truncation is itself recorded as a framework metric under the reserved prefix, without re-triggering a cap violation.
- [ ] Peak memory is attributed to the running attempt via task-local state, giving the per-node number (not process RSS), correct and non-cross-contaminating under concurrent nodes in one process, and tracking the attempt's high-water mark.
- [ ] The instrumented allocator behaves correctly when no attempt is current (unattributed, no panic) and installs as the process global allocator.
- [ ] Collected metrics are threaded into the attempt record and reach the run artifact unmodified via T42's fold function.
- [ ] A documented naming-and-units convention reference exists in the repo and is cited by the built-in metric names.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
None.

## Out of scope
- Rendering, coloring, or annotating metrics in diagrams (C24 / T46 / T47) and any run-summary aggregation such as critical path or peak slot residency (C22 / T43) — this ticket only produces the per-attempt metric set.
- Defining or storing the fold/artifact schema itself (owned by T42); this ticket delivers the metric payload the fold carries, not the fold.
- Logging and tracing spans, and secret redaction (C25 / T45) — metrics are a separate reporting channel.
- Declared-cost declaration and the admission-pool sizing that produces permit sizes (C12 / C5); this ticket only reads and reports the permit sizes an attempt was granted.
- Emitting metrics as a live stream, a queryable metadata store, or an external monitoring surface — dagr is not a metadata store or a scheduler; metrics live only in the attempt record and the run artifact.
- Any per-item or sub-task progress fan-out that would imply runtime graph expansion; a node that iterates internally reports progress through this same flat per-attempt metric set, and the graph shape never changes at runtime.
