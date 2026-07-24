# Metric naming and units convention (C23)

> **Status:** normative convention reference for C23 node metrics, authored by
> ticket T44 (ticket 055,
> [`docs/implementation/055-T44-node-metrics.md`](../implementation/055-T44-node-metrics.md)).
> Cited by the built-in metric names in
> [`crates/core/src/metrics.rs`](../../crates/core/src/metrics.rs) (`dagr_core::metrics`).

Node metrics (arch.md [`### C23 · Node metrics`](../arch.md)) are an **open,
unschematized** set of per-attempt measurements. "Open" is not "lawless": this
file is the documented naming and units convention every measurement — task and
framework — follows. It is a **checked-in, review-owned reference**, never a
runtime registry or a metadata store (that boundary is permanent; arch.md "When
not to use this").

## 1. Values are numeric

Every metric value is a **number** (`dagr_core::metrics::MetricValue`, a wrapped
`f64`). The run artifact carries them in an open numeric map
(`schemas/run/v1.schema.json`: the `attempts[].metrics` object,
`additionalProperties: { "type": "number" }`). There is no string, boolean, or
nested value — the attach API takes `impl Into<MetricValue>`, which only the
numeric primitives implement, so a non-numeric value fails to compile.

## 2. Units live in the name, as a suffix

A metric carries its unit as the **last `_`-separated token of its name**, so a
consumer reads the unit from the name alone with no side table:

| Suffix      | Unit                        | Example                     |
|-------------|-----------------------------|-----------------------------|
| `_bytes`    | bytes                       | `rows_read`? no — `bytes_spilled`, `dagr.peak_memory_bytes` |
| `_ns`       | nanoseconds                 | `dagr.phase.executing_ns`   |
| `_threads`  | a thread count              | `dagr.permit.compute_threads` |
| `_count`    | a dimensionless count/flag  | `dagr.metrics.dropped_entries_count` |
| `_entries`  | a count of entries          | (reserved; counts use `_count`) |

A pure count that has no physical unit uses `_count`. A name whose measured
quantity *is* its unit (e.g. `rows_read`, `groups_formed`, `entities_resolved`)
names the counted thing; when a count needs disambiguation from a rate or a
size, append `_count`.

Task examples that follow the convention: `rows_read`, `rows_written`,
`bytes_scanned`, `bytes_spilled`, `groups_formed`, `entities_resolved`.

## 3. The `dagr.` prefix is reserved for framework metrics

Names beginning with `dagr.` (`dagr_core::metrics::RESERVED_PREFIX`) are
**reserved for the framework**. A task attaching a metric under this prefix fails
**loudly at attach time** (`MetricError::ReservedPrefix`, naming the offending
metric) and the value is not recorded. A name that merely *contains* `dagr.`
mid-string (e.g. `my_dagr.metric`) does **not** start with the prefix and is
accepted.

### Built-in framework metrics (all under `dagr.`)

| Name                                  | Meaning                                          |
|---------------------------------------|--------------------------------------------------|
| `dagr.peak_memory_bytes`              | Allocator-attributed peak memory for the attempt |
| `dagr.permit.<unit>` (e.g. `dagr.permit.memory_bytes`, `dagr.permit.compute_threads`) | The admission-permit sizes the attempt was granted (C12; reported, not sized here) |
| `dagr.phase.<phase>_ns` (e.g. `dagr.phase.executing_ns`, `dagr.phase.permit_wait_ns`) | Per-phase timings (observational) |
| `dagr.metrics.truncated_count`        | 1 if this attempt's task metrics were truncated, else 0 |
| `dagr.metrics.dropped_entries_count`  | How many task entries the entry-count cap dropped |
| `dagr.metrics.dropped_bytes_count`    | How many encoded bytes the byte-size cap dropped  |

## 4. Caps and deterministic truncation

Each attempt's **task** metrics are capped at **128 entries** and **16 KiB
encoded** (`MAX_ENTRIES`, `MAX_ENCODED_BYTES`). Encoded size is a deterministic,
serialization-independent proxy: the sum over entries of
`name.len() + 8` (UTF-8 name bytes plus a fixed 8-byte budget for the `f64`
value, `VALUE_ENCODED_BYTES`).

Overflow is truncated by a **deterministic, order-independent** rule: keep the
**lexicographically-smallest names** up to the caps. The same set of metrics
attached in any order yields the same survivors and the same dropped set. The
truncation's occurrence and extent are recorded as the framework metrics above,
which are added **on top of** the task caps and never re-trigger a cap violation.

## 5. Determinism vs observation

Metric **names** and the **truncation** rule are deterministic. Timing and
peak-memory **values** are *observational* — they reflect a real execution and
are not reproducible across runs. They appear in the run artifact but are
**never** an input to a structural or policy fingerprint (C21): the fingerprint
is over the graph and policy, not over an execution's measurements.
