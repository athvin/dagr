# dagr cookbook

> **Status:** documentation deliverable, authored by ticket **T64** (ticket 079,
> [`docs/implementation/079-T64-readme-quickstart-and-cookbook.md`](implementation/079-T64-readme-quickstart-and-cookbook.md)).
> Every entry below is backed by **compiled, run code** — the tests in
> [`crates/cli/tests/cookbook.rs`](../crates/cli/tests/cookbook.rs) (and one
> compile-fail fixture in the T8 UI harness) — so no entry can claim a behaviour
> the shipped API does not have. A renamed or removed method fails the build.

This is a cookbook of the patterns the design *forces* on authors — the shapes
you reach for precisely because dagr does not have a scheduler, a DSL, or dynamic
graph expansion. Read the [README quickstart](../README.md#quickstart) first for
the one-call [`RunnableFlow`](../crates/cli/src/run_flow.rs) seam every runnable
entry here uses: you write plain [tasks](../crates/core/src/task.rs) and a flow,
and never a scheduler or a `NodeRunner` (arch.md **C1**).

The normative vocabulary these entries lean on — the nine terminal states, the
state classes, and the closed trigger-rule set — lives in
[`docs/arch.md`](arch.md) "Vocabulary". Read it once; the entries reference it.

## Contents

1. [Fan-out inside one node](#fan-out-inside-one-node)
2. [Fan-in](#fan-in)
3. [Branch in a task: self-skip vs succeed-with-empty](#branch-in-a-task-self-skip-vs-succeed-with-empty)
4. [Incremental cursors via scratch](#incremental-cursors-via-scratch)
5. [Durable stage boundaries](#durable-stage-boundaries)
6. [The non-`Send` capture error and its fixes](#the-non-send-capture-error-and-its-fixes)
7. [Two same-typed resources via newtypes](#two-same-typed-resources-via-newtypes)

---

## Fan-out inside one node

**The rule dagr forces:** the graph's shape never changes at runtime. A task that
discovers *N* files at runtime does **not** become *N* nodes. The blessed pattern
is **one node that iterates internally with bounded concurrency**, declaring the
cost of that internal parallelism honestly (arch.md **C5**, **C12**).

Fan-out is therefore a *within-node* concern, not a graph-shape change. You bound
the internal parallelism yourself, and you tell the admission controller how much
of the machine that costs through the node's **declared cost** (`NodePolicy`):

```rust
let node = flow.register_with::<FanOutInsideOneNode, _>(
    "process-all",
    FanOutInsideOneNode { max_in_flight: 4 },
    count,
    NodePolicy::new().compute_threads(4), // the declared cost of the internal parallelism
);
```

Inside `run`, the task processes its items with a concurrency ceiling — the honest
budget it declared. The *graph* stays a single processing node no matter how many
items appear at runtime; the run's stream records one `node-terminal` for it, not
one per item. This is what keeps the artifact and the diagram legible at scale,
and it is why declared cost must be honest: the run artifact juxtaposes declared
against measured cost so a dishonest declaration is visible (**C12**, **C23**).

> **Backing test:** `fan_out_inside_one_node_bounds_internal_parallelism_and_stays_one_node`
> in `crates/cli/tests/cookbook.rs`.

---

## Fan-in

**The pattern:** many upstreams joined into one node. A joining task binds
multiple upstream handles **as a tuple**, and its input type is that tuple — so
count, order, and types are all compile-checked at once (arch.md **C3**):

```rust
let count = flow.register_source("count", &MakeCount);   // Handle<u64>
let label = flow.register_source("label", &MakeLabel);   // Handle<String>
// The join declares `Input = (u64, String)` and binds the two handles as a tuple.
let join = flow.register("join", &JoinCountAndLabel, (count, label));
```

A data-dependent node is `all-succeeded` **by construction** — the builder
typestate offers no other trigger rule — so the join fires only when *every*
upstream succeeded (**C3**, arch.md Vocabulary). The maximum input arity is
[**8**](../crates/core/src/binding.rs); beyond it, aggregate the upstream values
into a struct produced by an intermediate node and depend on that one handle.

That aggregate-into-a-struct shape is also how you run a fan-in through the
one-call `RunnableFlow::run` seam today, which drives single-input nodes: an
intermediate node produces a `struct` holding the joined values, and the consumer
depends on that one handle. The all-succeeded semantics are identical.

> **Backing tests:** `fan_in_binds_many_upstream_handles_as_a_tuple_under_all_succeeded`
> (the compile-checked tuple binding + assembled edges) and
> `fan_in_via_aggregate_struct_runs_through_the_one_call_seam` (the runnable
> aggregate shape) in `crates/cli/tests/cookbook.rs`.

---

## Branch in a task: self-skip vs succeed-with-empty

**The rule dagr forces:** branching is expressed *in the task*, not the graph
(arch.md Vocabulary). A task that decides "nothing to do" returns a deliberate
**skip**, and the skip propagates. But sometimes a downstream join *must* run even
when one branch declined — and then the branch should **succeed with an explicit
empty value** instead of skipping. The two disciplines differ, and which you pick
is a real design decision.

**Self-skip** — the branch returns `TaskError::skip(...)`. Under the default
`all-succeeded` rule the downstream is marked `upstream-skipped` and never runs.
A run whose only non-success outcomes are skips is still a **successful** run.

```rust
async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<u64, TaskError> {
    Err(TaskError::skip("no work in this partition")) // propagates as upstream-skipped
}
```

**Succeed-with-empty** — the branch returns an explicit empty value (`Option::None`,
an empty batch). The join stays alive and runs on the empty value.

```rust
async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Option<u64>, TaskError> {
    Ok(None) // the join still runs; it sees an explicit empty value
}
```

**When to choose which.** Self-skip when a declined branch *should* short-circuit
everything downstream of it (there is genuinely nothing to do, and the downstream
work is meaningless without it). Succeed-with-empty when a downstream join must
still run — a summary that reports "0 rows," a notify step that must always fire —
so the empty case is a value the join handles, not a skip it inherits.

> **Backing tests:** `branch_in_task_self_skip_propagates_a_skip_to_the_join` and
> `branch_in_task_succeed_with_empty_keeps_the_join_alive` in
> `crates/cli/tests/cookbook.rs`.

---

## Incremental cursors via scratch

**The pattern:** a task remembers a checkpoint across its own retries — and across
a resume. The **durable scratch store** (arch.md **C18**) is a small, per-node,
per-run key-value store of opaque bytes, reached through the context as
`ctx.scratch()`. A value written on one attempt is readable on the next.

Use it for cursors, high-water marks, and "I already finished the first half"
checkpoints — **not** for passing data between nodes (that is what data edges are
for). Serialization is your affair; the store holds bytes.

```rust
fn advance_cursor(scratch: &ScratchStore) -> Result<u64, TaskError> {
    // Read the high-water cursor a prior attempt wrote, if any. A scratch I/O
    // error maps (via `?`) to a RETRY-ELIGIBLE failure — disk trouble is usually
    // transient (C18).
    let prior: u64 = match scratch.get(b"cursor")? {
        Some(bytes) => std::str::from_utf8(&bytes).unwrap().parse().unwrap_or(0),
        None => 0, // no checkpoint yet: start from the beginning
    };
    let advanced = prior + 512; // do the next slice of work
    scratch.put(b"cursor", advanced.to_string().as_bytes())?;
    Ok(advanced)
}
```

The cursor written on attempt one is what attempt two reads to resume from, rather
than starting over — the whole point of a checkpoint. Scratch of a *succeeded*
node is deleted; scratch of a node that did not succeed stays in the run's
directory, which is exactly what a later resume copies forward for a re-executing
node (**C18**, **C27**).

> **Backing test:** `incremental_cursor_written_on_attempt_one_is_read_on_the_next`
> in `crates/cli/tests/cookbook.rs`, driving the real `ScratchStore` across two
> stores that share the node's namespace (the observable "attempt one" / "attempt
> two").

---

## Durable stage boundaries

**The rule dagr forces:** in-memory values cannot be *rehydrated*. The moment a
re-running consumer demands an in-memory producer's value, the producer
re-executes (arch.md **C27**). That is a real property of a compiled language, and
it is useful pressure: it pushes you to make expensive stage boundaries produce
**durable, addressable** outputs.

A **durable** node's output type implements the [`DurableOutput`](../crates/core/src/assembly.rs)
contract — it serializes a *reference* to where the real value lives (not the value
itself), and rehydrates the typed value from that reference later:

```rust
impl DurableOutput for DatasetRef {
    fn serialize_reference(&self) -> String {
        self.location.clone() // e.g. "s3://bucket/run-42/stage-3.parquet"
    }
    fn rehydrate(reference: &str) -> Result<Self, RehydrateError> {
        Ok(DatasetRef { location: reference.to_string() })
    }
}
```

Mark the node durable in its policy (`NodePolicy::new().durable(true)`), and
register it through the durable path (`Flow::register_durable` /
`register_source_durable`, bounded on `DurableOutput`). **What this buys at
resume:** a durable node whose reference still resolves is `satisfied-from-prior`
and its value is *rehydrated* rather than recomputed, while an in-memory sibling
re-executes when a re-running consumer demands its value (**C10/C27**). The trade
is stated up front, not discovered mid-run: assembly *rejects* a node marked
durable whose output type does not implement the contract
(`ProblemKind::DurableWithoutContract`).

> **Backing tests:** `durable_output_reference_round_trips_losslessly` (the
> lossless `serialize_reference` → `rehydrate` round-trip that makes resume
> possible) and `a_node_marked_durable_needs_the_contract_or_assembly_rejects_it`
> in `crates/cli/tests/cookbook.rs`.

---

## The non-`Send` capture error and its fixes

**The most common first-hour error** (arch.md **C1**): a task value is moved onto
a worker thread to run, so it must be `Send + 'static`. Capturing a non-`Send`
value — an `Rc`, a `RefCell<Rc<…>>`, a raw pointer, a `MutexGuard` held across
construction — makes the task `!Send`, and registering it fails to **compile**:

```rust
use std::rc::Rc;

struct NonSendTask {
    shared: Rc<u32>, // Rc is !Send — so NonSendTask is !Send
}
impl Task for NonSendTask { /* ... */ }

let _ = flow.register_source("non-send", &task); // E0277: `Rc<u32>` is not `Send`
```

The compiler names both the offending type (`NonSendTask`) and the non-`Send`
captured type (`Rc<u32>`). **The fixes** — never weaken the bound:

- **Capture an `Arc` instead of an `Rc`.** `Arc<T>` is `Send + Sync` when `T` is,
  so `Arc<u32>` in the task makes the task `Send`. This is the fix for shared,
  read-only configuration.
- **Construct the value inside `run`.** If the non-`Send` value is genuinely a
  per-attempt working value (a `Cell`, a non-thread-safe client handle), build it
  *inside* `run` rather than capturing it in the task struct — the future's local
  is fine; only the captured configuration must be `Send`.
- **For a non-thread-safe client that must be shared,** use the owning-worker
  pattern (one thread owns it, others reach it through a channel) and register the
  channel end — not the client — as a resource (arch.md **C9**).

> **Backing fixture:** the broken form is the checked-in compile-fail case
> `crates/core/tests/ui/task_non_send_capture.rs` (pinned to the workspace
> toolchain by the T8 UI harness; the sibling `.stderr` asserts the diagnostic
> names `Rc<u32>` and `NonSendTask`). Every compiling cookbook task above is a
> *fix* — each captures only `Send` values or builds working values inside `run`.

---

## Two same-typed resources via newtypes

**The pattern:** long-lived external clients live in the **resource registry**
(arch.md **C9**), built once in your `main` and shared immutably for the run.
Tasks retrieve what they need **by type** — no string key, the same
no-lookup-by-name philosophy as typed handles. But two resources of the same
underlying type would be ambiguous, so you distinguish them with **newtypes**:

```rust
struct BillingClient(HttpClient);
struct AnalyticsClient(HttpClient); // same underlying type, distinct newtype

let registry = ResourceRegistry::builder()
    .register(BillingClient(HttpClient { base_url: "https://billing".into() }))?
    .register(AnalyticsClient(HttpClient { base_url: "https://analytics".into() }))?
    .build();

// In a task, retrieve each by its own type — unambiguous, no string key.
let billing = registry.get::<BillingClient>();
let analytics = registry.get::<AnalyticsClient>();
```

Registering two resources of the **literally identical** type is rejected as
ambiguous — a type-keyed `get::<HttpClient>()` could not choose between them, so
`ResourceRegistry::builder().register(...)` returns
`Err(RegistryError::Duplicate)` rather than silently replacing the first (**C9**).
The newtype pattern above is the fix. Secrets go in the registry behind the
[`Secret`](../crates/core/src/context.rs) wrapper, which has no `Debug`/`Display`
path, so the framework never serializes them into artifacts or its own logs.

> **Backing tests:** `two_same_typed_resources_are_distinguished_by_newtypes` and
> `registering_two_resources_of_the_identical_type_fails_as_ambiguous` in
> `crates/cli/tests/cookbook.rs`.
