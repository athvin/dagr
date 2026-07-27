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

## Authoring a task: `#[task]` first, hand-written `impl Task` as the fallback

The **primary** way to author a task is the [`#[task]`](../crates/macros/src/lib.rs)
attribute (ADR 082): put it on an inherent `impl` block that holds one
`async fn run`, and it generates the `impl Task` — inferring the input type from
the `run` arguments, the output type from the `Result<T, TaskError>` return, and
the execution class from the attribute (`#[task]` = await-bound,
`#[task(blocking)]`, `#[task(compute)]`). You write only the work:

```rust
use dagr_core::task;

struct Double;

#[task]
impl Double {
    async fn run(&mut self, input: u64) -> Result<u64, TaskError> {
        Ok(input * 2)
    }
}
```

That is the whole task — the four things arch.md **C1** says an author declares,
with no `type Input` / `type Output` / `EXECUTION_CLASS` scaffolding to write.
Zero inputs is `_input: ()`; two-to-eight inputs are one **tuple** parameter
(`(a, b): (A, B)`), delivered by value; an optional `ctx: &RunContext` parameter
is detected by type and threaded in.

**Hand-written `impl Task` stays a first-class fallback**, and the sole escape
hatch for anything the macro cannot express. It is `use dagr_core::task::Task;`
and the four declarations spelled out:

```rust
use dagr_core::task::{RunContext, Task};

impl Task for Double {
    type Input = u64;
    type Output = u64;
    async fn run(&mut self, _ctx: &RunContext, input: u64) -> Result<u64, TaskError> {
        Ok(input * 2)
    }
}
```

The two forms are equivalent — the macro expands to exactly this. The entries
below spell out `impl Task` (the fallback) so the associated types stay visible
next to each pattern; every one is equally authorable with `#[task]`.

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
8. [Common `#[task]` mistakes](#common-task-mistakes)
9. [Declaring DAGs with `#[dag]` and running them with one line](#declaring-dags-with-dag-and-running-them-with-one-line)

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

---

## Common `#[task]` mistakes

The [`#[task]`](../crates/macros/src/lib.rs) macro turns four hand-written
declarations into one `run` fn (see [Authoring a task](#authoring-a-task-task-first-hand-written-impl-task-as-the-fallback)
above), but a few misuses trip authors up. **A caveat that applies to all of
them:** a proc-macro rewrites your `impl` block, so a diagnostic can attribute the
error to the `#[task]` **attribute site** (the `#[task]` line) rather than the
exact offending line inside `run` — this is a known limitation (ADR 082). When a
message points at `#[task]`, read the whole `run` signature and body, not just the
attribute. Every mistake below is a committed compile-test in the macro's
**trybuild corpus** ([`crates/macros/tests/expand/`](../crates/macros/tests/expand)):
the `fail/` directory holds each broken form with its exact `.stderr` snapshot, so
these diagnostics are a versioned contract, not a moving target. Regenerate the
snapshots deliberately with `TRYBUILD=overwrite cargo test -p dagr-macros --test
trybuild` after a pinned-toolchain bump.

**1. A bare `-> T` return instead of `-> Result<T, TaskError>`.** A task must be
able to fail with a classified error, so the return type is not optional. Writing
`-> u64` where the framework needs `-> Result<u64, TaskError>` is rejected by the
macro with a `compile_error!` naming the required shape:

```rust
// WRONG — no failure channel:
#[task]
impl Double {
    async fn run(&mut self, input: u64) -> u64 { input * 2 }
    //                                     ^^^ error: #[task]'s `run` must return
    //                                         `Result<T, TaskError>`
}
```

**The fix:** wrap the output in `Result<T, TaskError>` and return `Ok(..)` — even
an infallible body declares the channel. (Pinned: `fail/bare_return.rs`.)

**2. Capturing a non-`Send` value.** A task value is moved onto a worker thread, so
it must be `Send + 'static`. Capturing an `Rc`, a `RefCell<Rc<…>>`, a raw pointer,
or a `MutexGuard` held across the body makes the task `!Send` and reds the build:

```rust
use std::rc::Rc;
#[task]                       // the diagnostic may point HERE (the attribute site)
impl NonSend {                //   rather than at the `Rc` field below
    async fn run(&mut self, input: u64) -> Result<u64, TaskError> {
        Ok(input + *self.shared) // self.shared: Rc<u32> — `Rc<u32>` is not `Send`
    }
}
```

**The fix** (never weaken the bound): capture an `Arc` instead of an `Rc` for
shared read-only config; construct a genuinely per-attempt non-`Send` value
*inside* `run` rather than in the struct; or, for a non-thread-safe client that
must be shared, use the owning-worker pattern and register the channel end as a
resource. See [The non-`Send` capture error and its fixes](#the-non-send-capture-error-and-its-fixes)
for the full treatment. (Pinned: `fail/non_send_capture.rs` — its snapshot is the
canonical example of the attribute-site attribution above.)

**3. More than eight inputs.** dagr's input-arity ceiling is **8** (arch.md **C3**,
[`binding.rs`](../crates/core/src/binding.rs) `MAX_INPUT_ARITY`). The macro adds no
second check — a 9-tuple flows through as `type Input` — so the ceiling is enforced
where the node is **wired**: binding nine upstream handles fails with the sealed
`Deps` trait's curated *"too many inputs: the maximum input arity is 8"*
diagnostic, whose note tells you the fix:

```text
error[E0277]: too many inputs bound to one task: the maximum input arity is 8
   = note: aggregate the upstream values into a struct produced by an
           intermediate node, then depend on that one handle
```

**The fix:** produce an aggregate `struct` from an intermediate node and depend on
that one handle — the same shape [Fan-in](#fan-in) uses past arity 8. (Pinned:
`fail/over_eight_inputs.rs`.)

**4. A deps mismatch at registration.** The `run` arguments declare the task's
input types, and `RunnableFlow::register` binds upstream handles whose value types
must **exactly** match them (`D: Deps<Inputs = T::Input>`, C3). Binding a
`Handle<String>` into a task whose `run` takes `input: u64` is a *compile* error,
not a runtime surprise, and the diagnostic names both sides:

```text
error[E0271]: type mismatch resolving `<Handle<String> as Deps>::Inputs == u64`
   |  ...register::<WantsU64, _>("wants-u64", WantsU64, label);
   |                                                    ^^^^^ expected `u64`, found `String`
```

**The fix:** bind the handle whose type matches the `run` argument, or change the
`run` argument to the upstream's actual type — the compiler names the expected and
actual types, so the mismatch is unambiguous. (Pinned: `fail/deps_mismatch.rs`.)

> **Backing corpus:** [`crates/macros/tests/trybuild.rs`](../crates/macros/tests/trybuild.rs)
> drives the whole `expand/pass/` (every accepted arity and execution class) and
> `expand/fail/` (the four mistakes above, each with a committed `.stderr`)
> boundary under the workspace-pinned toolchain, and
> `common_task_mistakes_have_compiling_fixes` in `crates/cli/tests/cookbook.rs`
> proves each **fix** above compiles and runs through `RunnableFlow`.

---

## Declaring DAGs with `#[dag]` and running them with one line

**The pattern:** just as [`#[task]`](#authoring-a-task-task-first-hand-written-impl-task-as-the-fallback)
generates the `impl Task` so you write only `run`, the
[`#[dag]`](../crates/macros/src/lib.rs) attribute (ADR 092) declares a DAG over the
[`FlowBuilder`](../crates/cli/src/flow_builder.rs) façade and **auto-registers** it,
so hosting many DAGs in one binary needs no hand-wired registry and `main` is one
line. It is the sugar over the [flow-registry](flow-registry.md) machinery: you write
`#[dag]` fns, and [`dagr_cli::run`](../crates/cli/src/run.rs) discovers every one at
link time, builds the registry, and delegates to `run_registry` (which owns `list` /
`graph <dag>` / `validate <dag>` / `run <dag>`).

One authoring import brings the surface into scope. Each `#[dag]` fn declares a root
with [`FlowBuilder::source`](../crates/cli/src/flow_builder.rs) and each dependent node
with [`f.task(name, task).depends_on(upstream)`](../crates/cli/src/flow_builder.rs) —
the dependency direction is explicit, and because a `Handle` has no `depends_on`, edges
point only backward (cycles are unrepresentable). The whole binary is `dagr_cli::run`:

```rust
use dagr_cli::prelude::*; // #[task], #[dag], FlowBuilder, run, RunnableFlow, …

#[dag] // name defaults to the fn name ("alpha"); #[dag(name = "nightly")] overrides
fn alpha(f: &mut FlowBuilder) {
    let rows = f.source("extract", Extract { rows: 3 });  // a root; rows: Handle<Rows>
    let _report = f.task("load", Load).depends_on(rows);  // load DEPENDS ON extract
}

#[dag]
fn beta(f: &mut FlowBuilder) {
    let _report = f.source("aggregate", Aggregate { seed: 7 });
}

fn main() -> std::process::ExitCode {
    // Discovers alpha + beta (sorted, deduped by name), delegates to run_registry.
    dagr_cli::run(std::env::args_os()).into()
}
```

The tasks and payloads carry **`#[derive(StableName)]`** (one line each). The
graph-emittable `f.source` / `f.node` require every task and payload type to have a
`StableName` — the author-declared name the graph artifact records — and the derive
supplies it (defaulting to the type's identifier; `#[stable_name = "…"]` overrides).
So a `#[task]` task composes with `#[dag]` with **no hand-written trait bodies**: the
three macros are the whole authoring surface.

The library owns every verb, so `list`, `graph <dag>`, `validate <dag>`, and
`run <dag> --store DIR` all work against any declared DAG with no extra code — each
`run <dag>` is its own independent run with its own run identity and store. A one-DAG
binary keeps its real name (`list` prints it, `graph <name>` selects it) and may omit
the name (`run` / `graph` / `validate` dispatch the sole DAG). Copy
[`crates/cli/examples/many_dags.rs`](../crates/cli/examples/many_dags.rs) whole; it is
the compiled, run reference for this pattern.

**Two operator-facing obligations (ADR 092) — the docs state them because they are
real:**

1. **Your app crate depends on `inventory = "0.3"`.** `#[dag]` expands to
   `::inventory::submit! { … }`, and `inventory`'s `$crate`-based macro resolves the
   crate by the caller's own extern prelude with **no path override** — so your
   `Cargo.toml` lists `inventory = "0.3"` directly, exactly as it already lists
   `dagr-core` for `#[task]`:

   ```toml
   [dependencies]
   dagr-cli = { git = "https://github.com/athvin/dagr" }
   dagr-core = { git = "https://github.com/athvin/dagr" }
   inventory = "0.3"
   ```

2. **`#[dag]` declarations live in the leaf binary crate.** `inventory` registers
   life-before-`main` constructors in a linker section, so a submission is **reliably
   collected only when it is compiled into the final linked binary** — a `#[dag]`
   placed in a *dependency library* the binary does not otherwise reference is dropped
   by linker dead-code elimination, and the binary sees zero DAGs. So put your
   `#[dag]`s in the binary crate that calls `dagr_cli::run` (across as many of its
   modules as you like). **Cross-crate DAG libraries are out of scope** for this
   milestone — this is a real limitation of the `inventory` approach, not an
   oversight. Prefer the short `dagr::run` spelling? Write `use dagr_cli as dagr;` at
   zero cost — there is no `crates/dagr` facade crate (ADR 092 rejected it).

**Grammar and its diagnostics.** `#[dag]` takes an optional `name = "…"` string
literal (`#[dag]` alone = the fn name); anything else — `#[dag(bogus)]`,
`#[dag(name = 42)]` — is a `compile_error!` naming the accepted form, and a fn with
the wrong shape (no `&mut FlowBuilder` parameter) is a natural argument-count error at
the generated factory. Each is a committed `trybuild` fixture in
[`crates/macros/tests/expand/fail/`](../crates/macros/tests/expand)
(`dag_bad_grammar.rs`, `dag_name_not_str.rs`, `dag_bad_signature.rs`). **Duplicate DAG
*names* are not a compile error:** they surface at **runtime** — `dagr_cli::run`
rejects two DAGs sharing a name with `InvalidUsage` before any flow is built (T79) —
so there is no compile-fail fixture for a duplicate name.

**The hand-wired `FlowRegistry` remains the explicit fallback.** `#[dag]` +
`dagr_cli::run` is the auto-discovery *option*; when you want the registry spelled out
in `main` (a computed flow set, a non-`#[dag]` factory, or simply no `inventory`
dependency), build a [`FlowRegistry`](../crates/cli/src/registry.rs) by hand and call
`run_registry` — see the [flow-registry guide](flow-registry.md) and
[`crates/cli/examples/multi_flow.rs`](../crates/cli/examples/multi_flow.rs). Both
produce the same `list` / `graph` / `validate` / `run` behaviour.

> **Backing example + tests:** [`crates/cli/examples/many_dags.rs`](../crates/cli/examples/many_dags.rs)
> is the compiled, run reference; `crates/cli/tests/dag_example_and_docs.rs`
> (`many_dags_run_writes_an_on_disk_event_stream`,
> `many_dags_graph_emits_through_the_sugar`) drives it end-to-end, and
> `crates/cli/tests/dag_macro.rs` + `dag_auto_discovery.rs` prove `#[dag]` discovery
> and the sort/dedup rules — all on both `ubuntu-latest` and `macos-latest`. The
> grammar/signature diagnostics are pinned by
> [`crates/macros/tests/trybuild.rs`](../crates/macros/tests/trybuild.rs).

## Running a flow programmatically (no `#[dag]`, no CLI verbs)

`#[dag]` + `dagr_cli::run` is the pipeline-binary path: the library owns `main` and
the verbs. When you instead want to **build and drive a flow in your own code** — an
embedded run, a test, a computed flow set, or to read a node's value back in-process —
register tasks on a [`RunnableFlow`](../crates/cli/src/run_flow.rs) and call
**`run_to_store`**. It builds the default event sink, the wall-clock clock, a fresh
run id, and the run store for you — one call, no hand-written plumbing:

```rust
use dagr_cli::prelude::*;

let mut flow = RunnableFlow::new();
let counted = flow.register_source("count", Count { up_to: 21 });
let doubled = flow.register::<Double, _>("double", Double, counted);

// One call: the sink, clock, run id, and store are all defaulted for you. Writes a
// real event stream under `<base>/report/<run-id>/events.jsonl`.
let report = flow.run_to_store("report", "./runs").expect("assembles and runs");
println!("{:?} -> {:?}", report.outcome(), report.output(doubled)); // Succeeded -> Some(42)
```

`run_to_store` returns a `RunToStoreError { Store, Assembly }` so a store that cannot
be opened stays distinct from a flow that does not assemble. Need a **custom** sink,
a deterministic `TickClock` (for byte-reproducible streams), or a tuned `RunConfig`?
The fully-explicit `flow.run(pipeline, &config, sink, clock)` stays available — the
public `dagr_cli::{FileSink, SystemClock, TickClock, mint_run_id}` are the same
defaults `run_to_store` uses, exposed for that path.

## Testing and inspecting a run

**Unit-test one task with no runtime.** A task is a value with a `run` method, so you
can test it in isolation with the [`SingleTaskTest`](../crates/core/src/test_kit.rs)
kit (`dagr_core::test_kit`, the default-on `test-kit` feature) — no flow, no
scheduler. A synchronous (`#[task(blocking)]` / `#[task(compute)]`) body runs with
`run_sync`; an await-bound body runs with `run_await`, which supplies the runtime so
the test author writes none.

**Read a run's event stream back without touching the pipeline.** The
`<base>/<pipeline>/<run-id>/events.jsonl` a run writes is self-contained: fold it with
[`read_records`](../crates/artifact/src/event_stream.rs)
(`dagr_artifact::event_stream`) and answer questions — terminal states, attempt
counts, the run bookends — from the artifact alone, with no access to the live flow.
This is the seam every renderer and the `fold` verb read.

**Catch an unintended rewiring in review.** Capture a pipeline's structure with
[`StructureSnapshot::from_pipeline`](../crates/cli/src/structure_snapshot.rs) and diff
two revisions: an intended change shows as a reviewable structural diff, and an
*accidental* rewiring shows up as a diff nobody meant to make — a pure-assembly check
that needs no run store, network, or database.
