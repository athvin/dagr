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
10. [Placing a node on remote compute](#placing-a-node-on-remote-compute)
11. [Querying run state across DAGs](#querying-run-state-across-dags)

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

## Placing a node on remote compute

**Problem.** One node in your graph wants 64 GiB, or a GPU, or a machine that is not
this one. Everything else is happy in-process, and you do not want two pipelines, two
binaries, or two mental models.

**Shape.** Declare a size on that node and select the remote executor at run time.
The rest of the graph is untouched: same binary, same pipeline, same artifacts.
`crates/cli/examples/placed_pipeline.rs` is the runnable version of everything below.

```rust,ignore
let sample = flow.register_source_placed(
    "sample",
    TakeSample { rows: 21 },
    NodePolicy::new(),
    Placement::new().cpu("500m").memory("512Mi"),
);
// No placement: this one runs wherever the invocation runs.
let _summary = flow.register_payload("summarise", Summarise, sample);
```

Every resource string is **opaque** — dagr never parses `"500m"` or `"512Mi"`, it
carries them to the platform verbatim, so a cluster that grows a new unit needs no
dagr release.

### Turning it on

Remote execution lives behind the **default-off `k8s` cargo feature**. A build that
did not compile it **refuses** `--dagr.executor=k8s` at bootstrap, naming the feature
— it never falls back to running your placed nodes here behind your back. A build
that *did* compile it, but was handed no cluster, refuses too, naming the nodes it
would have had to substitute.

| Knob | Environment | Default | What it bounds |
|---|---|---|---|
| `--dagr.executor` | `DAGR_EXECUTOR` | `local` | Which executor runs node attempts: `local` or `k8s`. |
| `--dagr.max-pods` | `DAGR_MAX_PODS` | unlimited | Concurrent placed attempts. Unset means uncapped — set it to stay inside a namespace quota. |
| `--dagr.pod-launch-retries` | `DAGR_POD_LAUNCH_RETRIES` | 2 | Extra *launches* a pod that never started may have. Distinct from `NodePolicy::retries`, which is the node's own budget for work that ran and failed. |

Precedence is the usual `flag > environment > default`. Under `--dagr.executor=local`
a placement is **recorded and ignored**: it is in the graph artifact, and the node
runs in-process at full speed on a machine that has never heard of a cluster. That is
what makes one binary genuinely both.

### Placement is policy, not an execution class

A `Placement` feeds the **policy hash** and never the **structural fingerprint**. So
moving a node between local and placed is a *policy* change: a resume prints a policy
diff and proceeds. A run started locally resumes under `--dagr.executor=k8s`, and the
reverse, with no structural refusal — which is exactly the point of making placement a
policy rather than a new execution class.

### The cost, measured

Remote start latency is **seconds**, against dagr's sub-millisecond local per-node
overhead — three orders of magnitude, and entirely outside dagr's control. Measured
(T101, one shared watch, concurrent submission):

| Condition | p50 | p99 |
|---|---|---|
| warm, co-located (kind), n=1 | 0.76 s | 0.76 s |
| warm, co-located, n=10 | 0.91 s | 0.92 s |
| warm, co-located, n=50 | 2.30 s | 3.44 s |
| warm, across a network (k3s), n=10 | 2.49 s | 2.50 s |
| **cold image pull**, n=50 | **13.03 s** | **24.19 s** |

**The tail is the image pull, not the platform.** Cold and warm differ by ~1.9 s at
n=1 and by ~20.7 s at p99 for n=50. Pre-pulled images and pinned digests are a
requirement of the operational story, not a tuning tip. Place a node when its own
work dominates ~1–2.5 s; do not place a graph of sub-second nodes one attempt at a
time.

### Somewhere for the pods to put things — not wired yet

A placed attempt reports by writing an attempt shard and its output where the
orchestrator can read them, so both sides need one container they can both reach.
**The shipped code cannot yet give a pod one.** This is the honest current state,
not a choice you make at provisioning time:

- The pod side writes through a **local filesystem path**, and `exec-node` **refuses
  an input reference that names any backend other than the local one** — so the
  `blob-s3` backend is not openable from inside a pod.
- The pod spec dagr builds carries an image, a command, the declared size, its
  identity labels and annotations, `restartPolicy: Never`, node selectors and
  tolerations. It has **no volume, no volumeMount and no environment field**, so a
  host path, an RWX claim, or a bucket's endpoint cannot be attached to it at all.

So a placed node runs end to end against dagr's in-process API fake, and **cannot
yet run against a real cluster**: the pod would start and have nowhere to report to.
Adding that seam is mechanism work owned by the node-runner ticket (T108). When it
lands, the shape is the usual one — an RWX volume mounted at the same path on both
sides, or the S3-compatible backend once `exec-node` can open it — and this section
becomes a choice rather than a gap.

What does **not** change when it lands: payloads travel through that container,
never through the API server, so there is no `ConfigMap` smuggling and no 1 MiB
ceiling. A reference that carries a signed query string is refused rather than
written into a pod's arguments, and a pod carries no credential for dagr's own run
index — the index is the orchestrator's, and the pod never links it.

### The RBAC an operator applies

Apply [`crates/k8s/manifests/dagr-orchestrator-rbac.yaml`](../crates/k8s/manifests/dagr-orchestrator-rbac.yaml),
substituting your namespace. It is a namespaced `Role` plus its `ServiceAccount` and
`RoleBinding`, granting six verbs on **one** resource in **one namespace** and
nothing else:

| Verb | Why the orchestrator needs it |
|---|---|
| `create` | Submit one attempt's pod. |
| `get` | The submission idempotency probe. |
| `list` | The startup discovery pass, and every resync. |
| `watch` | The single long-lived stream, one per orchestrator process. |
| `patch` | Rewrite `metadata.labels` in place — the whole of orphan adoption. |
| `delete` | Remove a pod dagr owns: a timeout, a cancellation, a revocation. |

Remove one and dagr **names the missing verb** and points at the manifest, rather
than retrying a denial that will never succeed or reporting a generic API error.
Read that at the strength it is proven at: the classifier is tested against a
**pinned fixture** of the denial message a Kubernetes API server sends, not against
a live cluster — nothing in this repository has been run against one. Deliberately absent: `update`
(a full replace races the platform's own `status` writes), `deletecollection`,
`pods/log`, `pods/exec`, and anything cluster-scoped. dagr submits a bare Pod with
`restartPolicy: Never` and does its own retrying — letting the platform retry too
would duplicate an attempt.

### What this is not

dagr is **not a scheduler** and installs **no control plane**: there is no chart, no
operator, no custom resource, and nothing that outlives the run. The orchestrator
makes outbound calls, holds one watch, and exits when the run ends. ADR 115 **narrowed**
one permanent non-goal and moved nothing else: a *distributed execution* system means
an engine that distributes the graph and its control — cooperating orchestrators,
work-stealing, cross-run queues — and that is still excluded. One process owns the
graph, the event stream, and every retry decision.

## Querying run state across DAGs

**The pattern:** one binary hosts many DAGs ([Many DAGs in one
binary](#declaring-dags-with-dag-and-running-them-with-one-line)), and you want a
**single place to query their state across runs** — "how many runs of each DAG
succeeded?", "which nodes take longest?", "what did each node end as most recently?".
dagr's answer is the **local run index** (the "metastore", ADR 097): an opt-in,
embedded, non-coordinating projection of the event streams the runs already write. It
is off by default; you turn it on with a toggle, and every `run` then *also* writes
its rows into one index file. This is a query convenience over the run store — the
event stream stays the source of truth (arch.md "What it is not") — and it is a
**default-off cargo feature**, so a build that never asks for it never pulls `libsql`.

**Native access only, same host.** ADR 097 fixed the access model: the index is a
`libSQL` file that is **byte-compatible with stock `SQLite`**, so the lowest-friction
query path needs *zero new tools* — plain `sqlite3 metastore.db "SELECT …"`. There is
no Postgres wire protocol, no server, and no remote/network access: the
file lives on the **same host's local filesystem** as the runs it indexes, and you
query it embedded. (`turso db shell <file>` and the `libsql` CLI open the same file
and are drop-in equivalents to `sqlite3` if you prefer them.)

**1. Turn the index on and run a few DAGs.** Build the binary with the default-off
`metastore` feature, then set the toggle (`flag > env > default`, arch.md C26) — a
`--dagr.metastore` flag, or `DAGR_METASTORE=1` in the environment once. Each `run`
writes into one `metastore.db` under the store base (override with
`--dagr.metastore-store <path>`):

```sh
# Build the many-dags example with the run index compiled in (default-off feature).
cargo run --features metastore --example many_dags -- \
    run alpha --store ./runs --dagr.metastore
cargo run --features metastore --example many_dags -- \
    run beta  --store ./runs --dagr.metastore   # or: DAGR_METASTORE=1 … run beta
cargo run --features metastore --example many_dags -- \
    run gamma --store ./runs --dagr.metastore
```

Three separate processes, one shared `./runs/metastore.db`. Each `run <dag>` is its
own independent run with its own identity — the index does **not** coordinate them; it
just records what each wrote.

**2. Query it with plain `sqlite3`.** The five M7 tables are `dag` (one row per DAG,
by stable name), `dag_version` (a DAG's structural fingerprint over time), `dag_run`
(one row per run), `node_attempt` (one row per attempt — retries included), and
`node_terminal` (the single terminal state per node per run). The `state` columns hold
dagr's canonical vocabulary spellings (the nine terminal node states; the six run
states). Some worked queries:

```sql
-- Runs per DAG, grouped by outcome state (the cross-run overview).
SELECT d.name AS dag, r.state, count(*) AS runs
FROM dag_run r JOIN dag d ON d.dag_id = r.dag_id
GROUP BY d.name, r.state
ORDER BY d.name, r.state;

-- Slowest nodes by executing-phase milliseconds, read from the per-attempt
-- phase-duration breakdown via SQLite's built-in JSON1 (json_extract).
SELECT node_id,
       max(json_extract(phase_durations_json, '$.executing')) AS exec_ms
FROM node_attempt
WHERE phase_durations_json IS NOT NULL
GROUP BY node_id
ORDER BY exec_ms DESC
LIMIT 5;

-- The latest terminal state of every node, across all runs, joined back to its DAG.
SELECT d.name AS dag, t.node_id, t.state
FROM node_terminal t
JOIN dag_run r ON r.run_id = t.run_id
JOIN dag d ON d.dag_id = r.dag_id
ORDER BY d.name, t.node_id;
```

These are read-only `SELECT`s against a file, from any process that can open it — no
dagr binary required, and nothing the engine coordinates on.

**3. Cross-run data lineage — "which runs touched dataset X".** When a run produces
or consumes a durable output, dagr projects that provenance into three M8 lineage
tables (T91): `output_produced` (one append-only row per produced output — `run_id`,
`node_id`, `attempt`, `uri`, `content_hash`, `size_bytes`, `kind`,
`produced_at_offset_ns`, `originating_run`), `input_consumed` (one row per consumed
durable input — `run_id`, `node_id`, `attempt`, `uri`, `content_hash`), and the
optional `asset` identity endpoint (`uri` primary key, `extra` JSON). The per-attempt
`node_attempt` row also carries the durable-reference metadata columns
(`content_hash`, `size_bytes`, `scheme`, `produced_at_offset_ns`).

The lineage rows reference a dataset **by its `uri` value** — there is **no foreign
key** to the `asset` row, so a lineage row survives garbage-collection (or deletion)
of the `asset` endpoint; the `asset` table is a convenience join target, populated on
first sight of a `uri`, not load-bearing. Two runs producing at the same `uri` with
different content hashes are two distinct append-only rows that both join to the one
`asset` row by value. This is a **local, non-coordinating index of per-run
provenance**: dagr is not an asset scheduler. There are no data-triggered runs, no
asset queues, no watchers, and no partitions — the whole asset-scheduler cluster is a
permanent non-goal (arch.md permanent non-goals).

```sql
-- Which runs PRODUCED a given dataset (by uri), newest offset first.
SELECT run_id, node_id, attempt, content_hash, size_bytes
FROM output_produced
WHERE uri = 's3://bucket/dataset'
ORDER BY produced_at_offset_ns DESC;

-- Which runs CONSUMED it — the downstream side of the same dataset.
SELECT run_id, node_id, attempt, content_hash
FROM input_consumed
WHERE uri = 's3://bucket/dataset';

-- The full producer→consumer picture for every dataset, joined to the asset
-- identity row BY VALUE (uri), with per-dataset produce/consume counts.
SELECT a.uri,
       count(DISTINCT p.run_id) AS produced_by_runs,
       count(DISTINCT c.run_id) AS consumed_by_runs
FROM asset a
LEFT JOIN output_produced p ON p.uri = a.uri
LEFT JOIN input_consumed  c ON c.uri = a.uri
GROUP BY a.uri
ORDER BY a.uri;
```

**4. Auditing what a placed attempt was launched with.** When a node runs on remote
compute, the orchestrator writes an `attempt-submitted` record **before** it creates
the remote work object — so "what was this task launched with, and did it read what we
told it to?" is answerable after the pod is gone, and a submission whose attempt never
reported still leaves a trace. Those records project into two M10 tables (T111):
`attempt_submitted` (one row per submitted attempt — `run_id`, `node_id`, `attempt`,
`executor`, the **intended** `target_name`, the **observed** `observed_name` /
`observed_uid` / `observed_host`, `structural_fingerprint`, `policy_hash`,
`tool_version`, `image_digest`, `input_count`, `submitted_at_offset_ns`, `completed`,
`outcome_state`) and `attempt_submitted_input` (one row per reference the attempt was
handed, keyed by its declared **`position`** — dagr binds inputs positionally, so the
order is the fact being recorded).

Read three columns carefully. **`completed`/`outcome_state`** carry
*submitted-but-never-completed* as a first-class state: `completed = 0` with a NULL
`outcome_state` means no attempt outcome ever arrived — that is a crashed
orchestrator, not a failure, and dagr's nine-state terminal taxonomy gains no tenth
member for it. **`input_count`** is `0` for a consume-nothing source and `NULL` when
the record never stated its inputs, so "zero" and "unknown" never blur. And
**`target_name` vs `observed_name`** are intent and reality kept apart, because they
diverge and a post-mortem needs both. Like the lineage tables, these carry every value
inline with **no foreign key** to anything — an audit row still resolves after its
referent, its pod, and even its attempt row are gone.

```sql
-- What was attempt 1 of node `extract` launched with — the ordered references,
-- their content hashes, and the image it ran.
SELECT i.position, i.uri, i.content_hash, s.image_digest, s.tool_version
FROM attempt_submitted s
JOIN attempt_submitted_input i
  ON i.run_id = s.run_id AND i.node_id = s.node_id AND i.attempt = s.attempt
WHERE s.node_id = 'extract' AND s.attempt = 1
ORDER BY s.run_id, i.position;

-- Which attempts were SUBMITTED but never COMPLETED — what a crashed
-- orchestrator left behind, with the pod identity to go looking for.
SELECT run_id, node_id, attempt, target_name, observed_name, observed_host
FROM attempt_submitted
WHERE completed = 0
ORDER BY run_id, node_id, attempt;

-- Which attempts READ a reference whose content hash differs from the one they
-- were SUBMITTED with — an out-of-band overwrite between launch and read.
-- (`IS NOT` compares NULL-safely, so a missing hash on either side shows up.)
SELECT s.run_id, s.node_id, s.attempt, s.position, s.uri,
       s.content_hash AS submitted_hash,
       c.content_hash AS read_hash
FROM attempt_submitted_input s
JOIN input_consumed c
  ON c.run_id = s.run_id AND c.node_id = s.node_id
 AND c.attempt = s.attempt AND c.uri = s.uri
WHERE c.content_hash IS NOT s.content_hash
ORDER BY s.run_id, s.node_id, s.attempt, s.position;

-- Which runs were LAUNCHED against a given uri, joined BY VALUE to the lineage
-- tables: who produced it, and whether the attempt actually consumed it.
SELECT s.run_id, s.node_id, s.attempt, s.position,
       count(DISTINCT p.run_id) AS produced_by_runs,
       count(DISTINCT c.run_id) AS consumed_by_runs
FROM attempt_submitted_input s
LEFT JOIN output_produced p ON p.uri = s.uri
LEFT JOIN input_consumed  c ON c.uri = s.uri AND c.run_id = s.run_id
WHERE s.uri = 's3://bucket/dataset'
GROUP BY s.run_id, s.node_id, s.attempt, s.position
ORDER BY s.run_id, s.position;
```

A local run emits no submission records at all, so these tables stay empty until you
run with a remote executor — and the divergence question stays a **query**, not a new
verb: it is a join, it lives beside the other cross-run queries, and a verb would grow
the command surface for something SQL already answers.

**Live now, backfill later — the two write paths.** The toggle above is the
**guaranteed live** tee: a run writes its index rows *as it executes*, and a metastore
write is as durable as an event-stream write — a failed index write surfaces as the
distinct sink-failure exit code, never silently swallowed. For runs that finished
*before* you turned the index on (or ran on another binary), **reconcile** them with
`dagr metastore init` / `dagr metastore sync` — the [reference](../README.md#the-run-index-metastore)
documents both verbs. Live and reconcile produce the **same rows** for the same run,
because both are the same guaranteed projection of the same event stream.

> **Backing example + tests:**
> [`crates/cli/examples/many_dags.rs`](../crates/cli/examples/many_dags.rs) is the
> compiled, run reference (it builds and runs with **and** without `--features
> metastore`). `crates/cli/tests/metastore_example_and_docs.rs` (behind the feature)
> runs `alpha`/`beta`/`gamma` into one store and **executes the `sqlite3` query block
> above verbatim** against it (the lineage queries run too — they return no rows for a
> run that produced none); `crates/cli/tests/metastore_docs_claims.rs` (always
> compiled) guards that this section stays truthful — native-access-only, no server,
> and lineage projected as a by-value / no-FK index, never an asset scheduler. The
> lineage projection itself is proven in
> `crates/metastore/tests/lineage_projection.rs` (T91: reconcile + live tee, the
> no-FK cross-run join, and the M7→T91 forward-migration/additivity path), and the
> submission/audit projection in
> `crates/metastore/tests/submission_projection.rs` (T111: live-equals-sync byte
> identity, idempotent re-sync, in-place upgrade, and the four audit queries above
> run against real submitted attempts) over
> `crates/artifact/tests/attempt_submitted_fold.rs` (the fold that surfaces a
> submission with no outcome). Both run on `ubuntu-latest` and `macos-latest`.
