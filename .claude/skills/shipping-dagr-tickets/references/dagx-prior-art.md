# dagx prior art

Distilled patterns from [swaits/dagx](https://github.com/swaits/dagx) (v0.3.1,
MIT) — a minimal, type-safe, runtime-agnostic async DAG executor with
compile-time cycle prevention. It is best-in-class prior art for dagr's
**compile-time half** (typed handles, wiring, erasure boundary, task macro,
test/CI structure) and an **anti-pattern catalog** for dagr's runtime half
(persistence, retries, permits, cancellation, resume), which dagx deliberately
scoped out. Read only the section routed to your ticket.

## Routing table

| Your ticket | Read |
|---|---|
| 018, 020, 021, 023, 024 (typed handles, builder, wiring, compile-failure suite) | §1 |
| 019 (task abstraction), any `#[task]`-style macro work | §2 |
| 027 (output slots), erasure-boundary decisions | §3 |
| 033 (panic containment) | §4 |
| 006, 007, 026, 035, 036, 037, 073, 074, 075 (CI + test structure) | §5 |
| 030, 031, 032, 034, 043, 044, 045, 046 and all runtime/persistence/resume tickets | §6 ANTI-PATTERNS (read before borrowing anything) |

## Contents

- [§1 Typed handles, builder, wiring](#§1-typed-handles-builder-wiring)
- [§2 Task trait and macro](#§2-task-trait-and-macro)
- [§3 Type-erasure boundary and outputs](#§3-type-erasure-boundary-and-outputs)
- [§4 Panic containment](#§4-panic-containment)
- [§5 Test and CI structure](#§5-test-and-ci-structure)
- [§6 ANTI-PATTERNS for dagr](#§6-anti-patterns-for-dagr)

## §1 Typed handles, builder, wiring

The typed-handle trio gives compile-checked wiring and free cycle prevention:

- **Typed opaque handle** — a u32 id plus variance/auto-trait-safe phantom:
  ```rust
  pub struct TaskHandle<T> { pub(crate) id: NodeId, pub(crate) _phantom: PhantomData<fn() -> T> }
  pub struct NodeId(pub u32);   // Copy + Hash
  ```
  Handles serve double duty: dependency wiring and post-run result retrieval.

- **Sealed positional binding** — a private trait maps handle tuples to input
  tuples so count, order, and types are all compile-checked:
  ```rust
  pub(crate) trait DepsTuple<Input> { fn to_node_ids(self) -> Vec<NodeId>; }
  impl<O> DepsTuple<(O,)> for &TaskHandle<O> { ... }        // bare handle, 1 dep
  impl<A, B> DepsTuple<(A, B)> for (&TaskHandle<A>, &TaskHandle<B>) { ... }  // macro to arity 8
  ```
  Usage: `dag.add_task(Add).depends_on((&x, &y))`. The arity cap of 8 is a
  deliberate design nudge (documented, macro-generated), a precedent for dagr's
  typed binding arity.

- **Type-state builder, consume-on-wire** — cycles become unrepresentable:
  ```rust
  #[must_use]
  pub struct TaskBuilder<'a, Input, Tk> { id: NodeId, dag: &'a mut DagRunner, _phantom: PhantomData<(Tk, Input)> }
  pub fn depends_on<D: DepsTuple<Input>>(self, deps: D) -> TaskHandle<Tk::Output>  // consumes self
  ```
  `depends_on` consumes the builder (wire exactly once) and `TaskHandle` has no
  `depends_on`, so edges can only point backward — no runtime cycle detection
  exists or is needed. Uses `#[allow(private_bounds)]` to keep `DepsTuple`
  sealed while appearing in a public signature. dagr's flow builder (023) can
  adopt this wholesale; registration-time backward refs (C4 ordering deps) fit
  the same shape.

- **GAT return-type dispatch** — `add_task` returns a handle for sourceless
  tasks and a builder otherwise:
  ```rust
  pub trait TaskWire<Input>: Task<Input> + Sync + 'static {
      type Retval<'dag>;
      fn new_from_dag<'dag>(id: NodeId, dag: &'dag mut DagRunner) -> Self::Retval<'dag>;
  }
  // impl for () returns TaskHandle directly; tuple impls return TaskBuilder
  ```

## §2 Task trait and macro

- **Trait shape** — generic over Input (not an associated type), so one struct
  can implement `Task` for several input shapes; RPIT-in-trait future:
  ```rust
  pub trait Task<Input>: Send where Input: Send + Sync + 'static {
      type Output: Send + Sync + 'static;
      fn run(self, input: TaskInput<Input>) -> impl Future<Output = Self::Output> + Send;
  }
  ```
  `run(self)` by value encodes run-exactly-once and needs no Mutex. **Caution
  for dagr (019):** consuming `self` is incompatible with retries — dagr's C14
  attempt runner needs re-runnable nodes (`&mut self` per arch.md C1, or
  clone/factory semantics). Single-dep tasks implement `Task<(T,)>`, never
  `Task<T>`.

- **Linear extraction type** — `TaskInput` peels one typed value per `next()`
  call, each returning a narrower `TaskInput`; over/under-extraction is a
  compile error. Macro-generated to arity 8.

- **`#[task]` macro recipe** (if dagr wants a node macro): sits on an `impl`
  block; derives Input from the `run()` parameter types (must be `&T`, enforced
  with an actionable multi-line compile error) and Output from the return type;
  renames the user method to `run_impl`; emits the trait impl with an extraction
  chain (`let (a, _input) = _input.next();` …) using `Span::mixed_site()`
  idents for hygiene; dispatches on (has-self × is-async) so stateless, `&self`,
  `&mut self`, sync, and async forms all work. `extern crate self as dagx;` in
  lib.rs lets the macro's `::dagx::` paths resolve inside the crate's own tests.

- dagx also has an internal closure-task adapter (`task_fn` in dagx-test) kept
  **out** of the public API — its own docs say the ergonomics are bad and to use
  the macro. Relevant to 019's expression-form open question.

## §3 Type-erasure boundary and outputs

One place in the whole library erases types:

```rust
pub(crate) trait ExecutableNode: Send {
    fn execute_with_deps(self: Box<Self>, deps: Vec<Arc<dyn Any + Send + Sync>>) -> ExecuteFuture;
}
type ExecuteFuture = Pin<Box<dyn Future<Output = Result<Arc<dyn Any + Send + Sync>, DagError>> + Send>>;
```

`TypedNode<Input, T: Task<Input>>` implements it, awaiting the typed task and
Arc-wrapping the output **once**; fan-out hands dependents Arc clones (O(1));
downcasts happen only at typed edges. This maps directly onto dagr's output
slots (027): store `Arc<dyn Any + Send + Sync>` keyed by node, downcast at
typed edges only, Arc-wrap-once for shared-read fan-out. **But see §6:** dagr's
slots must also serialize (event stream, resume) and release (zombie-aware),
which `Arc<dyn Any>` alone cannot do.

## §4 Panic containment

Both the inline fast path and the spawned path wrap identically, so tasks
behave the same on every runtime:

```rust
AssertUnwindSafe(fut).catch_unwind().unwrap_or_else(|payload| {
    let msg = if let Some(s) = payload.downcast_ref::<&str>() { s.to_string() }
        else if let Some(s) = payload.downcast_ref::<String>() { s.clone() }
        else { "unknown panic".to_string() };
    Err(DagError::TaskPanicked { task_id: node_id.0, panic_message: msg })
})
```

This is precisely dagr's 033 requirement: catch, extract `&str`/`String`
payloads, normalize to a typed error, apply identically on every execution
path. dagr additionally classifies the result into its terminal-state taxonomy.

## §5 Test and CI structure

Transplantable wholesale:

- **Unit tests as sibling files** — `mod tests;` at the bottom of each module →
  `src/runner/tests.rs` etc.; tests reach into `pub(crate)` fields and forge
  handles to prove wrong-type/wrong-dag panics with `#[should_panic]`.
- **Single-binary integration tests** — `tests/lib_tests.rs` is just category
  `mod` declarations (`mod parallelism; mod errors; …`): one compilation unit,
  categories runnable via `cargo test --test lib_tests -- parallelism::`.
- **Concurrency proofs** — `Arc<AtomicUsize>` current/max counters with
  `compare_exchange_weak` loops prove true parallelism; event-order logs use
  `Arc<Mutex<Vec<&'static str>>>` and assert orderings (including negative
  assertions like `!order.contains(&"t3_should_not_run")`).
- **Compile-time guarantees as documented tests** — `tests/cycle_prevention.rs`
  proves cycles are unrepresentable via commented-out lines
  (`// let _c2 = builder.depends_on(&source); // ERROR: use of moved value`).
  Note dagx has **no** trybuild harness — dagr's 007/024 go further with a real
  compile-failure suite under the pinned toolchain.
- **Runtime matrix** — the `test-case` crate parametrizes one test body over
  smol / futures-executor / pollster / async-executor via a local spawner trait.
  dagr commits to tokio (arch.md), so the pattern applies to its execution-class
  matrix instead.
- **Benches** — criterion with `harness = false`, one bench target with
  `criterion_group!` per module dir, tuned noise threshold/significance, plus
  head-to-head comparison benches against a competitor.
- **CI** (`.github/workflows/ci.yml`) — every job runs a 3-way feature matrix
  (`--no-default-features`, `--all-features`, default); jobs: lint (fmt +
  `clippy --all-targets -- -D warnings`), msrv (`cargo msrv verify`), test
  (3 OS × stable/nightly × features, nightly `continue-on-error`, rust-cache),
  examples (script builds and **runs** every example as a smoke test), coverage
  (tarpaulin `--fail-under 80`), security (cargo-audit), docs
  (`cargo doc --no-deps` per feature set). Local CI reproduction via `act`.
- **Perf micro-idiom** — `PassThroughHasher` identity hasher for u32-keyed
  maps (`write` panics on non-u32 use), with presized
  `HashMap::with_capacity_and_hasher`.
- **Optional tracing** — cfg-gated, never a hard dep:
  `#[cfg_attr(feature = "tracing", tracing::instrument(...))]`, structured
  fields, documented level policy (INFO run start/end, DEBUG wiring, TRACE
  per-task, ERROR panics). Relevant to 056 (C25), where dagr makes tracing a
  named public integration instead.

## §6 ANTI-PATTERNS for dagr

Read this before borrowing anything into a runtime ticket. Each pattern is
correct **for dagx's scope** and wrong for dagr's:

1. **Layer-barrier scheduling.** `compute_layers()` (Kahn) yields
   `Vec<Vec<NodeId>>`; each layer fully completes before the next starts, and a
   single-node layer runs inline as a fast path. One slow task stalls its whole
   layer. dagr's readiness tracker (028) must decrement per-node in-degrees on
   individual completion — dagx even builds `dependents` maps that would
   support this but never uses them at runtime.
2. **`run(self)` consumes the task.** Run-exactly-once by ownership is
   incompatible with retries: a second attempt needs the task again. dagr needs
   re-runnable nodes (`&mut self` + attempt loop per C1/C14).
3. **Consume-once output retrieval.** `DagOutput::get` removes the Arc,
   downcasts, and `Arc::into_inner`s it (refcount provably 1). Under resume,
   artifacts, and multi-reader fan-out, outputs must persist — dagr's slots
   hand out Arc clones with zombie-aware release (C10).
4. **Type-erased outputs cannot serialize.** `Arc<dyn Any + Send + Sync>` is
   fine in-memory; dagr's event stream (029) and resume (070) require
   serialization/artifact-store bounds at the slot boundary — a constraint dagx
   never faces. Decide the bound at the boundary (017/T4), not per call site.
5. **No cancellation, timeouts, permits, or partial-failure isolation.** First
   error aborts after the current layer; siblings finish, downstream never
   starts; recoverable errors are pushed into userland `Result<T, E>` outputs
   (circuit-breaker/error-boundary patterns live in dagx's tests, not the
   framework). dagr owns all of this: trigger rules, failure policy (044),
   cancellation/drain (045), admission permits (041), per-attempt timeouts (031).
6. **Minimal error type.** `DagError` carries only `task_id: u32` +
   `panic_message: String`. dagr's error taxonomy (016/T3) needs rich per-node,
   per-attempt context classified into the terminal-state taxonomy.
7. **Doc/code drift warning.** dagx's lib.rs claims `DagOutput::get` returns
   `DagResult<T>`; the code panics. Keep dagr's fallibility story consistent
   between rustdoc and code — dagr's rustdoc lint and criteria matrix exist to
   catch exactly this.

Bottom line: dagx contributes almost nothing to dagr's runtime half
(persistence, retries, permits, cancellation, resume, events). Treat its
simplicity as scope discipline to admire, not architecture to copy.
