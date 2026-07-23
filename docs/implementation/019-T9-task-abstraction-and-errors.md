# 019 · T9 — C1: task abstraction and error classification

> **Milestone:** M1 · **Size:** M · **Type:** feature · **Components:** C1
> **Branch:** `feat/t9-task-abstraction-and-errors` · **Depends on:** T1, T2, T3, T0.2 · **Blocks:** T10, T16

## Why / context
This ticket lands the atomic unit of work every later component wires, drives, and observes: the task abstraction of C1 (arch.md `### C1 · Task`) and the task-facing edge of the execution-class declaration from C13 (arch.md `### C13 · Execution class dispatch`). It builds directly on the decisions already frozen upstream: the crate skeleton (T1), the tokio/blocking/compute runtime shape (T2), the three-valued task-facing error enum vs the runner's superset outcome taxonomy (T3), and the output ownership/sharing model with its author-visible bounds (T0.2). Nothing here schedules, retries, or logs — this ticket delivers only the *declaration surface* the task author writes against, so that the driver (T16 run context, T20 attempt runner) and the handle layer (T10) can build on a stable, typed shape. The central open decision — how a task is *expressed* (trait vs generic struct vs closure wrapper) and how "types readable from the declaration" is judged — is locked by this ticket.

## Objective
Deliver the C1 task abstraction as a code-free-to-the-author-once-decided, statically typed unit of work, exposing exactly four declared elements and the classified error, with the spec's bounds enforced by the type system.

- Provide the task declaration form that names four things: the consumed input type, the produced output type, the execution class (defaulting to await-bound), and the work itself.
- Make the work signature take the task exclusively (`&mut self`), receive a run-context reference and the declared input, and return either the produced output or a classified error.
- Support constructor-captured configuration: a task is a value that holds fields set when it was constructed (threshold, target location, rule set), not a bare function.
- Support the no-input task: a task that consumes nothing and still produces a value.
- Enforce the spec bounds at the type level: task values are `Send + 'static`; output values are additionally `Send + Sync + 'static`. Do not implement the durable reference contract here — only leave the seam for it (C5/C27, later tickets).
- Provide the task-facing error type classifying at minimum retry-eligible failure, permanent failure, and deliberate skip, per T3's three-valued task-facing enum (the runner's superset stays out of this ticket).
- Wire the execution-class declaration as the fourth declared element with await-bound as the default when unspecified; this ticket declares and carries the class only — dispatch onto pools is C13/T33.
- Record, in an ADR or the ticket's design note, the resolution of the expression-form question and the rule for judging readable types.

## Test plan (write these first — TDD)
Every scenario is independently checkable; several are compile-fail cases that belong in the compile-failure harness (T8) and several are behavioral unit tests.

- **Readable declaration (positive).** Setup: define a task whose input type and output type are stated in its declaration. Action: inspect the declaration alone, without reading the work body. Expected: both the input type and the output type are recoverable from the declaration; a test that names those types and constructs/uses the task compiles, demonstrating they are surface-level facts.
- **No-input task produces a value.** Setup: define a task declaring that it consumes nothing and produces some concrete output type. Action: run its work with a stub run context and no input. Expected: it returns the produced value; the declaration compiles and requires no placeholder/unit dance the author has to explain.
- **Constructor-captured configuration is honored.** Setup: construct two task values of the same task type with different captured configuration (e.g. two thresholds). Action: run each task's work over the same input. Expected: each produces the output determined by its own captured configuration, proving the task is a value holding state, not a shared function.
- **Same task type, several values.** Setup: construct three independent task values of one task type, each with distinct configuration. Action: treat each as its own unit. Expected: they are three distinct values with independent configuration; nothing about constructing one affects another (registration into a flow is out of scope here — T10/T13).
- **Exclusive `&mut self` work signature.** Setup: define a task whose work mutates a captured field. Action: invoke the work twice in sequence against the same task value. Expected: the second invocation observes the mutation from the first; the signature takes the task exclusively so no synchronization is written by the author, and a test that tries to invoke the work through a shared reference fails to compile (compile-fail case).
- **Error classification round-trips.** Setup: construct one task-facing error of each class — retry-eligible, permanent, and deliberate skip. Action: match on / classify each. Expected: each is distinguishable as its own class; a retry-eligible value is never observed as permanent or skip, and vice versa. Include a case carrying an underlying cause to confirm the source error is preserved.
- **Work returns a classified error.** Setup: define a task whose work is arranged to fail. Action: run the work. Expected: the return is the error variant (not the output), and the returned error carries the intended classification.
- **Task-type bound: non-`Send` capture fails.** Setup: attempt to construct a task that captures a non-`Send` value. Action: compile. Expected: compilation fails; this is the documented most-common first-hour error and the failing example is the one referenced in the rustdoc (compile-fail case; the doc example is the mirror of this test).
- **Output-type bounds: missing `Sync` fails.** Setup: declare a task whose output type is `Send + 'static` but not `Sync`. Action: compile. Expected: compilation fails, because outputs must be `Send + Sync + 'static` so concurrent consumers can read them (compile-fail case).
- **Output-type bounds: missing `'static` fails.** Setup: declare a task whose output type borrows data (not `'static`). Action: compile. Expected: compilation fails (compile-fail case).
- **Execution class defaults to await-bound.** Setup: define a task that does not state an execution class. Action: read the class the task reports. Expected: it is await-bound. Setup a second task that states blocking (or compute) and confirm it reports that class instead.
- **Re-runnability contract holds.** Setup: define a task whose work is deterministic given its captured configuration and input. Action: invoke the work twice with equivalent input. Expected: both invocations succeed and produce equivalent output — the unit is safely re-runnable, which is what a retry relies on (retry itself is out of scope; this checks only that the *shape* permits it).
- **Author writes business logic only (documentation-backed).** Setup: take a representative task from the examples. Action: review its body against a checklist. Expected: the body contains no scheduling, retry, permit, timeout, or logging code — and a note confirms that removing those framework features would require no change to the body. This is validated by the acceptance-criterion checklist item below plus the example content, not by a runtime assertion.

## Definition of done
- [ ] A task declares exactly four things — consumed input type, produced output type, execution class, and the work — and the input/output types are readable from the declaration without reading the body.
- [ ] The work receives a run-context reference and the task's input and returns either the produced value or a classified error.
- [ ] A task is a value holding constructor-captured configuration; constructing the same task type several times yields several independent values.
- [ ] A task that consumes nothing can be defined and produces a value.
- [ ] The work takes the task exclusively (`&mut self`); the author writes no synchronization, and invoking the work through a shared reference fails to compile.
- [ ] The task-facing error distinguishes at minimum retry-eligible failure, permanent failure, and deliberate skip, matching T3's three-valued task-facing enum; the runner's superset outcome taxonomy is not introduced here.
- [ ] Task values are bounded `Send + 'static`; output values are bounded `Send + Sync + 'static`; violating either bound is a compile error.
- [ ] The `Send + 'static` (task) and `Send + Sync + 'static` (output) bounds are documented on the task declaration itself, with a worked example of the most common first-hour error — capturing a non-`Send` value in a task — mirrored by a compile-fail test.
- [ ] Execution class is the fourth declared element and defaults to await-bound when unspecified; the task carries/reports the declared class. (Dispatch onto pools is out of scope — C13/T33.)
- [ ] The unit is safely re-runnable in shape: task bodies contain business logic only (no scheduling, retry, permit, timeout, or logging code), and removing the framework's retry/timeout/logging features would require no change to any task body.
- [ ] The durable-output reference contract seam (C5/C27) is left unimplemented but not foreclosed; nothing here blocks a later durable marker from adding that bound.
- [ ] The expression-form decision (trait impl vs generic struct vs closure wrapper) and the rule for judging "types readable from the declaration" are recorded in an ADR/design note, including — if closures are permitted — how the readable-types rule applies to them.
- [ ] Compile-fail scenarios are added to the T8 harness; behavioral scenarios are unit tests; the acceptance-criteria coverage matrix (T7) is updated to map each C1 criterion to its test.
- [ ] Rustdoc on the public task surface documents the bounds and the worked first-hour-error example, and the crate's rustdoc lint passes.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
- ~~Trait impl vs generic struct vs closure wrapper as the task expression form — and, if closures are permitted, how "types readable from the declaration" is judged for a closure.~~ **Resolved** — see the design note below.

## Design note — the expression form (resolves the open question)

**Decision: a `Task` trait, implemented on an author-owned configuration
`struct`. Not a generic struct wrapper, and not a closure wrapper.** Recorded
here as the ticket's design note (the DoD permits "an ADR or the ticket's design
note"). Implemented in
[`crates/core/src/task.rs`](../../crates/core/src/task.rs).

The trait declares C1's four elements as trait members: `type Input` and `type
Output` (the consumed and produced types), `const EXECUTION_CLASS:
ExecutionClass` (defaulting to `AwaitBound`), and `fn run(&mut self, ctx:
&RunContext, input: Self::Input) -> impl Future<Output = Result<Self::Output,
TaskError>> + Send`. The type-level bounds are supertrait / associated-type
bounds: `Self: Send + 'static` and `Output: Send + Sync + 'static`.

**Rule for judging "types readable from the declaration."** A task's input and
output types are readable iff they are the trait implementation's associated
types — `<T as Task>::Input` and `<T as Task>::Output` — which are stated in the
`impl Task for T { type Input = …; type Output = …; }` block, recoverable by name
without reading the `run` body. The positive test
`input_and_output_types_are_readable_from_the_declaration`
(`crates/core/tests/task_abstraction.rs`) names both via the associated types
alone, and its compilation is the assertion. This is the mechanical form of the
human-judged C1 criterion (coverage matrix: C1 is `human`/release-checklist).

**Why not a generic struct wrapper.** A `struct Task<In, Out, F>` holding a
closure `F` would push the input/output types into type parameters of a
framework type rather than an author-declared `impl`; the readable-types rule
would then depend on how the author spelled the turbofish at the construction
site, which is exactly the ambiguity the criterion warns against. It also cannot
hold arbitrary constructor-captured configuration as first-class named fields the
way an author `struct` does.

**Why not a closure wrapper (and the closure-readability sub-question).** A bare
`FnMut(&RunContext, In) -> impl Future<…>` has **no** associated-type surface: a
closure's input and output types are inferred from its body and appear nowhere in
a declaration a reviewer can read without the body, so "types readable from the
declaration" cannot be satisfied for a closure without re-introducing explicit
type annotations that a trait `impl` states more naturally. The dagx prior art
(`§2`, routed to this ticket) reached the same conclusion empirically: its
internal closure-task adapter was kept **out** of the public API because "the
ergonomics are bad," and it steers authors to the trait/macro form. dagr
therefore does not offer a closure form; the readable-types rule is defined only
over the trait's associated types, so the closure sub-question is answered by
**not permitting closures**.

**Why `&mut self`, not `self` by value (dagx caution).** dagx's `Task` consumes
`self` (`run(self)`), which encodes run-exactly-once — but arch.md C1 and the
T0.2 ADR require `&mut self` so the runner can drive **sequential attempts**
(retries, C14) against the same task value without the author writing any
synchronization. Consuming `self` is incompatible with retry; `&mut self` is the
spec-mandated shape and is what the exclusive-access compile-fail UI sample
(`task_mut_self_through_shared_ref`) guards.

## Out of scope
- Typed handles and registration returning a handle (C2 / T10) — this ticket produces the task value; wiring it into a flow comes next.
- The data-dependency binding, exact type matching, arity, fan-out, and the concrete ownership delivery (by-value / shared-read / clone-on-read) at edges (C3 / T11). This ticket enforces only the type-level bounds; per-edge mode conflicts are *assembly* errors that require whole-graph facts and belong to the binding/assembly tickets.
- The run context's actual capabilities (C8 / T16) — tests here use a stub context; the real context is a downstream dependency this ticket only references by shape.
- The attempt runner, sequential-attempt enforcement across a node, retries, timeouts, permits, and logging (C14/C15/C12/C25 and their tickets). This ticket guarantees the *shape* that makes sequential re-runs safe (`&mut self`), not the runner that performs them.
- Execution-class *dispatch* onto the await/blocking/compute pools and the isolated framework runtime (C13 / T33 / T2). Only the task-facing class declaration and its default live here.
- The runner's full outcome taxonomy including `abandoned` and `satisfied-from-prior` (T3's superset) — the task-facing enum stays strictly three-valued.
- The durable-output reference contract implementation (C5/C27) — left as an unimplemented seam only.
- Any move toward runtime-mutable graph shape, scheduling, distribution, a metadata store, a web UI, a DSL, or backfill — permanently outside dagr.
