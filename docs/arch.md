# dagr — component specification

A plain-language breakdown of every atomic piece of the tool, what each one does, and the acceptance criteria that prove it works. Deliberately light on implementation. Each component is independently buildable and independently testable; the last sections describe how they stack, what "done" means for the whole product, and the operational and stability commitments the product makes.

---

## What this is

A developer writes units of work in Rust, declares how they connect, and compiles one binary. That binary *is* the pipeline. Running it executes the whole graph on whatever machine it was launched on, in the right order, with the right concurrency, and leaves behind a record of what happened.

There is no server, no scheduler, no database, no configuration file describing the graph, and no parsing step. The graph is expressed in code the compiler has already checked.

**What it is not, permanently:** a scheduler, a distributed execution system, a metadata store, a web interface, a domain-specific language, or a backfill orchestrator. Every one of those is a reasonable thing to want and none of them belong here.

**Also permanent: the graph's shape never changes at runtime.** A task that discovers N files at runtime does not become N nodes. The blessed pattern is one node that iterates internally with bounded concurrency, declaring the cost of its internal parallelism honestly (C5, C12) and reporting per-item progress through metrics (C23) and scratch checkpoints (C18). A bounded intra-task executor that draws sub-permits from the admission pools may be added later; runtime graph expansion will not.

---

## Vocabulary — terminal states and trigger rules

These two lists are normative. Every component that mentions a state or a rule means exactly one of these.

**Terminal states.** Every node ends a run in exactly one of:

| State | Meaning |
|---|---|
| `succeeded` | The task returned a value; the slot was filled. |
| `failed` | Permanent failure, retries exhausted, or a caught panic. |
| `timed-out` | The final attempt exceeded its per-attempt timeout. |
| `skipped` | The task itself returned a deliberate skip (an *originated* skip). |
| `upstream-skipped` | Never ran because an upstream skip propagated to it. Carries the identity of the originating node. |
| `upstream-failed` | Never ran because its trigger rule can no longer be satisfied due to an upstream failure. |
| `cancelled` | Observed the cancellation signal and returned promptly, or was never admitted after cancellation began. |
| `abandoned` | Was asked to cancel and never returned within the grace period; its thread was left behind. A *timed-out* attempt whose thread never returns stays `timed-out` — the leftover thread is recorded as a zombie event in the stream (C19), never as a second terminal state. |
| `satisfied-from-prior` | Not executed in this run; resume (C27) carried its prior success forward — its durable output was rehydrated, or its value was never demanded. Carries the originating run identity. |

Skips are propagated as `upstream-skipped`, failures as `upstream-failed`; the originated and propagated forms are distinct states, so artifacts and diagrams (C22, C24) can always distinguish the node that decided to skip from the nodes that were skipped as a consequence. Every node ends in exactly one of these states, exactly once. (`not-requested`, which appears in single-node replay artifacts, is an artifact marking for nodes outside the request — C26 — not a terminal state.)

**State classes.** Trigger rules are defined over classes, so they are total over the taxonomy — every state belongs to exactly one class:

- *success-like*: `succeeded`, `satisfied-from-prior`
- *skip-like*: `skipped`, `upstream-skipped`
- *failure-like*: `failed`, `timed-out`, `abandoned`, `upstream-failed`
- *stop-like*: `cancelled`

**Trigger rules.** The set is closed. A rule is evaluated for a node only once *every* upstream of that node is terminal — a rule never fires early on a partial result.

- `all-succeeded` (default): fires when every upstream is success-like. Can never fire once any upstream reaches a state in any other class; the node is then marked `upstream-skipped` when every non-success upstream is skip-like, `cancelled` when every non-success upstream is stop-like, and `upstream-failed` otherwise.
- `all-terminal`: fires when every upstream is terminal, regardless of class. Can always fire; it never propagates failure.
- `any-failed`: fires when every upstream is terminal and at least one is failure-like — a transitively `upstream-failed` upstream counts, because the guarded work did not complete either way. If no upstream is failure-like, the node is marked `skipped` (a contingency that never arose).

Non-default rules are only expressible on nodes that consume nothing (C4), so a rule firing without an upstream *value* can never leave a hole where an input was expected. Data-dependent nodes always use `all-succeeded` (C3), and that restriction is enforced at compile time.

Branching is expressed in the task, not the graph: a task that decides "nothing to do" returns a deliberate skip, and the skip propagates (C15). When a downstream join must run even if one branch declined, the branches should succeed with an explicit "empty" value (`Option`, an empty batch) instead of skipping — the cookbook covers both patterns.

---

## The shape of a run

A run passes through four phases, and the boundary between the first two is load-bearing:

1. **Assembly (pure).** Registrations are collected and checked. No network, no filesystem, no clock, no credentials, no parameter values (C7). This is what allows the graph to be emitted and validated in CI on every pull request.
2. **Bootstrap (deliberately impure).** Everything the runtime needs from the actual machine and the actual invocation happens here, in one named place: generate run identity, open the run store and the event stream, construct the resource registry and check it against declared requirements, probe container limits and size the admission pools, reject any node whose declared cost cannot fit, parse and validate parameters, capture allowlisted environment values. For the run verbs, the first two steps — mint identity, open the store and stream — happen *before* assembly executes, so even an assembly failure has a place to record itself; assembly itself stays pure (opening the store is the runtime's act, not the graph's), and the inspection verbs run assembly with no store at all. Bootstrap fails fast — a bootstrap failure produces artifacts (C22) and a distinct exit code (C26), and never hangs.
3. **Execution.** The readiness tracker, admission controller, and attempt runner do their work (C11–C18).
4. **Shutdown.** Drain or cancel in-flight work, run teardown nodes, flush the event stream, exit with a truthful code.

**The run store.** One operator-supplied base location — a local path by default, supplied by flag or environment variable — under which everything a run leaves behind lives: `<base>/<pipeline>/<run-id>/` holds the event stream, both artifacts, and scratch. The event stream is written through an injected sink (append a line, flush); the default sink is a local file under the run store. Pointing the base at storage that survives the container — a mounted volume, a synced directory — is the operator's one job, and it is the *only* infrastructure this tool ever asks for. Runs whose store did not survive are not resumable; everything else works regardless.

---

## Layer A — Authoring surface

The parts a pipeline developer touches directly.

### C1 · Task

**Purpose.** The atomic unit of work.

**Behavior.** A task declares four things: the type of value it consumes, the type of value it produces, its execution class (C13; default await-bound), and the work itself. The work receives a run context and its input, and returns either a produced value or a classified error. A task is a *value*, not a bare function — it holds configuration captured when it was constructed, such as a threshold, a target location, or a rule set. Registration moves the task value into the flow; "the same task type appearing several times" means constructing several values, each registered as its own node.

**Ownership of inputs.** A task declares the *type* of value it consumes; *how* it receives that value — by ownership, or as shared read access for the duration of the attempt — is a declared receive mode, visible in the signature. Type mismatches are compile errors (C3); mode conflicts are assembly errors, because they depend on whole-graph facts (consumer counts, retry policy) that only exist once every registration is in. The rules:

- A value with multiple consumers is delivered by shared read access. A consumer that demands ownership of a multiply-consumed value fails assembly, naming both consumers — or the edge explicitly opts into clone-on-read, requiring the value's type to be cloneable, with the memory multiplication that implies. Registering a second consumer for a value whose sole consumer takes ownership is therefore an assembly error at the new registration, not a silent behavior change.
- Ownership delivery consumes the slot: once the value has been moved into an attempt, the framework has no copy left. A consumer that takes ownership therefore may not have retries — an owned-input edge into a node with a nonzero retry count fails assembly unless that edge opts into clone-on-read (each attempt receives a fresh clone). Shared-access consumers retry freely; the slot still holds the value (C10).

Task values must be sendable to another thread and free of borrowed data (`Send + 'static`); the work takes the task exclusively (`&mut self`), which is what makes sequential attempts safe without any synchronization by the author. Output values must be sendable and shareable (`Send + Sync + 'static`) so concurrent consumers can read them; a node marked durable (C5, C27) additionally requires its output type to implement the reference contract.

A task must be safely re-runnable, because a retry does exactly that. Task authors write business logic only: never scheduling, retry, permit, or logging code.

**Acceptance criteria.**
- A task's input and output types are readable from its declaration without reading its body.
- A task that consumes nothing can be defined and produces a value.
- The error a task returns distinguishes at minimum: retry-eligible failure, permanent failure, and deliberate skip.
- A node's attempts are strictly sequential — attempt *n+1* never starts before attempt *n* has fully returned — and the task author does nothing to ensure this.
- A sole consumer can receive its input by value; a multi-consumer input is received as shared access; demanding ownership of a shared value fails at assembly (naming both consumers) unless clone-on-read is stated on that edge.
- An owned-input edge into a node with retries fails assembly unless the edge opts into clone-on-read; a shared-access consumer with retries finds its input intact on every attempt.
- The bounds on task and output types (`Send + 'static`; outputs additionally `Sync`) are documented on the task declaration itself, with a worked example of the most common first-hour error: capturing a non-`Send` value in a task.
- Removing the framework's retry, timeout, and logging features would require no change to any task body.

### C2 · Handle

**Purpose.** A typed claim on a value that does not exist yet.

**Behavior.** Registering a task with a flow returns a handle. Every registration supplies an explicit node name; names are unique across the pipeline (checked at assembly) and stable under declaration reordering — identity comes from the name, never from registration order. The handle carries the node's identity and the type of the value it will eventually hold. Handles are cheap and freely copyable. A handle is the *only* way to refer to another task's output — there is no lookup by name, index, or string key.

Because a handle can only be obtained by registering a node, and a node can only depend on handles that already exist — this holds for ordering edges too (C4) — a cycle cannot be expressed. This is structural, not a validation pass that runs later.

**Acceptance criteria.**
- Handles can be copied and passed around freely during pipeline construction.
- No API exists to obtain a handle for a node that has not been registered.
- No API exists to retrieve a node's output by name, index, or string key.
- An attempt to express a cycle — through data edges or ordering edges — fails to compile, demonstrated by a checked-in compile-failure test.
- Renaming a node changes its identity (and the structural fingerprint); reordering registrations changes nothing.

### C3 · Data dependency

**Purpose.** Declares that one task consumes another's output.

**Behavior.** Binding one or more handles to a task. The *value types* of the bound handles must exactly match the consuming task's declared input types; a mismatch is a compile error. The *receive mode* (owned versus shared — C1) is not part of type matching: mode conflicts are whole-graph facts and are rejected at assembly. A data dependency implies both ordering *and* that the upstream must have succeeded — if it didn't, there is no value, so the downstream input cannot be formed.

Multiple inputs bind as a tuple, up to a documented maximum arity; at the cliff, a curated diagnostic message says so rather than a wall of trait errors. Beyond the ceiling, aggregate upstream values into a struct produced by an intermediate node.

**Acceptance criteria.**
- Binding a handle of the wrong type is a compile error whose message contains both the expected and the supplied type names (verified by UI tests against the pinned workspace toolchain — C28).
- Binding a different number of handles than the task declares is a compile error.
- A node with data dependencies cannot be given any trigger rule other than `all-succeeded` — the builder's typestate makes it inexpressible, a compile error rather than a runtime check.
- One handle can be bound to any number of downstream tasks.
- The maximum input arity is documented, and exceeding it produces the curated message.

### C4 · Ordering dependency

**Purpose.** Declares "run after," with no data flowing.

**Behavior.** Constrains sequence only. Used where a task's *effect* rather than its *value* matters to a later task: cleanup after publish, cache warm before read. An ordering edge may attach to **any** node, including one that also has data dependencies — it is *non-default trigger rules* that are restricted to nodes that consume nothing (see Vocabulary), which is what keeps a rule from ever leaving a hole where a value was expected.

Ordering edges are declared at the downstream node's registration, against handles of already-registered upstreams — the same backward-reference discipline as data edges, which is what preserves C2's compile-time cycle guarantee.

Under the default `all-succeeded` rule, ordering upstreams count exactly like data upstreams: they must succeed, and skips and failures propagate across ordering edges just as they do across data edges. A node that should run regardless of how its ordering upstreams ended says so with `all-terminal`.

**Acceptance criteria.**
- An ordering edge can be declared at registration time against any already-registered node; no API exists to add an edge between two existing nodes afterward.
- A node may carry both data dependencies and additional ordering edges.
- Ordering edges are recorded distinctly from data edges in the graph artifact and drawn distinctly in diagrams.
- A node attached only by ordering edges receives no value, and its declaration reflects that.
- Under the default rule, an ordering upstream's failure or skip propagates to the downstream node exactly as a data upstream's would.

### C5 · Node policy

**Purpose.** The per-node operational knobs, kept separate from the task's logic.

**Behavior.** Attached at registration, never inside the task. Covers: retry count and backoff shape, per-attempt timeout, declared resource cost, trigger rule, execution class override, group membership, whether the output is retained after its consumers finish (C10), and whether the output is durable (C27).

**Declared cost** is a vector with one entry per admission pool (C12), in that pool's native unit: bytes for the memory pool, a thread count for the thread pools. Memory cost splits into *working memory* (held for the attempt, released at its terminal state) and *output residency* (transferred to the output slot when the value is produced, released when the last consumer is terminal — C10). Declare honestly: the run artifact juxtaposes declared against measured cost precisely so that dishonest declarations are visible (C12, C23).

**Execution-class override** is constrained by the shape of the work: synchronous work may move between the blocking and compute classes; await-bound work cannot be overridden to a synchronous class. An invalid override fails at assembly.

Defaults are conservative and stated explicitly: no retries, no timeout, zero declared cost, `all-succeeded`, execution class as declared by the task (await-bound if unspecified), no group, release the output once consumed, not durable.

**Acceptance criteria.**
- Every policy field has a documented default, applied uniformly.
- Every node's full effective policy appears in the graph artifact, including defaulted values.
- Changing any policy value requires no change to task code.
- A node with no stated policy behaves identically to one with every default written out — including under the fingerprint's policy hash (C21).
- An execution-class override incompatible with the task's declared work shape fails assembly.

### C6 · Group

**Purpose.** A naming and presentation namespace.

**Behavior.** A label attached to nodes for readability. Affects artifact organization and diagram clustering. Groups do not nest. A group label is presentation metadata: it is *not* part of node identity and is excluded from the graph fingerprint (C21), so renaming a group shows up in the structure diff (C28) but never breaks resume. It carries no execution semantics — no group-level concurrency limit, no group-level failure handling. Resisting that temptation is the point.

**Acceptance criteria.**
- Node names are unique across the whole pipeline regardless of grouping.
- A rendered diagram visually clusters nodes by group.
- Removing or renaming every group changes no execution behavior and no fingerprint.

### C7 · Flow assembly

**Purpose.** Collects registrations into a runnable, inspectable whole.

**Behavior.** A builder accumulates nodes and produces an immutable pipeline. Assembly performs the checks the compiler cannot: duplicate node names, an empty pipeline, invalid execution-class overrides, duplicate stable type/task names (C20), durable-marked nodes whose output type lacks the durability contract (C27). A node whose non-unit output has zero consumers and is neither retained nor durable is reported as a warning — it is usually a wiring mistake, but a legitimate effect-only node is common enough that it is not an error. Assembly also computes what the runtime needs — consumer count per node, remaining-dependency counts, execution order, and the graph fingerprint.

Assembly is pure: no network, no filesystem, no clock, no credentials, and *no parameter values* — parameters are parsed at bootstrap, after assembly, so the graph provably cannot depend on them. This is what allows the graph to be emitted and tested in an empty environment.

Checks that need the actual machine — capacity, resources, credentials — belong to bootstrap (see "The shape of a run"), which runs them before any node executes and fails fast with artifacts.

**Acceptance criteria.**
- Assembling the same pipeline twice in one process produces byte-identical graph artifacts (the generation-time field aside, per C20).
- A duplicate node name fails assembly and names both declarations.
- Assembly succeeds with every external resource absent, proven by a test that runs it in an empty environment.
- Assembly reports *all* problems it finds, not only the first.
- Consumer counts are exact for every node before any execution begins.
- No parameter value is reachable during registration or assembly.
- Bootstrap rejects, before any node executes: a declared cost no pool can ever satisfy, a missing declared resource, an invalid parameter — each with a distinct, complete error report and an assembly/bootstrap-failure artifact.

---

## Layer B — Execution core

The parts that run the pipeline. A pipeline developer should be able to ignore all of this.

### C8 · Run context

**Purpose.** What every task is told about the run it is part of.

**Behavior.** A read-only handle passed into every invocation. Carries: run identity, pipeline identity, node identity, current attempt number and the maximum, the run's parameters, an optional data interval, a cancellation signal, a logging span, access to the resource registry, and access to the durable scratch store. Teardown nodes (C17) additionally see the terminal states of the nodes they cover, so cleanup can no-op when setup never ran.

**The data interval** is a caller-supplied, tool-opaque pair of values recorded verbatim in artifacts. The tool never computes an interval, never advances one, and never persists one between runs — a backfill is the *caller* looping over invocations with different intervals. This is the boundary with "backfill orchestrator," stated here so nobody rediscovers it in a design meeting.

It holds no mutable shared state and offers no route back to the scheduler.

**Acceptance criteria.**
- Every field is populated on every invocation, including the first attempt of the first node.
- The attempt number increments across retries and appears in both logs and artifacts.
- No context API can modify the graph, reorder work, or influence scheduling.
- A context can be constructed by hand in a unit test so a single task can be exercised with no runtime running.
- The data interval appears in artifacts exactly as supplied, and no framework code path interprets its contents.

### C9 · Resource registry

**Purpose.** Dependency injection for long-lived external clients.

**Behavior.** Built once by the developer's own code in `main` — the framework fetches nothing from anywhere — and shared immutably for the run. Holds object storage clients, database connection pools, HTTP clients, secret material. Tasks retrieve what they need by type; two resources of the same underlying type are distinguished by newtype wrappers, which is the same no-string-lookup philosophy as C2 — and registering a second resource of the literally identical type fails registry construction as ambiguous, rather than silently replacing the first. Resources are `Send + Sync + 'static`; a client that is not thread-safe is wrapped in the documented owning-worker pattern (one thread owns it, others reach it through a channel).

Each node *declares* the resource types it requires at registration. Bootstrap validates the registry against the declared requirements — a missing resource is a startup failure naming both the resource and every node requiring it, never a mid-run surprise at three in the morning. Declared requirements appear in the graph artifact.

**Secrets** are marked as such when placed in the registry, via a wrapper with no `Debug`/`Display` path. The framework never serializes registry contents into artifacts or its own log lines, and additionally scrubs marked secret values from framework-controlled output paths. A task author who formats a secret into their own log line is outside the guarantee — that boundary is stated in C25.

This replaces the connections-and-variables pattern from hosted schedulers: no lookup service, no per-task credential fetch, no network round trip to find out where the database is.

**Acceptance criteria.**
- A run whose pipeline declares an unregistered resource fails at bootstrap, before any node executes, naming both the missing resource and the nodes requiring it.
- The registry cannot be mutated once the run begins.
- Any resource can be replaced with a fake in a test without modifying task code.
- Two same-typed resources are distinguishable via the newtype pattern, demonstrated in a documented example.
- Marked secret values never appear in artifacts or framework-emitted log lines, verified by a test that plants a sentinel value.

### C10 · Output slot

**Purpose.** Holds a produced value between its production and its last consumption.

**Behavior.** Each node owns exactly one slot, typed to that node's output, empty until the node succeeds. Downstream nodes hold a direct reference to that slot, established at assembly time, so reading it requires no lookup and no runtime type check.

Each slot knows how many consumers remain. The value is released when **every consumer has reached a terminal state and every consumer's work has actually returned** — not when the last one has read it, because a consumer that read the value and then failed a retry-eligible attempt must find its input still there on the next attempt. The second condition matters for zombies: an abandoned-but-running consumer (C14) still holds its read access, so the value cannot be reclaimed — and is not *counted* as reclaimed (see accounting below) — until that closure returns. Nodes explicitly marked retained keep their value until the run ends; the retained value is redeemable by the embedding program after the run completes (the handle can be exchanged for the value once the run has ended), which is the knob's entire purpose.

**Memory accounting.** A slot's value is counted once against the memory pool, not once per consumer: the producer's declared output residency transfers from the producing attempt to the slot when the value is produced, and is released only when the slot actually releases — which, per the rule above, waits for zombie consumers to return, so the pool never regains capacity for bytes a leftover thread still pins (the same honesty rule as C12's zombie accounting). Retained values are counted until run end. "Memory reclaimed" means returned to the allocator, not necessarily to the operating system — tests measure allocator-level residency, not process RSS.

**Authoring guidance.** In-memory values are for small results: parameters, summaries, manifests, keys. Anything large — a dataset, a file's contents — should be written to external storage by the task, with the *reference* as the node's output. That is also exactly what makes a stage boundary durable and therefore resumable (C27).

**Acceptance criteria.**
- A slot is never read before it is filled; a violation is a framework defect that fails loudly and names the node.
- A shared-access consumer that reads a value and then retries finds the value still available on its next attempt.
- After the final consumer of a node reaches a terminal state and every consumer's work has returned, that node's value is unreachable and its memory returned to the allocator; while an abandoned consumer's closure is still running, the residency stays counted against the memory pool.
- Peak allocator-level memory across a long chain does not grow with the chain's length when nothing is retained — verified against a synthetic hundred-node chain.
- Values still retained at the end of the run are identified in the run artifact and redeemable by the embedding program; released ones are not.
- Peak slot residency (measured) appears in the run artifact alongside declared output residency.

### C11 · Readiness tracker

**Purpose.** Decides what is eligible to run, and when.

**Behavior.** Maintains a remaining-dependency count per node. When any node reaches a terminal state, each dependent is decremented; a node's trigger rule is evaluated once all of its upstreams are terminal (see Vocabulary — rules never fire on partial results), and a node whose rule fires becomes ready. A node whose rule *can never fire* is immediately assigned its propagated terminal state (`upstream-failed`, `upstream-skipped`, or `skipped` for an `any-failed` contingency that never arose) without executing. A node becomes ready the instant its own dependencies allow — work is never batched into waves where a whole level must finish before the next begins.

**Acceptance criteria.**
- A node whose dependencies complete early starts before unrelated slower work finishes.
- In a diamond shape with one slow branch, the fast branch's descendants are not delayed by the slow branch unless they depend on it.
- Every node ends in exactly one terminal state from the normative taxonomy, and the run ends precisely when nothing is pending or in flight. *In flight* means attempts whose outcome is undecided: an abandoned-but-running closure is decided, so it does not hold the run open indefinitely — at natural run end the process waits up to the grace period (C16) for zombies to return, then exits, recording a zombie-at-exit event for each (C19).
- For every trigger rule, both the fires case and the can-never-fire case are covered by tests, including the resulting terminal states — with `satisfied-from-prior` upstreams covered explicitly (they count as success-like).
- The tracker cannot deadlock: a test over randomly generated graphs with randomized outcomes confirms every run terminates.

### C12 · Admission controller

**Purpose.** Stops the pipeline from exceeding the machine it is running on.

**Behavior.** Holds weighted capacity pools for the genuinely constrained resources — memory and threads at minimum. A ready node is admitted only when its declared cost fits the remaining capacity of *every* pool it needs; acquisition is all-or-nothing across pools (no permit is held while waiting for another — that is how two nodes deadlock). Admission order is oldest-ready-first with bounded bypass: a small node may jump the queue only when admitting it cannot delay the oldest waiter, so a large node is never starved by a stream of small ones.

The permit is held for the whole attempt and released on every terminal outcome — with one honest exception. Work that was timed out or cancelled but has not actually returned (a blocking closure cannot be killed — C14) is **abandoned-but-running**: it still occupies its thread and its memory, so its cost remains counted against the pools until the closure actually returns. The capacity invariant counts zombies, because a ledger that releases what is still running is a ledger that lies, and the container's OOM killer audits it.

Pool sizes are derived at bootstrap from the container's actual limits — cgroup v2 first, then cgroup v1, then host resources when neither exists (the development-machine case) — with a headroom fraction that defaults to 20%. Unlimited-sentinel values fall back to host resources. Every pool gets at least one unit (the compute pool has at least one thread even under a fractional CPU quota). An operator flag can pin any pool's capacity outright, which is also how CI makes capacity deterministic. A node whose declared cost exceeds any pool's total capacity is rejected at bootstrap — fail fast, never wedge at admission.

This is the component that turns a memory ceiling into a throughput limit instead of a crash, and it is the primary lever on infrastructure cost. It is honest only when declared costs are honest: the run artifact juxtaposes declared against measured cost per node so that bad declarations are visible, and a memory-constrained run warns about nodes with no declared cost.

**Acceptance criteria.**
- The combined declared cost of executing nodes — *including abandoned-but-running work* — never exceeds pool capacity.
- A node whose declared cost exceeds total capacity fails at bootstrap, not at admission time.
- Permits are released on success, permanent failure, retry-eligible failure, and cooperative cancellation, and for timeout and abandonment only when the underlying work has actually returned — each verified by a test that induces that specific outcome.
- Acquisition across multiple pools is atomic; a test with two contending multi-pool nodes proves no deadlock.
- A large-cost node ready behind a stream of small nodes is eventually admitted, verified by a starvation test.
- Pool sizes reflect the container's limit when one exists and the host's resources when one does not; the pinning flag overrides both.
- Time spent waiting for a permit is recorded separately from time spent executing.
- Declared and measured cost appear side by side in the run artifact.

### C13 · Execution class dispatch

**Purpose.** Puts each kind of work on the right kind of thread.

**Behavior.** Three classes. *Await-bound* work — network calls, waiting — runs on the async runtime. *Blocking* work — a synchronous database call — runs on a dedicated pool so it cannot starve the runtime. *Compute-bound* work runs on a fixed pool sized to the container's CPU allocation.

The class is declared by the task (C1) and may be overridden by policy within the limits stated in C5.

The framework's own machinery — timers, cancellation fan-out, the event-stream writer, signal handling — runs isolated from task execution, so misbehaving tasks can degrade *task* progress but can never disable the safety rails.

The async runtime is tokio, named as a supported public dependency: its types may appear where the API is honest about it, and the commitment is recorded in the Stability section. Context-exposed types are dagr-owned wherever practical.

**Acceptance criteria.**
- A long synchronous task does not delay progress on unrelated await-bound work.
- Concurrently executing compute-class tasks never exceed the compute pool's size.
- Misdeclaring a class may stall the run's progress, but never corrupts data, never corrupts artifacts already written, and never disables timeouts, cancellation, or the event stream — verified by a test in which every task worker is blocked and a timeout still fires and SIGTERM still yields a complete stream.

### C14 · Attempt runner

**Purpose.** Executes one node with all its operational behavior wrapped around it.

**Behavior.** For each attempt in turn: open a span, record the waiting and admission phases, start the timeout, dispatch by execution class, await the outcome, classify it, and then either fill the slot, schedule another attempt after a backoff, or reach a terminal failure.

Classification distinguishes retry-eligible failure, permanent failure, deliberate skip, timeout, and panic. Timeout is retry-eligible by default, subject to the node's retry budget.

**Timeout semantics differ by class, honestly.** An await-bound attempt that exceeds its timeout is truly cancelled — its future is dropped, and its permit releases immediately. A blocking or compute attempt cannot be killed: on timeout the attempt is *marked* timed out immediately (the event is emitted, the node's fate is decided), but the thread keeps running as abandoned-but-running work whose permit is held until the closure actually returns (C12). A retry of that node is deferred until the previous attempt's closure has returned — the alternative is the same task instance running concurrently with its own zombie, which violates C1's exclusivity. An abandoned attempt can never fill the output slot and can never write scratch; whatever it computes after its timeout is discarded. A node's terminal state is decided exactly once — a blocking timeout is and stays `timed-out`; `abandoned` arises only on the cancellation path (C16), never as a second state after `timed-out`. If the process is exiting while leftover threads are still running, they die with the process, each recorded as a *zombie-at-exit event* in the stream (C19), which changes no node's state.

**Panics.** A panic is caught, attributed to its node, and converted to a permanent failure rather than being allowed to unwind the run. The binary checks its panic strategy at startup and refuses to run under `panic = "abort"` with a clear message naming the required profile setting. The framework applies `AssertUnwindSafe` at the catch boundary; integrity of shared resources after a caught panic is the resource author's responsibility, and the prescribed pattern is poisoning — a pooled connection that may be mid-statement is marked broken, not returned to rotation. The framework installs its panic hook once, attributes panics to nodes via task-local state, and coexists with the test harness's own hook.

Backoff is exponential with jitter and a cap, all set per node.

**Acceptance criteria.**
- A retry-eligible error is retried up to the configured count and no further.
- A permanent error is not retried, regardless of remaining attempts.
- An await-bound task exceeding its timeout is cancelled and its permit released immediately; a blocking task exceeding its timeout is recorded as timed out immediately while its permit is held until the closure returns, and its retry starts only after that return — each verified separately.
- An abandoned attempt's late result never fills a slot and never writes scratch.
- A panicking task fails only its own node; the rest of the run proceeds per the failure policy.
- A binary built with `panic = "abort"` refuses to start, with a message naming the fix.
- Every attempt produces exactly one *attempt-outcome* record in the event stream (alongside its per-transition events — C19), including attempts that timed out or panicked.
- Backoff delays are jittered, so a fan-out of simultaneous retries does not resynchronize.

### C15 · Failure policy and propagation

**Purpose.** Decides what happens to the rest of the run when something fails.

**Behavior.** Two modes. *Stop on first failure* signals cancellation to everything in flight and admits no further default-rule work. What still runs after the stop, in order: the in-flight drain, then every consume-nothing node with a *non-default* trigger rule whose rule fires on the resulting terminal picture — a notify-on-failure or cleanup contingency is precisely the work a failure is supposed to trigger, and stop mode would be self-defeating if it cancelled it — then teardown nodes (C17). Nodes whose rules do not fire are marked per the Vocabulary's can-never-fire classes; pending default-rule nodes unrelated to the failure end `cancelled`. *Continue independent* lets branches unrelated to the failure run to completion. In both modes, propagation is governed by trigger rules: a node is marked `upstream-failed` only when its trigger rule can no longer be satisfied — so an `all-terminal` cleanup node downstream of a failure still runs, which is the entire reason non-default rules exist.

Deliberate skips propagate as `upstream-skipped` (see Vocabulary), carrying the originating node's identity, and a run containing only skips is a successful run.

**Acceptance criteria.**
- Under stop-on-first-failure, no default-rule non-teardown node is admitted after the first terminal failure is observed; a consume-nothing contingency node whose non-default rule fires on the final picture (e.g. `any-failed` on the failed node) still executes — both verified.
- Under continue-independent, a node with no ancestral relationship to the failure still completes.
- No node ever executes if any of its data dependencies did not succeed.
- A node whose trigger rule can still be satisfied after an upstream failure (e.g. `all-terminal`) executes; one whose rule cannot is marked `upstream-failed` without executing — both verified.
- Every node has exactly one terminal state in the run artifact, including nodes that never ran.
- A run whose only non-success outcomes were skips reports overall success, and every propagated skip records its originating node.

### C16 · Cancellation and shutdown

**Purpose.** Stops the run without leaving debris behind — and is honest about which debris it can actually prevent.

**Behavior.** A run-scoped cancellation signal with a per-attempt child. Triggered by a failure under stop-on-first-failure, an operator interrupt, or a termination signal from the orchestrator. On cancellation, in-flight work is asked to stop and given a grace period; a task that observes the signal and returns promptly is recorded as `cancelled`. Work that does not return within grace is recorded as `abandoned` — cancellation of synchronous work is cooperative-only, and the spec does not pretend otherwise.

The shutdown budget is arithmetic, not hope: grace period (default 10 seconds) plus the teardown deadline (default 15 seconds, C17) plus a bounded final flush (2 seconds) must fit inside the orchestrator's kill window — the defaults assume Kubernetes' 30-second `terminationGracePeriodSeconds`, and both values are operator flags. The binary prints its worst-case shutdown budget at startup so a misconfiguration is visible before it matters.

Cleanup guarantees are scoped to what is enforceable: tasks that observe cancellation within grace, and teardown nodes, clean up after themselves. After grace, the process exits promptly; abandoned threads die without destructors, and residual debris is the province of per-run temp-dir conventions (everything a task writes locally goes under the run's temp directory, deleted by the next invocation or the operator) and teardown nodes — which may race a zombie thread, so teardown's deletions are best-effort by design. If the event sink is unwritable at shutdown, the process waits a bounded time, then exits with the distinct sink-failure code (C26) rather than hanging.

**Acceptance criteria.**
- On a termination signal, the process writes a complete event stream before exiting, within the shutdown budget.
- A task that observes cancellation and returns promptly is recorded as `cancelled`; one that does not return within grace is recorded as `abandoned` — both distinct from `failed`.
- Grace and teardown deadlines are operator-configurable, and the worst-case shutdown budget is printed at startup.
- Temporary artifacts created by cooperative tasks are cleaned up on cancellation; the per-run temp directory is removed by the next invocation regardless.
- An unwritable event sink at shutdown produces a bounded wait and a distinct exit code, not a hang.

### C17 · Setup and teardown nodes

**Purpose.** Resource lifecycle that must happen regardless of outcome.

**Behavior.** A teardown node is ordered after a set of nodes and runs once those nodes have finished in *any* state — it fires only when all of them are terminal, so a teardown covering many nodes runs late by construction; attach teardowns narrowly. Its context exposes the covered nodes' terminal states, so it can no-op when setup never ran. Its own failure is recorded but does not change the run's outcome and does not prevent other teardowns from running. Teardown runs under a fresh, uncancelled signal with its own deadline (default 15 seconds), so a cancelled run still cleans up after itself. Teardown bypasses admission — it must not compete for capacity with the run it is cleaning up after — and its declared cost must be zero; assembly rejects a teardown node with a nonzero cost, which is what keeps the bypass consistent with C12's capacity invariant.

Teardown interacts with resume (C27): if a teardown that covers a node executed in the prior run, that node's durable output may have been destroyed, so resume treats such nodes as not satisfiable and re-executes them. The general rule, stated where developers will see it: outputs that teardown deletes are not resume-safe.

Setup nodes are ordinary nodes; the distinction exists only so teardown has something to attach to.

**Acceptance criteria.**
- A teardown node runs when its upstream succeeded, failed, was skipped, was cancelled, or was abandoned.
- A failing teardown node is recorded as failed, but the run's overall outcome is determined only by non-teardown nodes.
- Several teardown nodes all run even when one of them fails.
- Teardown nodes never have data dependencies, and their context exposes covered upstream terminal states.
- Teardown executes even when the run was cancelled by a termination signal, under its fresh signal and deadline.
- On resume, a node covered by a teardown that executed in the prior run is re-executed, never satisfied-from-prior.

### C18 · Durable scratch store

**Purpose.** Lets a task remember something across its own retries — and across a resume.

**Behavior.** A small key-value store scoped to one node within one run, reached through the context. Values are opaque bytes; serialization is the task's affair. Values written on one attempt are readable on the next. Intended for cursors, high-water marks, and "I already finished the first half" checkpoints — explicitly *not* for passing data between nodes, which is what data edges are for. There is no hard size bound, but the store is designed for values measured in kilobytes, and the documentation says so.

Scratch lives in the run store: on local disk under the run's directory normally, and under the durable base when the operator has pointed the run store somewhere that survives the container. On resume (C27), the scratch of every node that will re-execute is copied forward from the linked prior run into the new run's namespace — a resumed node continues from its checkpoint rather than starting over, which is half the point of checkpoints.

Cleanup: a node's scratch is deleted when the node succeeds. Scratch of nodes that did not succeed stays in the run's directory — that is exactly what a later resume copies forward — and is removed by prune (C26); nothing is deleted implicitly at run end. A scratch read or write failure is classified as a retry-eligible task failure — disk trouble is transient more often than not.

**Acceptance criteria.**
- A value written on attempt one is readable on attempt two.
- Keys are namespaced by run and node; two nodes cannot collide.
- A node cannot read another node's scratch values, and this is enforced rather than conventional.
- With a durable run store, a resumed run's re-executing nodes see the prior run's scratch values.
- Scratch of a succeeded node is gone; scratch I/O failure surfaces as a retry-eligible failure.

---

## Layer C — Artifacts and observability

The parts that make a finished run explicable.

### C19 · Event stream

**Purpose.** The crash-proof record of what happened. Everything else is derived from it.

**Behavior.** An append-only sequence of single-line records, written through the run store's sink (see "The shape of a run") as events occur rather than buffered until the end. Each record carries the run identity, a schema version, a monotonic sequence number, a wall-clock stamp (informational), and a monotonic offset from run start (authoritative — durations are computed from offsets, never from wall clocks). Every state transition is an event: run started, node became ready, node admitted, attempt started, attempt succeeded, attempt failed, node reached terminal state, zombie-at-exit (C14), run finished. The run-started event carries every run-artifact header field known at start — everything but the overall outcome and summary, which exist only at the end — so even a stream that ends one event later identifies its run completely. For the run verbs, identity is minted and the store and stream open *before* assembly executes, so even an assembly failure has a place to record itself; the verbs that only inspect the graph (validate, graph, render) run assembly without a store and report to standard output and exit codes. If opening the store itself fails, there is nowhere to write an artifact by definition: the error goes to stderr with the sink-failure exit code, and the spec promises nothing more.

"Flushed" means: no user-space buffering — each record is written to the sink before the transition is considered recorded — with an fsync at run end and at cancellation. Stronger per-event durability is the sink's business; the default local-file sink does not fsync per event, and the spec does not pretend the last block always survives a power cut: a reader tolerates and discards at most one trailing partial record.

A mid-run sink failure is a run-level fault: the run moves to cancelling with reason "event stream unwritable," makes a best-effort final report to stderr, and exits with the distinct sink-failure code. A pipeline that cannot record what it did should stop doing things.

Run identity is a UUIDv7, operator-overridable. Each run writes under its own `<base>/<pipeline>/<run-id>/` directory, so two simultaneous runs on one machine never share a file; analysis across runs is concatenation partitioned by run identity, which is safe because every record carries it. Retention is handled by the prune verb (C26); nothing is deleted implicitly.

The event stream is also the sole live-telemetry integration point: anything that wants runtime visibility tails the stream and ships it. Push-export of metrics from inside the process is a task/resource concern, deliberately not a framework feature.

Writing continuously rather than at exit is the whole point. The dominant failure mode in a container is an abrupt kill with no chance to run an exit handler, and a record that only materializes on clean shutdown is absent exactly when it is most needed.

**Acceptance criteria.**
- Killing the process abruptly at any moment leaves a stream whose every record but at most one trailing partial is valid and parseable.
- Every record carries the run identity and schema version; records from concurrent runs can be concatenated and partitioned safely.
- Sequence numbers are gapless and strictly increasing within a run.
- Two simultaneous runs of the same binary write disjoint files and both produce valid streams.
- The stream is written through the run-store sink, which the operator can point at storage that survives the container.
- A stream can be folded into a run artifact by a function that needs no access to the original run (C22, C26).
- An induced mid-run sink failure cancels the run and exits with the sink-failure code.

### C20 · Graph artifact

**Purpose.** The pipeline's structure, obtainable without executing it.

**Behavior.** Emitted on demand by the binary itself. Describes every node — name, group, stable task name, stable input and output type names, execution class, complete effective policy, declared resource requirements, and dependency lists — and every edge, including whether it carries data or only ordering, and for data edges the stable name of the type carried.

**Stable names are author-declared**, via a constant on the task and payload types (a derive will make this one line); uniqueness is checked at assembly. `std::any::type_name` is explicitly unstable across compiler versions and may appear only as an informational debug field, never in the fingerprint and never as an identity. Recorded names therefore match the *declared* names of the types the compiler enforced.

Carries a header: schema version identifier, tool version, generation time, pipeline identity, build provenance — tool version, git commit, and lockfile hash, all embedded at build time — and the graph fingerprint. Generation time is excluded from byte-identity comparisons; everything else in the artifact is fixed per binary.

Emitting it must require no credentials, no network, and no database. That constraint is what lets it run in continuous integration on every pull request.

**Acceptance criteria.**
- The artifact can be produced in an empty environment with no configuration present.
- Emitting twice from the same binary produces identical bytes outside the generation-time field.
- Every node and edge that the running pipeline would use appears in the artifact, including declared resource requirements.
- Recorded type and task names are the author-declared stable names, and duplicate declared names fail assembly.
- The artifact validates against its published schema.

### C21 · Graph fingerprint

**Purpose.** A stable identity for a pipeline's shape.

**Behavior.** Two hashes, not one. The **structural fingerprint** covers the node set (stable names), the edge set (with carried type names and edge kinds), and trigger rules — the things that determine whether a prior run's outputs mean anything to this binary. The **policy hash** covers the remaining policy values (retries, timeouts, costs, classes). Resume (C27) is gated on the structural fingerprint only; a policy divergence prints a per-node diff and proceeds, because "I raised the timeout and want to resume the expensive half-finished run" is the *motivating case* for resume, not a reason to refuse it. Group labels are in neither hash (C6). Defaulted policy values hash identically to written-out defaults (C5).

Both hashes deliberately exclude timestamps, hostnames, compiler versions, and anything else environmental. Two builds of unchanged source — on different machines, with different toolchains — produce the same fingerprint, which is testable because every input is author-declared. The fingerprint algorithm is versioned; an algorithm-version mismatch reads as "cannot compare," which resume reports distinctly from "topology differs."

Changing a task's *internal logic* without changing its interface does **not** change the fingerprint. This is a real limitation with no cheap fix in a compiled language, and it is documented where developers encounter it — on the resume verb and the structure-assertion API — rather than buried here. Where node-level change detection is genuinely needed, a hand-maintained version marker on the task is the honest answer: visible, reviewable, and obviously manual, rather than an automatic hash that silently under-detects.

**Acceptance criteria.**
- Two builds from the same source, including on different toolchains, produce the same structural fingerprint and policy hash.
- Any structural change (node add/remove/rename, rewire, trigger-rule change, carried-type change) changes the structural fingerprint; any policy-only change changes only the policy hash.
- A group rename changes neither hash.
- Both hashes and the algorithm version appear in the graph artifact and in every run artifact.
- The internal-logic limitation is documented at the point of use.

### C22 · Run artifact

**Purpose.** The outcome of one execution, joinable to the structure.

**Behavior.** Derived from the event stream — at the end of a run, or by folding a partial stream after a crash (the fold is a standalone function and a CLI verb, C26; a crashed run's artifact is produced by the *next* invocation, since the dead one can't). The header carries run identity, pipeline identity, both fingerprint hashes, the parameters and data interval it was invoked with, allowlisted captured environment values (the allowlist is declared at pipeline construction and empty by default), resume lineage (immediate parent and lineage root run identities, when resumed), and the overall outcome.

A run that failed before execution still produces an artifact: outcome `assembly-failed` or `bootstrap-failed` (the two are distinct — one is the graph's fault, the other the machine's), fingerprint present only when assembly succeeded, the complete error list, zero attempts. A single-node replay (C26) produces a distinct artifact variant in which nodes outside the request are marked `not-requested` — an artifact marking, not a terminal state. The fingerprint-match, node-coverage, and taxonomy criteria below apply to full runs that passed assembly.

The body carries one record per *attempt* rather than per node — retries are the interesting signal for capacity planning, and collapsing them to a final outcome throws that away. Each record holds node identity, attempt number, terminal status from the normative taxonomy, elapsed time broken into named phases (computed from monotonic offsets, so phases sum to the attempt total exactly), which worker ran it, a message, structured error detail, the metrics the task reported, declared-versus-measured cost, and the durable-output reference if the node produced one (C27). Satisfied-from-prior nodes carry their originating run identity, and resumed artifacts copy durable references forward so every artifact is self-contained.

The summary carries total elapsed time, critical path time, peak measured slot residency (C10), values still retained at run end, and the time and capacity that was pinned by abandoned-but-running work. The first two numbers together answer whether the run was limited by its dependency structure or by its resources — which is the question that determines what machine to run it on; the last says how much of the machine a misbehaving task quietly kept.

**Schema evolution is bounded, not vibes:** schemas are versioned; evolution within a version is additive-only; readers ignore unknown fields and default missing ones; the folding function declares which stream schema versions it reads; and a checked-in fixture corpus with one artifact per released schema version is parsed in CI forever after.

**Acceptance criteria.**
- Every node present in the graph artifact appears at least once in the run artifact, including never-ran nodes with their propagated terminal states.
- A crashed run's stream folds into an artifact, marked interrupted, containing everything up to the crash — produced by a later invocation of the binary.
- An assembly-failed run produces the assembly-failure artifact variant.
- Phase durations for an attempt sum exactly to that attempt's total (both derive from monotonic offsets).
- The artifact names a structural fingerprint matching a graph artifact from the same build.
- The artifact validates against its published schema, and every fixture-corpus artifact from prior schema versions remains parseable by current tooling in CI.
- No environment value outside the declared allowlist appears in any artifact.

### C23 · Node metrics

**Purpose.** Lets each task report what it measured.

**Behavior.** An open, unschematized set of measurements attached to an attempt. The framework contributes what only it can know — allocator-attributed peak memory, permit sizes, phase timings. The task contributes what only it can know — rows read and written, bytes scanned, bytes spilled, groups formed, entities resolved.

Open by design: adding a new measurement must never require a framework release. But open is not lawless: metric values are numeric, with units carried in the name per the documented convention (`rows_read`, `bytes_spilled`); the `dagr.` prefix is reserved for framework metrics, and a task attaching under it fails loudly at attach time; and each attempt's metrics are capped (128 entries, 16 KiB encoded) with deterministic truncation that is itself recorded as a framework metric.

Peak memory is measured by an instrumented allocator attributing allocations to the running attempt via task-local state. Under concurrent nodes in one process this attributes what the attempt allocated, not what the process happened to be using — the honest, per-node number, which is the one that belongs next to a declared cost.

**Acceptance criteria.**
- A task can attach a new measurement with no framework change.
- Framework-contributed measurements are present even when a task attaches none.
- Measurements reach the run artifact unmodified.
- A task metric under the reserved prefix fails at attach time; metrics past the cap are truncated deterministically and the truncation is recorded.
- A documented naming and units convention exists, and every built-in measurement follows it.

### C24 · Renderers

**Purpose.** Makes the pipeline legible to humans.

**Behavior.** Reads a graph artifact and produces diagram source in Graphviz DOT and Mermaid, distinguishing data edges from ordering edges and clustering nodes by group. Optionally overlays a run artifact to colour nodes by terminal state — using the normative taxonomy, with originated and propagated skips distinguishable — and annotate them with duration.

Renderers consume artifacts only, never a live pipeline, so they work equally on a historical run from three months ago.

Readable output with no manual layout is the design goal; the *criteria* are the mechanical proxies for it.

**Acceptance criteria.**
- Both output formats are accepted by their reference tools in CI (`dot` parses; Mermaid's parser accepts).
- Every node and edge in the artifact appears in the output; data and ordering edges carry distinct styling; groups render as clusters — all verified structurally, plus golden-file tests.
- With a run artifact overlaid, every terminal state maps to a documented distinct style, and originated skips are distinguishable from propagated ones.
- Rendering requires no access to the binary that produced the artifacts.

### C25 · Logging integration

**Purpose.** Makes a failed run debuggable from logs alone.

**Behavior.** Every attempt runs inside a span carrying run, node, and attempt identity, so every line emitted beneath it — including from third-party libraries the task calls — is attributable without correlating timestamps. Output is structured by default so it can be queried, with a human-readable mode for local development.

**Acceptance criteria.**
- Any log line produced during an attempt can be traced to its node and attempt without timestamp correlation.
- Switching between structured and human-readable output requires no code change.
- Lines from concurrently executing nodes are unambiguously separable.
- Marked secret values from the resource registry (C9) never appear on framework-emitted output paths, verified with a planted sentinel. A task author formatting a secret into their own log line is outside this guarantee, and the documentation says so.

---

## Layer D — Developer and operator surface

### C26 · Command-line contract

**Purpose.** The shape every pipeline binary shares, so operators learn it once.

**Behavior.** A standard set of verbs supplied by the library rather than reinvented per pipeline: emit the graph; validate and exit; render a diagram; run; run a single node; resume a prior run; fold an event stream into a run artifact (the crashed-run path — system criterion 3's crash clause is tested through this verb); prune old runs from the run store by count or age. Parameters are supplied as typed arguments — the pipeline declares a typed parameter struct, the library derives the parsing — validated at bootstrap, after assembly (which never sees them, C7), and carried in the context thereafter. Library-owned flags live in a reserved namespace so pipeline parameters can never collide with them.

**Run a single node** means: replay node N from run R. Inputs are rehydrated from the durable references recorded in R's artifact (C27); a node whose inputs were not durable cannot be replayed, and the error says which input and why. A node that consumes nothing can run standalone without a prior run. The resulting artifact marks unselected nodes `not-requested`.

**Exit codes are by cause, with precedence.** *Run failure* means a non-teardown node ended `failed` or `timed-out` — those two states and no others; `abandoned` arises only on the cancellation path (C14, C16) and attributes to cancellation. Run failure wins whenever it occurred, even when that failure then triggered cancellation; cancellation is reported only for externally originated termination with no run failure. Distinct codes exist for: success (including skip-only runs), run failure, assembly failure, bootstrap failure, cancellation, resume refusal (also used by a single-node replay refused for a non-durable input), sink failure, and invalid usage. The exact numbering is documented in one table and never changes within a major version.

**Acceptance criteria.**
- Every verb behaves identically across all pipelines built with the library.
- Validate exits non-zero on any assembly failure and prints every problem found.
- Run exits with the run-failure code if any non-teardown node ended `failed` or `timed-out` — including under stop-on-first-failure, where the self-inflicted cancellation does not mask the failure.
- The exit-code table is exhaustive over verbs and causes, and each code has a test.
- Invalid parameters are rejected at bootstrap, before any node executes.
- A pipeline parameter cannot shadow or collide with a library flag.
- Running the binary with no arguments prints the available verbs and exits cleanly.
- Folding a crashed run's stream with the fold verb produces the interrupted artifact.

### C27 · Resume

**Purpose.** Avoid repeating expensive work after an interruption.

**Behavior.** Given a prior run's directory in the run store, first verify: the structural fingerprint matches this binary (a mismatch refuses and prints the structural diff; a policy-hash divergence prints a per-node diff and proceeds); the fingerprint algorithm versions are comparable (a mismatch refuses as "cannot compare"); the tool version matches (v1 refuses across tool versions, with a clear message). Parameters and the data interval are *derived from the prior artifact* — supplying conflicting values refuses with a diff, and overriding requires a force flag that is recorded in the resumed artifact. Resume requires the original run to have used a run store; a run whose store is gone is not resumable, and the refusal says so.

**The durable-output contract** is what makes any of this possible. Durability is declared per node in policy (C5); a durable node's output type implements the reference contract — serialize a reference to where the value durably lives, and rehydrate the typed value from that reference later. Assembly rejects a durable-marked node whose output type lacks the contract. On success, the reference is recorded in the attempt record (C22). At resume, references of candidate nodes get a cheap existence check before anything is skipped — a reference to a deleted object fails the resume plan, not the eleventh node of the resumed run.

**The algorithm is demand-driven, in three steps.** First, the *must-run seed*: every node whose prior terminal state was not `succeeded`, plus every node covered by a teardown that executed in the prior run (C17). Second, close downward: everything reachable from the seed re-runs, its trigger rule re-evaluated against this run's states. Third, resolve demand upward: a re-running node demands the values of its data inputs; a demanded producer that is durable with an intact reference has its slot filled by rehydration; a demanded producer that is *not* durable joins the must-run set, and its own demands cascade upward the same way.

Every prior success left outside the must-run set is marked `satisfied-from-prior` — durable or not, because an undemanded value never needs rehydrating and the node's *effect* stands. This is what makes the cleanup-after-publish shape resume correctly: publish (succeeded, ordering-only, nothing demands its value) is satisfied, cleanup re-runs, and cleanup's trigger rule sees a success-like upstream (Vocabulary). Resuming a fully successful run has an empty seed and is a no-op.

Nodes whose outputs were in-memory values cannot be *rehydrated*: the moment a re-running consumer demands their value, they re-execute, and their demands cascade upward. This is a genuine property of the design, and it is stated plainly to developers rather than worked around: it creates useful pressure to make expensive stage boundaries produce durable, addressable outputs (C10's authoring guidance).

**Acceptance criteria.**
- Resuming against a mismatched structural fingerprint refuses and prints the structural difference; a policy-only change proceeds and prints the per-node policy diff.
- A satisfied node is not re-executed, and a re-executing consumer that demands its value receives the rehydrated value.
- A node that succeeded with an in-memory output is re-executed when and only when a re-executing consumer demands its value.
- A prior success whose value nothing demands is satisfied-from-prior even when not durable, verified with the cleanup-after-publish shape: ordering upstream succeeded, downstream re-runs, rule fires.
- A dangling durable reference fails the resume plan before execution begins.
- Resuming a fully successful run is a no-op that exits successfully.
- Supplying parameters that conflict with the prior run refuses with a diff; the force flag overrides and is recorded.
- A resumed run produces its own artifact, linked to both its immediate parent and its lineage root.

### C28 · Testing surface

**Purpose.** Makes pipelines testable without infrastructure.

**Behavior.** Three levels, shipped with the library rather than rebuilt in each pipeline. A single task can be invoked with a hand-built context and fake resources — no live network, no database; synchronous tasks need no async runtime at all, and await-bound tasks use a plain test runtime the surface provides. A pipeline's structure can be asserted against a checked-in fixture: the assertion is a *semantic* comparison over node set, edge set, and effective policies — volatile header fields excluded — whose failure output is a structural diff, and whose fixture is regenerated through a blessed, deliberate flow (an update flag that rewrites the canonical, stably-ordered fixture for review), so unintended rewiring fails review rather than production. A whole pipeline can be executed end to end against fakes and a tiny fixture, exercising the real scheduler.

The framework tests itself the same way it asks pipelines to be tested: compile-fail and error-message tests are library-internal, pinned to the workspace toolchain, asserting only that both type names appear in the message; toolchain bumps regenerate those fixtures deliberately. The framework's own I/O is covered by a fault-injection suite — kill-points around every event write, disk-full, and slow or failing sinks.

**Acceptance criteria.**
- A single-task test of a synchronous task requires no async runtime, no network, and no database; an await-bound task test needs only the provided test runtime.
- A structure test fails when a node is added, removed, renamed, rewired, regrouped, or has its effective policy changed — a group rename is review-visible here (C6) even though it never touches the fingerprint — and does not fail on a rebuild or a toolchain bump; its output is a structural diff.
- The fixture-update flow is a single documented command, and the fixture serialization is canonical and stably ordered.
- A full-pipeline test against fakes completes in seconds.
- No pipeline needs to write its own test harness.
- The framework's fault-injection suite covers kill-points, disk-full, and failing sinks.

---

## Operational model

Something outside this tool decides *when* a pipeline runs. That is not a gap; it is the design. A dagr binary is triggered by cron, a Kubernetes Job, a CI step, systemd, or a human — bring your own trigger. What the binary owes its invoker is exactly: truthful exit codes (C26), prompt and honest reaction to termination signals within a stated shutdown budget (C16), and artifacts at a predictable location in the run store (C19).

The supported concurrency model is **one run per container**. Concurrent runs on a shared host are safe with respect to the run store — per-run directories never collide — but each process sizes its admission pools from the same machine limits, so two memory-hungry runs sharing a host can jointly exceed it. Splitting capacity across simultaneous runs is the operator's call, made with the pool-pinning flag (C12); the tool does not coordinate between processes, because that road ends in building a scheduler.

The default shutdown budget assumes a Kubernetes-style 30-second kill window (C16). Different orchestrator, different flags.

---

## Stability

For a tool whose entire pitch is compile-time confidence, breaking the authoring API is expensive and breaking recorded artifacts is worse. Commitments:

- **Authoring API:** semantic versioning. Within a major version, pipelines keep compiling.
- **MSRV:** pinned in the workspace, documented in the README; raising it is a minor version bump, called out in release notes.
- **Async runtime:** tokio is a supported public dependency (C13); replacing or major-bumping it is a major version event.
- **Schemas:** the event stream, graph artifact, and run artifact each carry a schema version. Evolution within a version is additive-only; readers ignore unknown fields and default missing ones. A fixture corpus with one artifact per released schema version is parsed in CI forever after (C22).
- **Fingerprint:** the algorithm is versioned (C21); cross-toolchain stability is guaranteed and tested; algorithm changes read as "cannot compare," never as false difference.
- **Scratch and stream formats across tool versions:** v1 makes no cross-version resume promise (C27 refuses) and says so; the refusal message is the documentation.
- **Supply chain:** `cargo audit` and `cargo deny` (licenses, sources, advisories) run in CI; build provenance (tool version, git commit, lockfile hash) is embedded in every binary and every artifact (C20); the core crate holds a minimal dependency set, and additions to it are reviewed as API decisions.

---

## Platform support

- **Tier 1 — Linux containers.** Everything works; cgroup v2 and v1 limit detection (C12); the full test suite runs in CI here, including the fault-injection and signal tests.
- **Dev-supported — macOS.** Everything compiles and runs; pool sizing falls back to host resources; documented divergences only (no cgroups, different fsync semantics). A CI job runs the core suite.
- **Windows — explicitly unsupported in v1.** Signal semantics and the process model differ enough that pretending otherwise would mean untested promises. Revisit on demand.

Platform-conditional acceptance criteria (limit detection, signal handling, flush behavior) are named as such in the coverage matrix.

---

## Performance envelope

Designed for graphs of **ten to one thousand nodes**. Below ten, plain code is genuinely simpler (see "When not to use this"); far above a thousand, artifact sizes and diagram legibility degrade before the runtime does. Framework overhead per node — scheduling, admission, event writing; everything but the task's own work — is budgeted at **under one millisecond**, held by a CI benchmark that runs a thousand-node no-op graph and fails on regression. Artifact and stream sizes stay proportional to attempt count; the fixture corpus includes a ten-thousand-attempt artifact to keep the tooling honest at scale.

---

## Documentation

Deliverables, held to in CI where a machine can check them:

- **README quickstart:** empty directory to a compiled, run, artifact-inspected two-node pipeline. The quickstart's code blocks compile and run verbatim in CI (system criterion 1).
- **Rustdoc on every public item**, enforced by lint in CI.
- **Runnable examples** covering each layer.
- **A cookbook of the patterns the design forces:** fan-out inside one node (with the declared-cost rule for internal parallelism); fan-in; branch-in-task with self-skip versus succeed-with-empty for joins; incremental cursors via the scratch store; durable stage boundaries and what they buy at resume time; the non-`Send` capture error and its fixes; two same-typed resources via newtypes.

---

## When not to use this

A three-node script that runs one thing after another does not need a framework; write the three calls, and reach for this when any of the following shows up: work that must overlap under a memory ceiling, retries whose interaction with ordering you have gotten wrong once already, a run you needed to explain after the fact and couldn't, or a half-day pipeline that died at hour three and had to start over. Those are the adoption triggers. Below them, plain tokio is the honest recommendation, and the README says exactly that.

---

## Build order

Each milestone is shippable and independently demonstrable. Nothing from a later milestone is required to make an earlier one useful.

**M1 — It runs.** C1, C2, C3, C7, C8, C10, C11, C14, C19.
*Done when:* a three-node chain executes in order, one node fails and retries successfully, and the event stream shows every transition. Nothing else exists yet — no artifacts, no admission control, no CLI.

**M2 — It survives.** C5, C9, C12, C13, C15, C16.
*Done when:* a pipeline whose combined *declared* demand exceeds the configured memory capacity completes without exceeding it, and an induced mid-run failure stops the run cleanly with nothing orphaned.

**M3 — It explains itself.** C20, C21, C22, C23, C24, C25.
*Done when:* a run produces both artifacts, the rendered diagram is reviewable, and "which node was slowest, and was it waiting or working?" is answerable from artifacts without reading a single log line.

**M4 — It is operable.** C4, C6, C17, C18, C26, C27, C28.
*Done when:* a pipeline killed mid-run resumes and skips completed durable work, and a structural change to the graph is caught in code review.

---

## System-level acceptance

The criteria that make this a product rather than a collection of parts. All must hold simultaneously. Each is classed **[machine]** — enforced by an automated test in CI — or **[human]** — verified against the release checklist, which is version-controlled and reviewed like code.

1. **[machine]** The README quickstart compiles and runs verbatim in CI, taking a developer comfortable with Rust and cargo (no async experience required) from empty directory to a compiled, run, artifact-inspected two-node pipeline. **[human]** The walkthrough stays completable in under thirty minutes — a design goal audited each release, not a timer in CI.
2. **[machine]** Mis-wiring two tasks is a compile error whose message contains both type names involved.
3. **[machine]** Every run produces artifacts — including runs that crashed (via the fold verb), were cancelled, or failed during assembly or bootstrap.
4. Determinism, in three honest parts: **[machine]** (a) *structural* — two builds of the same source produce identical fingerprints and byte-identical graph artifacts (generation time aside), on different toolchains; **[machine]** (b) *interpretive* — the same recorded outcomes yield the same terminal states, propagation decisions, and artifact, replayed through the C28 harness with scripted task results; (c) what tasks *do* against external systems is theirs, and the tool claims nothing about it — a disclaimer, carried unclassified in the criteria matrix. "Same input" means same parameters and data interval.
5. **[machine]** A run's duration and resource profile can be answered entirely from artifacts, with no access to the machine that produced them.
6. **[machine]** Adding a node to an existing pipeline requires no changes outside: the new task's own module, the assembly site, the structure fixture, and — when it introduces a new resource — the registry construction in `main`. Verified via the structure-diff on a reference pipeline.
7. **[machine]** Nothing in the tool requires a server, database, or scheduler to be running — the binary and its arguments are sufficient to run and to produce local artifacts. Crash-surviving artifacts and resume additionally require the one thing the operational model asks for: a run-store location on storage that outlives the container.
8. **[machine]** Every machine-classed acceptance criterion above and in C1–C28 is covered by an automated test, and that coverage is itself verified in continuous integration from a checked-in criteria matrix. **[human]** The human-classed criteria — diagram readability (C24), documentation-at-point-of-use (C21), the thirty-minute walkthrough, and judgment-shaped criteria such as C1's types-readable-from-the-declaration among them — are on the release checklist. The matrix is the source of truth for every criterion's classification: each criterion in this document appears there exactly once, as machine, human, or disclaimer, and a criterion absent from the matrix fails CI.

---

## Amendment changelog

This revision incorporates the findings of an adversarial design review (2026-07-22; 60 confirmed findings). Two kinds of change:

**Fixes** — internal contradictions with a single honest resolution:
- C10 release now fires on consumer *terminal state*, not last read (retry could not re-form its input).
- C12/C14 permit accounting now counts abandoned-but-running work; per-class timeout semantics defined (blocking work cannot be killed in Rust); retry deferred past zombie return (C1 exclusivity).
- Trigger rules enumerated as a closed set with per-rule fire/can-never-fire semantics; C15 propagation scoped to rule-unsatisfiable, which un-deadens `all-terminal` cleanup nodes.
- C4 self-contradiction resolved (ordering edges attach to any node; non-default rules restricted to consume-nothing nodes); ordering edges are registration-time backward references, preserving C2's compile-time cycle claim.
- Teardown carved out of C15/C16's admit-nothing-after-failure rules.
- Impure **bootstrap** phase named; capacity, resource, and parameter checks moved there from pure assembly; run store contract defined, and criterion 7 reworded honestly.
- C9's pre-run check made implementable via declared resource requirements; newtype disambiguation.
- C27's durable-output contract defined (declare, reference, rehydrate, existence-check); resume made demand-driven, resolving its self-contradiction; parameters derived from the prior artifact.
- Terminal-state taxonomy made normative; originated and propagated skips distinguished; `abandoned` and `satisfied-from-prior` added.
- Criterion 4 split into structural/interpretive/disclaimed determinism; criterion 8 partitioned machine/human; C24's subjective criteria replaced with mechanical proxies.
- Stable author-declared names replace `std::any::type_name` in artifacts and fingerprint (unstable across toolchains).
- Fingerprint split into structural fingerprint (gates resume) and policy hash (diff-and-proceed); groups excluded from both.
- Renamed flowline → **dagr** throughout.

**Decisions** — defensible choices that could have gone another way; each is validated by an M0 spike or ADR before it hardens (see docs/tasks.md):
- Input ownership: sole-consumer-owns / multi-consumer-shared-read / per-edge clone-on-read opt-in; `&mut self` task exclusivity; `Send + Sync + 'static` outputs.
- Memory cost split into working memory and output residency; slot leases charged to the memory pool.
- Trigger-rule set: `all-succeeded`, `all-terminal`, `any-failed` (closed for v1).
- Admission: oldest-ready-first with bounded bypass; all-or-nothing multi-pool acquisition; cgroup v2→v1→host; 20% headroom; pinning flag.
- Timeout retry-eligible by default; `panic=abort` refused at startup.
- Shutdown budget: 10 s grace + 15 s teardown + 2 s flush against an assumed 30 s kill window.
- Run store layout `<base>/<pipeline>/<run-id>/`; UUIDv7 run ids; write-through (no user-space buffering), fsync at end/cancel; sink failure cancels the run; prune verb; retention otherwise operator-owned.
- Scratch: opaque bytes; copied forward on resume for re-executing nodes; deleted on node success; I/O failure is retry-eligible.
- tokio named as a public dependency; renderers are DOT + Mermaid; metrics caps 128 entries / 16 KiB.
- Policy-hash divergence proceeds with a printed diff (resume refuses only on structural mismatch); v1 refuses cross-tool-version resume.
- Single-node verb defined as replay-from-run with rehydrated durable inputs.
- Groups do not nest; Windows unsupported in v1; performance envelope 10–1,000 nodes at <1 ms/node overhead.

**Second pass** — an adversarial re-review of the amended spec caught interactions between the new mechanisms, fixed in place:
- Trigger rules made total over the taxonomy via state classes; `satisfied-from-prior` counts as success-like (resume would otherwise have broken every downstream rule).
- Owned-input delivery constrained: multi-consumer or retrying edges need shared access or clone-on-read (an owned value moved into a failed attempt cannot be re-formed); type-vs-mode checking split between compile time and assembly.
- `abandoned` restricted to the cancellation path — a timeout never yields a second terminal state; leftover threads are zombie-at-exit *events*; run end waits only the bounded grace for zombies.
- Slot release and residency accounting wait for zombie consumers' closures to return, so the memory ledger never frees bytes a leftover thread still pins.
- Stop-on-first-failure admits consume-nothing contingency nodes whose non-default rules fire on the final picture (notify-on-failure works under stop mode).
- Run verbs mint identity and open the run store before assembly, so assembly failures have a place to land; `assembly-failed` and `bootstrap-failed` are distinct outcomes; exit-code "run failure" is exactly `failed`/`timed-out` on a non-teardown node.
- Resume's `satisfied-from-prior` extended to undemanded prior successes (the cleanup-after-publish shape resumes correctly); the seed/closure/demand algorithm stated explicitly.
- Group renames fail the structure test (review-visible) but never the fingerprint; teardown declared cost pinned to zero, keeping its admission bypass consistent with C12; duplicate same-type registry registration rejected; scratch retention freed of the undefined "resume enabled" mode.
