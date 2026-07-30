# dagr-core

The execution core of [dagr](https://github.com/athvin/dagr) — the authoring
surface and run-loop machinery that make one compiled Rust binary *be* a DAG
pipeline (or several, one selected per invocation). You write units of work,
declare how they connect, and the compiler has already checked the graph: nothing
here needs a server, a scheduler, or a database running, and there is no config
file describing the shape and no parsing step.

This crate is the **live-pipeline surface**. It holds the task abstraction, typed
handles, dependency binding, flow assembly, output slots, readiness, admission,
retry/timeout, cancellation, scratch, and resume — the code that *is* a running
pipeline.

## Its place in the workspace

dagr is a six-crate workspace and the dependency direction is load-bearing, not
tidiness:

```text
cli ──────► core, artifact, render   (the pipeline binary; the one place the
                                      live pipeline and rendering meet)
render ───► artifact                 (renderers consume artifacts ONLY)
metastore ► artifact                 (the opt-in run index; no edge onto core)
core ─────► macros  (build-time)     ◄── you are here
artifact ─► (nothing)
```

`dagr-core` has **no runtime dependencies at all**. Its only edge is a
build-time, optional path to `dagr-macros` for the `#[task]` attribute, which a
proc macro means runs inside the compiler and is never linked into the shipped
program; `--no-default-features` drops the edge entirely. Adding to this crate's
dependency set is reviewed as an API decision.

Neither `dagr-render` nor `dagr-metastore` has an edge onto this crate, so
rendering and the run index are *structurally* incapable of reaching a
live-pipeline type — a guarantee held by a missing edge in the crate graph rather
than by convention.

## What you write

A task is a configuration-holding struct with four declared elements — the input
type, the output type, the execution class, and the work — and a task body
contains business logic and nothing else. There is no scheduling, retry, permit,
timeout, or logging code inside it:

```rust
use dagr_core::TaskError;
use dagr_core::task::{RunContext, Task};

struct Double;

impl Task for Double {
    type Input = u64;
    type Output = u64;

    async fn run(&mut self, _ctx: &RunContext, n: u64) -> Result<u64, TaskError> {
        Ok(n * 2)
    }
}
```

Wiring is typed: a node registration hands back a `Handle<T>` carrying the value
type the node will produce, and binding a handle of the wrong type to a task is a
**compile error** naming both types. A `Handle` has no `depends_on`, so an edge
can only point backward and a cycle is unrepresentable — there is no runtime
cycle check because there is nothing to check.

Most authors reach the rest of dagr through
[`dagr-cli`](https://docs.rs/dagr-cli), which supplies the run loop, the
command-line contract, and the one-call `RunnableFlow` seam.

## Features

| Feature | Default | What it turns on |
|---|---|---|
| `macros` | on | the `#[task]` attribute, re-exported as `dagr_core::task` (build-time only) |
| `test-kit` | on | `SingleTaskTest`, the shipped utility for exercising **one** task with no runtime, driver, or event stream |

`--no-default-features` leaves a crate with an empty dependency graph.

## Documentation

The module index on the [crate root](https://docs.rs/dagr-core) is the map of
what lives where. The component specification the whole workspace is built
against is [`docs/arch.md`](https://github.com/athvin/dagr/blob/main/docs/arch.md);
its **Vocabulary** section (nine terminal states, four state classes, three
trigger rules) is normative — every component here means exactly one of those.

Licensed MIT.
