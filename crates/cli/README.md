# dagr-cli

The pipeline binary of [dagr](https://github.com/athvin/dagr): the run-loop
driver and the command-line contract every dagr binary shares. This is the crate
an application depends on to turn a declared graph into a program you can run.

You get the standard verbs — `run`, `graph`, `validate`, `render`, `list`,
`resume`, `fold`, `prune` — with typed parameters, a reserved library-flag
namespace a pipeline parameter can never shadow, and an exit-code table an
orchestrator can branch on. `main` is a one-liner.

## Its place in the workspace

```text
cli ──────► core, artifact, render  (+ metastore, behind a default-off feature)
                                                                   ◄── you are here
render ───► artifact
metastore ► artifact
core ─────► macros  (build-time)
artifact ─► (nothing)
```

`dagr-cli` is the one crate where the live pipeline (`dagr-core`), the records
(`dagr-artifact`), and rendering (`dagr-render`) meet. Nothing depends on it.
Invoking rendering here as the binary's `render` subcommand still consumes
artifacts only, so it does not weaken the renderer-independence guarantee the
crate graph enforces.

Every runtime dependency dagr has lives here, never in `dagr-core`: the async
runtime, the compute pool, the argument parser, the tracing subscriber, and the
DAG registry.

## What it owns

- **`driver`** — the run loop: mint identity, open the event stream, run the
  assembly and bootstrap fail-fast checks, admit under the memory and thread
  pools, drive attempts to terminal states, propagate failures and skips, react
  to a termination signal within a stated shutdown budget, and finalize. Every
  run ends with a truthful exit code and artifacts at a predictable location.
- **`run_flow`** — the one-call `RunnableFlow` seam. A task author runs a flow
  without hand-writing scheduler plumbing.
- **`registry` / `run`** — many DAGs in one binary, hand-registered or
  auto-discovered at link time.
- **`contract`** — the C26 command-line contract and the exit-code table.
- **`structure_snapshot`** — a pipeline's whole structure test in two library
  calls, so adding a node is caught in code review rather than in production.
- **`full_pipeline`** — the fakes harness: a whole flow of fake tasks driven
  through the **real** run loop, deterministically.

## Features

| Feature | Default | What it turns on |
|---|---|---|
| `dag` | on | link-time DAG auto-discovery and the `#[dag]` re-export |
| `test-kit` | on | the full-pipeline fakes harness |
| `metastore` | **off** | the `dagr metastore init` verb and the opt-in libSQL run index |
| `schema-validation` | off | published-schema validation in the artifact round-trip tests |

## Something else decides *when* a pipeline runs

That is the design, not a gap. A dagr binary is triggered by cron, a Kubernetes
Job, a CI step, systemd, or a human. What it owes its invoker is exactly:
truthful exit codes, a prompt and honest reaction to termination signals within a
stated shutdown budget, and artifacts at a predictable location.

## Documentation

The quickstart — empty directory to a compiled, run, artifact-inspected two-node
pipeline — is in the
[repository README](https://github.com/athvin/dagr#quickstart), and its code
blocks compile and run verbatim in CI. The component specification is
[`docs/arch.md`](https://github.com/athvin/dagr/blob/main/docs/arch.md) — C26
(command-line contract), C27 (resume), C28 (testing surface).

Licensed MIT.
