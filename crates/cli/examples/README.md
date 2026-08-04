# dagr examples

Not every file here is a tutorial. Some are compiled corpora that integration
tests and CI drive by name. This index says which is which so you copy the right
one.

## Start here (the golden path)

- **`quickstart.rs`** — the whole authoring story in ~25 lines: two `#[task]`
  tasks, a `#[derive(StableName)]` payload, one `#[dag]` fn, a one-line
  `dagr_cli::run` `main`. Byte-identical to the README quickstart. Run it:
  `cargo run --example quickstart -- run --store ./runs`.
- **`many_dags.rs`** — many `#[dag]`-declared flows in one binary, selected by
  name (`list` / `graph <name>` / `run <name>`), auto-discovered with no registry
  edit. The copyable pattern for a multi-DAG binary.

- **`placed_pipeline.rs`** — the M10 capability demo: one pipeline, one node with a
  declared size (500m CPU, 512Mi memory), runnable from the **same binary** under
  both executors. Locally, with no cluster and no warning about one:
  `cargo run --example placed_pipeline -- run placed_pipeline --store ./runs
  --dagr.executor=local`. With the placed node on a pod, in a build that compiled
  the default-off `k8s` feature and with the four `DAGR_DEMO_*` deployment facts
  set: `cargo run --features k8s --example placed_pipeline -- run placed_pipeline
  --store ./runs --dagr.executor=k8s --run-id <id>`. Read
  [`docs/cookbook.md`](../../../docs/cookbook.md#placing-a-node-on-remote-compute)
  before the first remote run.

## Documented fallback

- **`multi_flow.rs`** — the same multi-flow shape wired **by hand** into a
  `FlowRegistry` (no `#[dag]`). The explicit escape hatch when you don't want the
  attribute; see [`docs/flow-registry.md`](../../../docs/flow-registry.md).

## CI determinism fixtures — do not change their printed output

These are built under two toolchains in CI and diffed byte-for-byte. Editing the
pipeline shape or the printed bytes breaks the cross-toolchain determinism gate.

- **`fingerprint_fixture.rs`** — a fixed pipeline whose structural + policy
  fingerprints must be byte-identical across toolchains.
- **`reference_pipeline_artifact.rs`** — a fixed pipeline whose emitted graph
  artifact + fingerprints must be byte-identical across toolchains (kept in sync
  with `tests/system_acceptance_gate.rs`).
- **`m9_baseline_capture.rs`** — the M9 gate's behavioural-identity probe: one
  deterministic snapshot of the reference pipeline's graph artifact, both
  fingerprints, a scripted run's terminal states, its folded run artifact, and
  every event-stream record. `tests/m9_acceptance_gate.rs` spawns it and diffs
  its output against `tests/fixtures/m9-baseline/reference.snapshot.txt`, which
  the **same file** produced against the pre-M9 tree — so it is byte-diffed
  across two *engines* rather than two toolchains. Its source digest is recorded
  in that fixture's `PROVENANCE.md` and re-checked on every run: editing this
  file without re-capturing the baseline fails the gate.

## Test discovery corpora — spawned by integration tests

These exist to be run by `cargo run --example <name>` from a test; each is a
separately-linked binary so its `inventory` submissions stay isolated (the
leaf-binary contract). They are not teaching material.

- **`one_dag.rs`** — a single discovered DAG (hand-written `inventory::submit!`);
  drives `tests/dag_auto_discovery.rs`.
- **`dup_dags.rs`** — two DAGs sharing a name, to prove duplicate rejection;
  drives `tests/dag_auto_discovery.rs`.
- **`dag_macro_smoke.rs`** — the `#[dag]` expansion/rename/collision corpus;
  drives `tests/dag_macro.rs`.
