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
