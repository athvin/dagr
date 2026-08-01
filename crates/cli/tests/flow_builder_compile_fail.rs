//! **The `FlowBuilder` façade's compile-time safety boundary, pinned as a
//! `trybuild` corpus.** Written first (TDD).
//!
//! The façade returns the *real* `Handle<T>` and forwards to the same
//! stable-name-aware registrars `RunnableFlow` exposes, so the compile-time
//! guarantees are inherited verbatim. This suite pins the three the ticket names:
//!
//! - `node_wrong_type.rs` — a `Handle<T>` bound into a `node` whose task declares a
//!   *different* `Input` reds the build (the `Deps<Inputs = T::Input>` bound). The
//!   `.stderr` snapshot shows the expected-vs-actual type names.
//! - `depends_on_wrong_type.rs` — the same mis-wiring through the explicit
//!   `f.task(..).depends_on(..)` builder reds the build too (it carries the identical
//!   `Deps<Inputs = T::Input>` bound), pinning that the ergonomic spelling loses no
//!   compile-time guarantee.
//! - `source_without_stable_name.rs` — a task **without** `StableName` passed to the
//!   graph-emittable `source` fails to compile (the `T: StableName` bound); the same
//!   task through `source_erased` compiles (proven in the pass sample below).
//! - `no_run_or_into_pipeline.rs` — the façade exposes **no** consuming/execution
//!   method: `f.run(..)` / `f.into_pipeline()` do not resolve on a `FlowBuilder`.
//!
//! A companion **pass** sample proves the escape hatch: the same `StableName`-less
//! task compiles through `source_erased` / `node_erased`.
//!
//! `trybuild` matches `.stderr` byte-exactly under the workspace-pinned toolchain
//! (`rust-toolchain.toml`); regenerate deliberately with
//! `TRYBUILD=overwrite cargo test -p dagr-cli --test flow_builder_compile_fail`
//! and review the diff — never a silent overwrite.
//!
//! # Why the harness runs under the default feature resolution only
//!
//! Byte-exactness makes a snapshot a function of *more* than the sample. The
//! `source_without_stable_name` diagnostic reproduces rustc's "the following other
//! types implement trait `StableName`" list, and that list is the set of `StableName`
//! impls reachable from `dagr_cli` — so it is a function of the crate's **feature
//! resolution**, not of the boundary being pinned. rustc prints the set in full up to
//! nine candidates and truncates to eight plus "and N others" beyond that, and the
//! default resolution sits at exactly nine, so *any* optional feature that adds an
//! impl rewrites the snapshot. `blob` is such a feature: with `test-kit` it compiles
//! `dagr_cli::exec_node_demo`, whose four pipeline tasks push the list over the cliff.
//! `trybuild` reads the enabled features out of this test binary's own fingerprint and
//! passes them to the project it generates, so the snapshot cannot be blessed for two
//! resolutions at once and there is no per-case opt-out.
//!
//! The boundary itself is feature-independent — `FlowBuilder`'s bounds are not gated
//! by any feature — so pinning it once, under the resolution the snapshots were
//! blessed against, is the whole of the coverage; running the same samples again under
//! `--all-features` only re-asserted rustc's candidate-list formatting. The harness is
//! therefore compiled out when `blob` is on, which is what `cargo test --workspace
//! --all-features` (the `feature-matrix` CI job) selects. The `test` job, on both
//! platform tiers, runs it under the default resolution.
#![cfg(not(feature = "blob"))]

/// Compile every `tests/ui/flow_builder/pass/*.rs` and assert it builds; compile
/// every `tests/ui/flow_builder/fail/*.rs` and assert it fails with output
/// byte-identical to the sibling `.stderr`.
#[test]
fn flow_builder_facade_compile_time_boundary() {
    let t = trybuild::TestCases::new();
    t.pass("tests/ui/flow_builder/pass/*.rs");
    t.compile_fail("tests/ui/flow_builder/fail/*.rs");
}
