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

/// Compile every `tests/ui/flow_builder/pass/*.rs` and assert it builds; compile
/// every `tests/ui/flow_builder/fail/*.rs` and assert it fails with output
/// byte-identical to the sibling `.stderr`.
#[test]
fn flow_builder_facade_compile_time_boundary() {
    let t = trybuild::TestCases::new();
    t.pass("tests/ui/flow_builder/pass/*.rs");
    t.compile_fail("tests/ui/flow_builder/fail/*.rs");
}
