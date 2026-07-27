//! **The `#[task]` accept/reject boundary, pinned as a `trybuild` corpus.**
//!
//! The `#[task]` macro is the primary task-authoring style, and proc-macro
//! diagnostics "can point at the `#[task]` site rather than the offending
//! line." This suite turns that known limitation into a **tested, versioned
//! contract**: every arity and execution-class variant the macro accepts compiles
//! here, and every documented misuse fails with a committed, byte-stable `.stderr`
//! snapshot. The cookbook "common mistakes" section cross-references this corpus
//! as the canonical example set.
//!
//! # Why `trybuild` here, and a hand-built UI harness in `dagr-core`
//!
//! The UI harness (`crates/core/tests/ui.rs`) is deliberately **not**
//! `trybuild`: its wrong-type diagnostics must assert only that both type-name
//! substrings appear and tolerate prose churn, which `trybuild`'s exact,
//! line-by-line `.stderr` match cannot express. This corpus is the opposite case:
//! it *wants* the exact, whole-diagnostic snapshot, because the point is to pin
//! the macro's `compile_error!` wording and the natural borrow/`Send` diagnostic
//! **verbatim** so a regression in either is a red build. So `trybuild` is the
//! right tool here and the wrong tool there — the two harnesses are chosen for
//! their opposite matching semantics, not by accident.
//!
//! The licence concern that steered the core harness away from `trybuild` (its
//! tree pulls `unicode-ident`, `(MIT OR Apache-2.0) AND Unicode-3.0`) does **not**
//! apply: `unicode-ident` is already in the workspace lockfile (a transitive
//! build-time dep of `serde_derive`) and `Unicode-3.0` is already allowed in
//! `deny.toml`, so this corpus adds no new licence surface.
//!
//! # Toolchain pinning (the same contract as the core UI harness)
//!
//! `trybuild` matches `.stderr` **exactly**, so a snapshot is only reproducible
//! under a fixed toolchain. dagr pins one in `rust-toolchain.toml`,
//! and CI runs this corpus under it (`cargo test --workspace`, `crates/macros`).
//! When the pinned toolchain changes, regenerate the snapshots **deliberately**
//! with `TRYBUILD=overwrite cargo test -p dagr-macros --test trybuild` and review
//! the diff — never a silent overwrite.
//!
//! # Two directories, one boundary
//!
//! - `tests/expand/pass/` — every accepted shape: zero/one/two/eight-input
//!   arities, each execution class (`#[task]`, `#[task(blocking)]`,
//!   `#[task(compute)]`), and both with and without a `ctx: &RunContext`
//!   parameter. A red here means the macro rejected something it must accept.
//! - `tests/expand/fail/` — every documented misuse, each with a committed
//!   sibling `.stderr`: over-8 inputs, a non-`Send` capture held across the body,
//!   a deps mismatch at the `RunnableFlow` registration seam, and a bare
//!   non-`Result` return. A red here means the diagnostic wording drifted.

/// Run the whole compile-pass / compile-fail corpus. `trybuild` compiles each
/// `pass/*.rs` and asserts it builds, and each `fail/*.rs` and asserts it fails
/// with output byte-identical to the sibling `.stderr`.
#[test]
fn task_macro_accept_reject_boundary() {
    let t = trybuild::TestCases::new();
    t.pass("tests/expand/pass/*.rs");
    t.compile_fail("tests/expand/fail/*.rs");
}
