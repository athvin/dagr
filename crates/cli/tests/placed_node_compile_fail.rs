//! T108 · **remote eligibility is enforced at registration, not at run time.**
//! Written first (TDD).
//!
//! ADR 115 §8: *"a node is remote-eligible iff its input and output types implement
//! `Payload`. A non-serializable payload on a remote node is a compile error, in
//! keeping with mis-wiring already being a compile error."* The placement-taking
//! registrars carry the `Payload` bound, so a placed node whose payload types
//! cannot cross a process boundary reds the build with a message naming the type
//! and the missing bound — rather than assembling, running, and discovering at
//! submission time that there is nothing to send.
//!
//! Byte-exactness makes a `.stderr` snapshot a function of the crate's feature
//! resolution, not only of the sample (see `flow_builder_compile_fail.rs` for the
//! full reasoning and the `StableName` truncation cliff it documents). This harness
//! is pinned to the default resolution for the same reason, and is compiled out
//! under `blob` — which `cargo test --workspace --all-features` selects.
//!
//! Regenerate deliberately with
//! `TRYBUILD=overwrite cargo test -p dagr-cli --test placed_node_compile_fail`
//! and review the diff.

#![cfg(not(feature = "blob"))]

/// Compile every `tests/ui/placed_node/pass/*.rs` and assert it builds; compile
/// every `tests/ui/placed_node/fail/*.rs` and assert it fails with output
/// byte-identical to the sibling `.stderr`.
#[test]
fn a_placed_node_must_carry_payload_types() {
    let t = trybuild::TestCases::new();
    t.pass("tests/ui/placed_node/pass/*.rs");
    t.compile_fail("tests/ui/placed_node/fail/*.rs");
}
