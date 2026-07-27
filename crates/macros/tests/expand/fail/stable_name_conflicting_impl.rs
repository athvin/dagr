//! Compile-fail: `#[derive(StableName)]` **and** a hand-written `impl StableName`
//! for the same type. This is the ONE collision the design admits, and it is the
//! ordinary `E0119` "conflicting implementations" error — it happens only when an
//! author explicitly does both, never from `#[task]` (which emits no `StableName`).
//! Pinning it documents that the derive is coherence-safe: one origin of the impl.

use dagr_core::StableName;

#[derive(StableName)]
struct Dup;

// A second implementation of the same trait for the same type.
impl StableName for Dup {
    const STABLE_NAME: &'static str = "Dup";
}

fn main() {
    let _ = Dup;
}
