//! Compile-fail: `#[derive(Payload)]` on a type with a lifetime parameter. Decoding
//! produces an **owned** value out of a byte slice the decoder does not keep, so a
//! borrowing type can never be decoded; the derive rejects it with a spanned,
//! actionable `compile_error!` instead of emitting an impl whose `decode` cannot be
//! written.

use dagr_core::{Payload, StableName};

#[derive(StableName, Payload)]
struct Borrowed<'a> {
    label: &'a str,
}

fn main() {
    let _ = Borrowed { label: "x" };
}
