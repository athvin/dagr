//! Compile-fail: `#[derive(Payload)]` on a `union`. A union has no discoverable
//! active field, so there is nothing to encode and nothing to decode into; the
//! derive says so with a spanned `compile_error!` rather than generating something
//! subtly wrong.

use dagr_core::{Payload, StableName};

#[derive(StableName, Payload)]
union Overlapping {
    a: u32,
    b: f32,
}

fn main() {
    let _ = Overlapping { a: 1 };
}
