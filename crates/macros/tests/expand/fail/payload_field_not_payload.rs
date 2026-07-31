//! Compile-fail: a `#[derive(Payload)]` struct with a field whose type is **not** a
//! payload. The generated code carries the field's own span, so the unsatisfied
//! bound points at the offending field — and the trait's
//! `#[diagnostic::on_unimplemented]` note says what to do about it — rather than a
//! wall of trait-bound text pointing at the derive.

use dagr_core::{Payload, StableName};

/// Not a payload: no `#[derive(Payload)]`, so it has no codec.
struct NotAPayload {
    inner: u32,
}

#[derive(StableName, Payload)]
struct HasABadField {
    count: u64,
    opaque: NotAPayload,
}

fn main() {
    let _ = HasABadField {
        count: 1,
        opaque: NotAPayload { inner: 2 },
    };
}
