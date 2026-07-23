// UI compile-failure fixture — ticket T5 (018), case `typed_handle_unforgeable`.
//
// PROVES (C2): a handle is obtainable ONLY by registering a node — there is NO
// escape hatch to FABRICATE one. The handle's own fields are private to the
// module that defines it and there is no public constructor, so a struct-literal
// from outside the module (the only other way to make one) fails to compile.
// This is the "No API exists to obtain a handle for a node that has not been
// registered" acceptance criterion, and it is why there can be no lookup by
// name/index/key: the ONLY currency is a handle a `register` call already
// returned.
//
// Wired to the T8 UI harness (crates/core/tests/ui.rs); the sibling `.stderr`
// names the substrings the diagnostic must contain, and the harness asserts
// this sample FAILS to compile under the pinned toolchain (C28).
//
// THROWAWAY, intentionally NON-COMPILING SKETCH — NOT dagr's real authoring API
// (typed handles land in T10). It models only the settled T5 decision that a
// handle is unforgeable by construction (private fields, no constructor).

#![allow(dead_code)]

mod flow {
    use std::marker::PhantomData;

    #[derive(Clone, Copy)]
    pub struct NodeId(pub u32);

    // Handle carries identity + value type; BOTH fields (`id`, `_t`) are PRIVATE
    // and there is deliberately NO public constructor. A handle can only be
    // produced INSIDE this module — i.e. by `register` (elided) — mirroring C2.
    pub struct Handle<T> {
        id: NodeId,
        _t: PhantomData<fn() -> T>,
    }
    impl<T> Clone for Handle<T> {
        fn clone(&self) -> Self {
            *self
        }
    }
    impl<T> Copy for Handle<T> {}

    // There is deliberately NO `Flow::get(name)`, `get(index)`, or `get(key)`:
    // the ONLY way to a handle is a `register` call's return value.
    pub struct Flow;
}

use flow::{Handle, NodeId};
use std::marker::PhantomData;

struct Alpha;

fn main() {
    // Fabricate a handle directly: the struct literal names the private fields
    // `id` and `_t` from OUTSIDE the module. There is no public constructor and
    // no lookup API, so a handle cannot be forged — the mis-wiring is a compile
    // error, not a runtime check.
    let _forged: Handle<Alpha> = Handle {
        id: NodeId(0),
        _t: PhantomData,
    };
}
