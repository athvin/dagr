// UI compile-failure fixture — ticket T12 (024), case `wiring_handle_unforgeable`.
//
// PROVES (C2; arch.md `### C2 · Handle`): a handle is obtainable ONLY by
// registering a node — there is NO public constructor to FABRICATE one. This is
// the REAL `dagr_core::handle::Handle`, not the throwaway T5 sketch: both of the
// handle's fields (`id`, `value`) are PRIVATE and its only constructor
// (`Handle::for_registration`) is crate-private, so a struct literal from an
// external crate names private fields and fails to compile (E0451). This is the
// "No API exists to obtain a handle for a node that has not been registered"
// acceptance criterion — the only currency for referring to a node's output is a
// handle a registration already returned.
//
// Wired to the T8 UI harness (crates/core/tests/ui.rs); the sibling `.stderr`
// names the substrings the diagnostic must contain, and the harness asserts this
// sample FAILS to compile under the pinned toolchain (C28).

use dagr_core::handle::{Handle, NodeId};
use std::marker::PhantomData;

struct Alpha;

fn main() {
    // Fabricate a handle directly: the struct literal names the PRIVATE fields
    // `id` and `value` from OUTSIDE the defining crate. There is no public
    // constructor (`for_registration` is crate-private) and no lookup API, so a
    // handle cannot be forged — the mis-wiring is a compile error, not a runtime
    // check. (`NodeId::from_name` is public — it mints an opaque identity token,
    // never a handle — so the forgery attempt reaches the private-field wall.)
    let _forged: Handle<Alpha> = Handle {
        id: NodeId::from_name("a"),
        value: PhantomData,
    };
}
