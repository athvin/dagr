// UI compile-failure fixture — ticket T30 (040), case
// `registry_non_send_resource`.
//
// PROVES (C9; arch.md §223): stored resources are `Send + Sync + 'static`. A
// client that is not thread-safe cannot be registered directly — the escape
// hatch is the documented owning-worker channel pattern (one thread owns it,
// others reach it through a channel), NOT relaxing the bound. Registering a
// `!Send`/`!Sync` value (here one holding an `Rc`) fails to COMPILE.
//
// This is the REAL registry API (`dagr_core::context::ResourceRegistry`), linked
// against the built rlib by the T8 harness (crates/core/tests/ui.rs). The
// sibling `.stderr` names the substrings the diagnostic must contain; the harness
// asserts only that this sample FAILS to compile under the pinned toolchain
// (C28).

use std::rc::Rc;

use dagr_core::context::ResourceRegistry;

/// A resource that is NOT thread-safe: it holds an `Rc`, so it is `!Send` and
/// `!Sync`. It therefore cannot be stored in the registry (which requires
/// `Send + Sync + 'static`). The fix is the owning-worker pattern, documented in
/// the registry rustdoc — not registering the non-thread-safe client itself.
struct NonThreadSafeClient {
    shared: Rc<u32>,
}

fn main() {
    let client = NonThreadSafeClient { shared: Rc::new(7) };
    // `register` demands `R: Send + Sync + 'static`; `NonThreadSafeClient` holds
    // an `Rc<u32>` and is `!Send`/`!Sync`, so this is an E0277 unsatisfied bound
    // naming `NonThreadSafeClient` (and the `Send`/`Sync` marker). It cannot
    // compile.
    let _ = ResourceRegistry::builder().register(client);
}
