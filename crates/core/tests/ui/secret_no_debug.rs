// UI compile-failure fixture — ticket T30 (040), case `secret_no_debug`.
//
// PROVES (C9; arch.md §227): a value marked secret in the resource registry is
// wrapped in `dagr_core::context::Secret`, which has NO `Debug` path. Formatting
// a `Secret` with the debug formatter (`{:?}`) fails to COMPILE, so the framework
// cannot accidentally emit secret material through a `Debug` derive — the
// wrapper's redaction guarantee is enforced by the type system, not by a runtime
// scrub. (End-to-end framework log-line redaction is C25/T45; this is the wrapper
// that makes it possible.)
//
// This is the REAL registry API (`dagr_core::context::Secret`), linked against
// the built rlib by the T8 harness (crates/core/tests/ui.rs). The sibling
// `.stderr` names the substrings the diagnostic must contain; the harness asserts
// only that this sample FAILS to compile under the pinned toolchain (C28),
// asserting the type name and the missing-trait facet appear — never prose.

use dagr_core::context::Secret;

fn main() {
    let secret = Secret::new(String::from("do-not-print"));
    // `Secret<String>` implements no `Debug` — the whole point of the marker —
    // so formatting it with the debug formatter is an E0277
    // `Secret<String>: Debug` is not satisfied. It cannot compile.
    println!("{secret:?}");
}
