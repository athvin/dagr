// UI compile-failure fixture — ticket T30 (040), case `secret_no_display`.
//
// PROVES (C9; arch.md §227): the secret marker `dagr_core::context::Secret` has
// NO `Display` path either. Formatting a `Secret` with the display formatter
// (`{}`) fails to COMPILE, so no framework code can render secret material into a
// human string through `Display`. Together with `secret_no_debug`, this pins the
// "no Debug/Display path" guarantee at compile time.
//
// This is the REAL registry API (`dagr_core::context::Secret`), linked against
// the built rlib by the T8 harness (crates/core/tests/ui.rs). The sibling
// `.stderr` names the substrings the diagnostic must contain; the harness asserts
// only that this sample FAILS to compile under the pinned toolchain (C28).

use dagr_core::context::Secret;

fn main() {
    let secret = Secret::new(String::from("do-not-print"));
    // `Secret<String>` implements no `Display`, so formatting it with `{}` is an
    // E0277 `Secret<String>: Display` is not satisfied. It cannot compile.
    println!("{secret}");
}
