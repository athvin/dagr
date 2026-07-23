// UI compile-failure sample — ticket T9 (019), C1 output-type bound (missing
// `Sync`).
//
// Output values must be `Send + Sync + 'static` so concurrent consumers can
// read a shared slot value (arch.md C1; T0.2 ADR). This sample declares an
// output type that is `Send + 'static` but NOT `Sync` and shows it is rejected.
//
// The T8 harness compiles this STANDALONE with no `--extern dagr_core`, so the
// real `Task` trait is unavailable; the exact output bound (`Send + Sync +
// 'static`) is reproduced with a local `assert_output_bound`. `Cell<u32>` is
// `Send + 'static` but `!Sync`, so it violates the bound.
//
// The diagnostic names two distinct types the snapshot keys on: `Cell` (the
// non-`Sync` output value's type) and `NonSyncOutput` (the offending output
// type wrapping it).

use std::cell::Cell;

/// An output type that is `Send + 'static` but NOT `Sync` (because `Cell` is
/// `!Sync`). Outputs live in a shared slot read concurrently, so `Sync` is
/// required — this type cannot be an output.
struct NonSyncOutput {
    value: Cell<u32>,
}

/// Stand-in for the framework's requirement that a task's OUTPUT be
/// `Send + Sync + 'static`. Mirrored here locally.
fn assert_output_bound<T: Send + Sync + 'static>() {}

fn main() {
    // The output bound demands `Sync`; `NonSyncOutput` holds a `Cell<u32>` and
    // is therefore `!Sync`, so this fails to compile with an E0277 naming both
    // `Cell` and `NonSyncOutput`.
    assert_output_bound::<NonSyncOutput>();
}
