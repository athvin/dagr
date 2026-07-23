// UI compile-failure sample — ticket T9 (019), C1 output-type bound (missing
// `'static`).
//
// Output values must be `Send + Sync + 'static` (arch.md C1; T0.2 ADR). An
// output type that borrows data is not `'static` and cannot outlive the attempt
// to live in the slot, so it is rejected.
//
// The T8 harness compiles this STANDALONE with no `--extern dagr_core`, so the
// real `Task` trait is unavailable; the exact output bound is reproduced with a
// local `assert_output_bound`. `BorrowedOutput<'a>` borrows data and is
// therefore not `'static`.
//
// The diagnostic names two distinct types the snapshot keys on: `BorrowedOutput`
// (the non-`'static` output type) and `assert_output_bound` (the bound function
// whose `'static` requirement it violates), plus the lifetime-escape wording.

/// An output type that borrows data — NOT `'static`, so it cannot live in the
/// output slot beyond the attempt that produced it.
struct BorrowedOutput<'a> {
    value: &'a u32,
}

/// Stand-in for the framework's requirement that a task's OUTPUT be
/// `Send + Sync + 'static`. Mirrored here locally.
fn assert_output_bound<T: Send + Sync + 'static>(_value: T) {}

fn main() {
    let local: u32 = 7;
    let borrowed = BorrowedOutput { value: &local };
    // The output bound demands `'static`; `BorrowedOutput` borrows `local`, so
    // it does not satisfy `'static` and this fails to compile — the borrow does
    // not live long enough for `assert_output_bound`'s `'static` requirement.
    assert_output_bound(borrowed);
}
