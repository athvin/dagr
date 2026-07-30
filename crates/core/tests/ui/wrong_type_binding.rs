//! UI compile-failure sample (harness seed).
//!
//! This is the ONE worked sample the harness ships. It is a throwaway,
//! intentionally NON-COMPILING snippet — NOT a use of dagr's real authoring API
//! (handles, binding, the builder typestate), which does not exist yet and lands
//! in the later builder work. Its only job is to make the harness
//! prove itself against a diagnostic that mentions two distinct type names: a
//! wrong-type binding is a compile error whose message names both the
//! expected and the supplied type, and the harness asserts only that both type
//! names appear. The real wiring compile-fail cases — cyclic construction,
//! wrong-arity binding, a non-`all-succeeded` trigger rule on a data-consuming
//! node, the arity-ceiling curated diagnostic — arrive later; they drop into this
//! same `tests/ui/` directory with no harness changes.
//!
//! The two type names the harness keys on are `ExpectedWidget` (the declared /
//! expected type) and `SuppliedGadget` (the wrong / supplied type). They are
//! deliberately custom, unmistakable names so the both-names assertion can never
//! pass vacuously and is legible in review.

struct ExpectedWidget;
struct SuppliedGadget;

fn consume(_widget: ExpectedWidget) {}

fn main() {
    // Supplying `SuppliedGadget` where `ExpectedWidget` is expected is an
    // E0308 "mismatched types" error whose message names both types.
    consume(SuppliedGadget);
}
