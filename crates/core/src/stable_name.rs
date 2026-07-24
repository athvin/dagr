//! Author-declared **stable names** for task and payload types — the identity
//! the C20 graph artifact and the C21 fingerprints record (arch.md `### C20 ·
//! Graph artifact`, `### C21 · Graph fingerprint`; the T0.7 ADR,
//! `docs/implementation/013-T0.7-stable-name-and-fingerprint-adr.md` §1).
//!
//! Node and pipeline-shape identity rest on **author-declared** names, never on
//! a compiler-derived string. [`std::any::type_name`] is **explicitly unstable
//! across compiler versions** (its output format is not a stability guarantee),
//! so it may appear **only as an informational debug field** in the artifact —
//! never as identity, never in either fingerprint (T0.7 §1). This module carries
//! the author-declared alternative.
//!
//! # The contract (T0.7 §1)
//!
//! A [`StableName`] carries the name as an **associated constant**, implemented by
//! **both task types and payload (input/output) value types** — the types a task
//! consumes and produces. The stored name is the **declared name the author
//! writes** for the compiler-enforced type, so a recorded name always matches a
//! real, compiler-checked type and is byte-stable across toolchains by
//! construction.
//!
//! The common case is one line — a payload or task type names itself after its
//! own Rust identifier:
//!
//! ```
//! use dagr_core::stable_name::StableName;
//!
//! struct RowCount(u64);
//! impl StableName for RowCount {
//!     const STABLE_NAME: &'static str = "RowCount";
//! }
//! assert_eq!(RowCount::STABLE_NAME, "RowCount");
//! ```
//!
//! An author who needs the recorded name to survive a *Rust* rename (or to
//! disambiguate two types that share a short name) writes the constant explicitly
//! with a different value; that explicit name is then the stable identity and a
//! later Rust rename does not move it (T0.7 §1). The one-line **derive** the ADR
//! anticipates is a later ergonomic convenience; the trait itself — the identity
//! contract every consumer binds to — is this module's.
//!
//! # Well-formedness (T0.7 §1)
//!
//! A stable name is **non-empty** and drawn from a **fixed character set / shape**
//! — ASCII letters, digits, and a small punctuation set (`_`, `-`, `.`, and `::`
//! for namespacing) with **no whitespace and no control characters** — so it
//! round-trips through the artifact encoding (T4), the run-store path segment
//! where a pipeline name is a directory component (T0.6), and the DOT/Mermaid
//! renderers (T46) without escaping surprises. A malformed stable name is an
//! **assembly** failure, not a silent truncation; [`is_well_formed`] is the
//! predicate the assembly check applies. **Uniqueness** of declared task and
//! payload names is likewise a **whole-pipeline assembly check**, not a
//! compile-time one (T0.7 §1) — the check and its "names both declarations" error
//! are assembly's (C7 / T14), not this module's; this module fixes only the
//! per-name well-formedness rule and the trait its consumers read.

/// An author-declared, toolchain-stable name for a task or payload type
/// (arch.md C20; T0.7 §1).
///
/// Implemented by **task types** (the stable *task* name the artifact records)
/// and by **payload (input/output) value types** (the stable *type* name a data
/// edge and a node's input/output list record). The associated
/// [`STABLE_NAME`](StableName::STABLE_NAME) constant is the **author-declared**
/// identity — never [`std::any::type_name`], which is unstable across toolchains
/// and admitted only as an informational debug field (T0.7 §1).
///
/// # Well-formedness
///
/// A conforming implementation supplies a [well-formed](is_well_formed) name:
/// non-empty, ASCII letters/digits and the punctuation set `_ - . :`, no
/// whitespace and no control characters. A malformed name is rejected at
/// **assembly** (C7 / T14) or by the artifact builder (T40), not here — the trait
/// cannot enforce a `const`'s shape at the type level, so the enforcement point
/// is the whole-pipeline pass, consistent with the ADR's "assembly failure, not a
/// compile error" rule.
pub trait StableName {
    /// The author-declared stable name of this type — the identity the graph
    /// artifact (C20) records and the fingerprints (C21) hash. It must be
    /// [well-formed](is_well_formed); a malformed value fails at the whole-pipeline
    /// check.
    const STABLE_NAME: &'static str;
}

/// The unit type `()` — a consume-nothing task's input and an **effect-only**
/// (`()`-output) node's output type (C1). It carries the reserved
/// [`UNIT_STABLE_NAME`] sentinel so that an effect node's produced type still has
/// an author-stable name (the stable-name-aware registrar's `T::Output:
/// StableName` bound is satisfied), rather than being an un-nameable special case.
/// The sentinel is exempt from the general [`is_well_formed`] rule; the artifact
/// builder accepts it as a reserved name (T0.7 §2).
impl StableName for () {
    const STABLE_NAME: &'static str = UNIT_STABLE_NAME;
}

/// The reserved stable name of the unit type `()` — a consume-nothing input list
/// records **no** entry, and an effect-only (`()`-output) node records this
/// sentinel as its output name rather than an absent field (arch.md C1; T0.7 §2).
/// It is a **reserved** name, exempt from the [`is_well_formed`] character-set
/// rule (the parentheses are not in the general set), so the artifact builder
/// accepts it without a whole-pipeline rejection.
pub const UNIT_STABLE_NAME: &str = "()";

/// Whether `name` is a **well-formed** stable name (arch.md C20; T0.7 §1).
///
/// A well-formed stable name is **non-empty** and composed **only** of ASCII
/// letters, ASCII digits, and the punctuation set `_`, `-`, `.`, and `:` (the
/// `::` namespacing separator is two `:` characters, so `:` alone is permitted).
/// It contains **no whitespace** and **no control characters**, so it round-trips
/// through the artifact encoding, the run-store path segment, and the renderers
/// without escaping. The reserved unit sentinel [`UNIT_STABLE_NAME`] (`"()"`) is
/// **not** well-formed under this predicate — it is handled as a reserved name by
/// the artifact builder, not by this general rule.
///
/// This is the predicate the **whole-pipeline** stable-name validity check (C7 /
/// T14; the artifact builder, T40) applies; a malformed name is a failure, never a
/// silent truncation (T0.7 §1).
#[must_use]
pub fn is_well_formed(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.' | b':'))
}

/// Declare the **stable names of a task's declared input types**, positionally
/// (arch.md C20; T0.7 §2).
///
/// A **data-dependent** task's [`Input`](crate::task::Task::Input) is either a
/// single payload type or a tuple of payload types (the C3 binding, up to
/// [`MAX_INPUT_ARITY`](crate::binding::MAX_INPUT_ARITY)). This sealed trait maps
/// that input shape to the **ordered list of author-declared stable input type
/// names** the graph artifact records for the node, so the recorded input names
/// match the compiler-enforced input types exactly.
///
/// It is implemented for a single [`StableName`] payload type (a one-element list)
/// and for tuples of [`StableName`] payload types (2..=8), mirroring the
/// [`Deps`](crate::binding::Deps) arities. A **source** node consumes nothing, so
/// its input list is empty by construction — the source registrar records an empty
/// list directly and never routes through this trait, which is why there is no
/// `()` case here. It is **sealed** so the set of input shapes stays the curated,
/// finite set the binding machinery fixes.
pub trait StableInputNames: sealed::SealedInputs {
    /// The ordered stable names of this input shape's payload types — one entry
    /// per bound input, in declaration order.
    #[must_use]
    fn stable_input_names() -> Vec<&'static str>;
}

// A single-input data node: `type Input = T` for one payload type `T: StableName`.
// Tuples do not implement `StableName`, so this blanket impl does not overlap the
// tuple impls below; `()` (a source) never routes through this trait.
impl<T: StableName> StableInputNames for T {
    fn stable_input_names() -> Vec<&'static str> {
        vec![<T as StableName>::STABLE_NAME]
    }
}

macro_rules! stable_inputs_tuple {
    ($($ty:ident => $idx:tt),+) => {
        impl<$($ty: StableName),+> StableInputNames for ($($ty,)+) {
            fn stable_input_names() -> Vec<&'static str> {
                vec![$(<$ty as StableName>::STABLE_NAME),+]
            }
        }
    };
}
stable_inputs_tuple!(I0 => 0, I1 => 1);
stable_inputs_tuple!(I0 => 0, I1 => 1, I2 => 2);
stable_inputs_tuple!(I0 => 0, I1 => 1, I2 => 2, I3 => 3);
stable_inputs_tuple!(I0 => 0, I1 => 1, I2 => 2, I3 => 3, I4 => 4);
stable_inputs_tuple!(I0 => 0, I1 => 1, I2 => 2, I3 => 3, I4 => 4, I5 => 5);
stable_inputs_tuple!(I0 => 0, I1 => 1, I2 => 2, I3 => 3, I4 => 4, I5 => 5, I6 => 6);
stable_inputs_tuple!(I0 => 0, I1 => 1, I2 => 2, I3 => 3, I4 => 4, I5 => 5, I6 => 6, I7 => 7);

mod sealed {
    use super::StableName;

    /// Sealed guard for [`StableInputNames`](super::StableInputNames): only a
    /// single [`StableName`] payload and tuples of [`StableName`] payloads
    /// implement it, so the input-shape set stays the curated finite set the
    /// binding machinery fixes.
    pub trait SealedInputs {}

    impl<T: StableName> SealedInputs for T {}

    macro_rules! seal_inputs_tuple {
        ($($ty:ident),+) => {
            impl<$($ty: StableName),+> SealedInputs for ($($ty,)+) {}
        };
    }
    seal_inputs_tuple!(I0, I1);
    seal_inputs_tuple!(I0, I1, I2);
    seal_inputs_tuple!(I0, I1, I2, I3);
    seal_inputs_tuple!(I0, I1, I2, I3, I4);
    seal_inputs_tuple!(I0, I1, I2, I3, I4, I5);
    seal_inputs_tuple!(I0, I1, I2, I3, I4, I5, I6);
    seal_inputs_tuple!(I0, I1, I2, I3, I4, I5, I6, I7);
}
