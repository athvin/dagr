//! Compile-pass: `#[derive(StableName)]` — the default name (the struct's own
//! identifier), an explicit `#[stable_name = "…"]` override, and a generic struct
//! (the impl reproduces the generics with no added bound; the name is the bare
//! ident). Each `const _` is a **compile-time** proof that `STABLE_NAME` is exactly
//! the expected literal.

// One import brings both the derive (macro namespace) and the trait (type namespace)
// under the name `StableName` — the standard trait+derive pairing.
use dagr_core::StableName;

/// Default: the recorded name is the struct's identifier.
#[derive(StableName)]
struct Rows(u64);

/// Override: `#[stable_name = "…"]` fixes the recorded identity so a later *Rust*
/// rename does not move it.
#[derive(StableName)]
#[stable_name = "warehouse.Rows"]
struct Renamed;

/// Generic: the impl carries the type's own generics with no added `StableName`
/// bound; the default name is the bare ident (`"Wrapper"`, never `"Wrapper<T>"`).
#[derive(StableName)]
struct Wrapper<T>(T);

/// `const`-context string equality, so the name checks below are compile-time proofs.
const fn str_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    let mut i = 0;
    while i < a.len() {
        if a[i] != b[i] {
            return false;
        }
        i += 1;
    }
    true
}

const _: () = assert!(str_eq(<Rows as StableName>::STABLE_NAME, "Rows"));
const _: () = assert!(str_eq(<Renamed as StableName>::STABLE_NAME, "warehouse.Rows"));
const _: () = assert!(str_eq(<Wrapper<u64> as StableName>::STABLE_NAME, "Wrapper"));

fn main() {
    let _ = (Rows(1), Renamed, Wrapper(1u64));
}
