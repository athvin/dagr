//! Compile-fail: `#[stable_name("x")]` — the list form, not the accepted
//! `#[stable_name = "…"]` name-value pair. The derive rejects it with a spanned
//! `compile_error!` naming the accepted grammar.

use dagr_core::StableName;

#[derive(StableName)]
#[stable_name("x")]
struct Bad;

fn main() {
    let _ = Bad;
}
