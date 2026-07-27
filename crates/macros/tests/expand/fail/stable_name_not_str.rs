//! Compile-fail: `#[stable_name = 42]` — the right key but a non-string value. The
//! derive rejects it with a spanned `compile_error!` naming the accepted
//! `#[stable_name = "…"]` string-literal grammar.

use dagr_core::StableName;

#[derive(StableName)]
#[stable_name = 42]
struct Bad;

fn main() {
    let _ = Bad;
}
