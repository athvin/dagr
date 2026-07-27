//! Compile-fail: a `#[dag]` fn with the **wrong shape** — it declares no
//! `&mut FlowBuilder` parameter, so the generated factory's `#fn(&mut builder)` call
//! cannot type-check.
//!
//! `#[dag]` deliberately does **not** re-validate the fn signature in the macro: it
//! keeps the user's fn verbatim and generates a factory that hands it a
//! `&mut FlowBuilder`. A mis-shaped fn (here, one taking no arguments) therefore
//! surfaces as a natural, well-attributed argument-count error at the generated call
//! site — the same discipline `#[task]`'s `result_ok_type` relies on for a divergent
//! error type (see `dagr-macros`' `expand_dag`). This snapshot pins that diagnostic so
//! a drift is a red build. (Duplicate DAG *names* are a different misuse and surface at
//! **runtime** — `dagr_cli::run` rejects them with `InvalidUsage`, T79 — never here at
//! compile time; the corpus has no compile-fail fixture for a duplicate name.)

use dagr_cli::dag;

#[dag]
fn etl() {}

fn main() {}
