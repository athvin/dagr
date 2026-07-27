//! Compile-fail: `#[dag(bogus)]` — an argument that is not the accepted
//! `name = "…"` grammar. The macro rejects it with a `compile_error!` naming the
//! accepted form, mirroring `#[task]`'s attribute rejection. This snapshot pins that
//! message so a drift in the wording is a red build.

use dagr_cli::dag;

#[dag(bogus)]
fn etl(_f: &mut dagr_cli::prelude::FlowBuilder) {}

fn main() {}
