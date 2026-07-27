//! Compile-fail: `#[dag(name = 42)]` — the right key (`name`) but a non-string
//! value. A DAG name is a string literal, so the macro rejects a non-`"…"` value
//! with a `compile_error!` naming the accepted `name = "…"` grammar. This snapshot
//! pins that message.

use dagr_cli::dag;

#[dag(name = 42)]
fn etl(_f: &mut dagr_cli::prelude::FlowBuilder) {}

fn main() {}
