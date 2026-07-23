//! Standalone dagr renderer binary (placeholder skeleton).
//!
//! Its purpose at this milestone is structural, not behavioural: it proves that
//! a renderer builds and links as its own binary with **no access to the
//! pipeline binary** — it depends only on `dagr-render` (and transitively
//! `dagr-artifact`), never on `dagr-core` or `dagr-cli` (arch.md
//! C24 · Renderers). The ADR in ticket T1 records this as the chosen answer to
//! "must the renderer be a separate binary?": the renderer is a library usable
//! both as this standalone binary and as the pipeline binary's `render`
//! subcommand, and the artifact-only crate edge is what satisfies C24 either
//! way.
//!
//! Actual argument parsing and DOT/Mermaid emission land in C24's own tickets
//! (T46, T47); this placeholder does nothing yet.

fn main() {}
