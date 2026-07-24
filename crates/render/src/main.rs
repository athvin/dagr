//! Standalone dagr renderer binary (arch.md `### C24 · Renderers`).
//!
//! Reads **one graph artifact** and writes diagram source — Graphviz DOT
//! (default) or Mermaid — to standard output. Its independence is structural, not
//! merely behavioural: it links only `dagr-render` (and transitively
//! `dagr-artifact`), never `dagr-core` or `dagr-cli`, so a renderer provably
//! needs **no access to the binary that produced the artifact** (C24 line 523).
//! The T1 ADR records this as the answer to "must the renderer be a separate
//! binary?": the renderer is a library usable both as this standalone binary and
//! as the pipeline binary's `render` subcommand, and the artifact-only crate edge
//! satisfies C24 either way.
//!
//! Usage:
//!
//! ```text
//! dagr-render [--format dot|mermaid] <graph-artifact.json>
//! ```
//!
//! The user-facing CLI `render` verb (argument parsing, exit-code table) is
//! C26/T55 — this binary is the minimal, dependency-proving standalone form.

use std::process::ExitCode;

use dagr_render::{render_dot, render_mermaid, GraphArtifact};

/// The output format the standalone binary emits.
enum Format {
    Dot,
    Mermaid,
}

fn main() -> ExitCode {
    let mut format = Format::Dot;
    let mut path: Option<String> = None;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--format" | "-f" => {
                let Some(value) = args.next() else {
                    return usage_error("--format needs a value: dot or mermaid");
                };
                format = match value.as_str() {
                    "dot" => Format::Dot,
                    "mermaid" | "mmd" => Format::Mermaid,
                    other => return usage_error(&format!("unknown format `{other}`")),
                };
            }
            "-h" | "--help" => {
                print_usage();
                return ExitCode::SUCCESS;
            }
            other if path.is_none() => path = Some(other.to_string()),
            other => return usage_error(&format!("unexpected argument `{other}`")),
        }
    }

    let Some(path) = path else {
        return usage_error("a graph-artifact path is required");
    };

    let raw = match std::fs::read_to_string(&path) {
        Ok(raw) => raw,
        Err(e) => {
            eprintln!("dagr-render: cannot read {path}: {e}");
            return ExitCode::FAILURE;
        }
    };

    let artifact = match GraphArtifact::from_json_str(&raw) {
        Ok(artifact) => artifact,
        Err(e) => {
            // A schema-invalid artifact is refused with a diagnostic naming the
            // problem, not rendered partially (C24).
            eprintln!("dagr-render: {path}: {e}");
            return ExitCode::FAILURE;
        }
    };

    let output = match format {
        Format::Dot => render_dot(&artifact),
        Format::Mermaid => render_mermaid(&artifact),
    };
    print!("{output}");
    ExitCode::SUCCESS
}

fn print_usage() {
    eprintln!("usage: dagr-render [--format dot|mermaid] <graph-artifact.json>");
}

fn usage_error(msg: &str) -> ExitCode {
    eprintln!("dagr-render: {msg}");
    print_usage();
    ExitCode::FAILURE
}
