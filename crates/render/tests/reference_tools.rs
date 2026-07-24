//! C24 · T46 — the rendered DOT and Mermaid are accepted by their **reference
//! tools** (arch.md C24 line 520 "both output formats are accepted by their
//! reference tools in CI: `dot` parses; Mermaid's parser accepts"). Written
//! first, TDD.
//!
//! Each test renders the **real** 30-node fixture through the renderer and pipes
//! the output through the format's reference tool:
//!
//! * DOT → `dot` (Graphviz) in parse/validation mode (`-Tcanon`, output
//!   discarded), which exits zero on well-formed input and non-zero on malformed
//!   input — so the check has teeth (a deliberately-malformed diagram is
//!   rejected).
//! * Mermaid → the Mermaid CLI parser (`mmdc`, `@mermaid-js/mermaid-cli`), which
//!   accepts well-formed flowchart source and errors on malformed input.
//!
//! # Local vs CI
//!
//! These tools are external binaries, not cargo crates. Locally they may be
//! absent, so an absent tool **skips** with a printed notice (keeping
//! `cargo test --workspace` green on a developer machine without Graphviz /
//! Node). In CI the tools are installed and the environment variable
//! `DAGR_REQUIRE_RENDER_TOOLS=1` is set, which turns an absent tool into a hard
//! **failure** — so the gate is mandatory exactly where the ticket requires it
//! (CI), and a missing tool can never silently pass the acceptance gate.

use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use dagr_render::{render_dot, render_mermaid, GraphArtifact};

/// Whether CI has declared the reference tools mandatory. When set, an absent
/// tool is a failure, not a skip.
fn tools_required() -> bool {
    std::env::var("DAGR_REQUIRE_RENDER_TOOLS")
        .is_ok_and(|v| v == "1" || v.eq_ignore_ascii_case("true"))
}

fn fixture_path(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(name)
}

fn thirty_node() -> GraphArtifact {
    let raw = std::fs::read_to_string(fixture_path("thirty-node.graph.json")).unwrap();
    GraphArtifact::from_json_str(&raw).unwrap()
}

/// True if `program` (optionally with a probe arg) can be executed.
fn tool_available(program: &str, probe_args: &[&str]) -> bool {
    Command::new(program)
        .args(probe_args)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}

/// Feed `input` to `program args…` on stdin and return whether it exited zero.
fn accepts_on_stdin(program: &str, args: &[&str], input: &str) -> bool {
    let Ok(mut child) = Command::new(program)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
    else {
        return false;
    };
    if let Some(mut stdin) = child.stdin.take() {
        let _ = stdin.write_all(input.as_bytes());
        // drop stdin to send EOF
    }
    child.wait().is_ok_and(|s| s.success())
}

/// **Reference tool accepts DOT (CI gate).** The fixture DOT output is piped
/// through `dot` in parse/validation mode; `dot` accepts it and exits zero, and
/// a deliberately-malformed diagram is rejected (the check has teeth).
#[test]
fn dot_reference_tool_accepts_the_rendered_dot() {
    if !tool_available("dot", &["-V"]) {
        assert!(
            !tools_required(),
            "`dot` (Graphviz) is required in CI (DAGR_REQUIRE_RENDER_TOOLS=1) but was not found"
        );
        eprintln!("SKIP: `dot` (Graphviz) not installed; skipping the DOT reference-tool gate");
        return;
    }

    let art = thirty_node();
    let dot = render_dot(&art);

    // `-Tcanon` parses and pretty-prints; output discarded. Exit zero == parsed.
    assert!(
        accepts_on_stdin("dot", &["-Tcanon", "-o", null_device()], &dot),
        "`dot` must accept the rendered DOT output"
    );

    // Teeth: a deliberately-malformed diagram is rejected.
    let malformed = "digraph { a -> ";
    assert!(
        !accepts_on_stdin("dot", &["-Tcanon", "-o", null_device()], malformed),
        "`dot` must reject a malformed diagram — the check has teeth"
    );
}

/// **Reference tool accepts Mermaid (CI gate).** The fixture Mermaid output is
/// run through Mermaid's parser (`mmdc`), which accepts it without error; a
/// deliberately-malformed diagram is rejected.
#[test]
fn mermaid_reference_tool_accepts_the_rendered_mermaid() {
    // `mmdc` reads from a file, not stdin, and needs a headless browser; probe by
    // running `--version`.
    if !tool_available("mmdc", &["--version"]) {
        assert!(
            !tools_required(),
            "Mermaid's parser (`mmdc`) is required in CI (DAGR_REQUIRE_RENDER_TOOLS=1) but was not found"
        );
        eprintln!(
            "SKIP: `mmdc` (mermaid-cli) not installed; skipping the Mermaid reference-tool gate"
        );
        return;
    }

    let art = thirty_node();
    let mmd = render_mermaid(&art);

    assert!(
        mmdc_accepts(&mmd),
        "Mermaid's parser (`mmdc`) must accept the rendered Mermaid output"
    );

    // Teeth: a malformed flowchart is rejected.
    let malformed = "flowchart TB\n  a --> \n";
    assert!(
        !mmdc_accepts(malformed),
        "Mermaid's parser must reject a malformed diagram — the check has teeth"
    );
}

/// Write `input` to a temp `.mmd` and run `mmdc` producing an SVG in a temp dir;
/// return whether it exited zero. `mmdc` parses (and renders) the input, so a
/// zero exit means the parser accepted it.
fn mmdc_accepts(input: &str) -> bool {
    let dir = std::env::temp_dir().join(format!("dagr-render-mmdc-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&dir);
    let in_path = dir.join(format!("in-{}.mmd", rand_token()));
    let out_path = dir.join(format!("out-{}.svg", rand_token()));
    if std::fs::write(&in_path, input).is_err() {
        return false;
    }
    let status = Command::new("mmdc")
        .arg("--input")
        .arg(&in_path)
        .arg("--output")
        .arg(&out_path)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
    let ok = status.is_ok_and(|s| s.success());
    let _ = std::fs::remove_file(&in_path);
    let _ = std::fs::remove_file(&out_path);
    ok
}

/// The platform null device path for discarding `dot`'s output.
fn null_device() -> &'static str {
    if cfg!(windows) {
        "NUL"
    } else {
        "/dev/null"
    }
}

/// A cheap unique-ish token for temp file names (no external rng dependency).
fn rand_token() -> u128 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos())
}
