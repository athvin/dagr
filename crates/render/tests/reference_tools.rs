//! C24 · T46 — the rendered DOT and Mermaid are accepted by their **reference
//! tools** (arch.md C24 line 520 "both output formats are accepted by their
//! reference tools in CI: `dot` parses; Mermaid's parser accepts"). Written
//! first, TDD.
//!
//! Each test renders the **real** 30-node fixture through the renderer and runs
//! the output through the format's reference tool:
//!
//! * DOT → `dot` (Graphviz) in parse/validation mode (`-Tcanon`, output
//!   discarded), which exits zero on well-formed input and non-zero on malformed
//!   input — so the check has teeth (a deliberately-malformed diagram is
//!   rejected).
//! * Mermaid → **Mermaid's own parser**, `mermaid.parse()` from the
//!   `mermaid` library, run **browserless** under Node. The `DoD` is
//!   "Mermaid's parser accepts the Mermaid" (057 `DoD` / arch.md C24 line 520),
//!   NOT SVG rendering — so we call the parser directly instead of the `mmdc`
//!   CLI, which would launch headless Chromium via puppeteer (unavailable and
//!   flaky in CI). `mermaid.parse(src)` resolves on valid flowchart syntax and
//!   throws on invalid syntax; a tiny jsdom DOM satisfies the library's
//!   DOM/`DOMPurify` dependency without any browser. This keeps the exact
//!   arch-required guarantee ("the parser accepts") while removing all Chromium
//!   fragility from every future PR.
//!
//! # Local vs CI
//!
//! These tools are external programs, not cargo crates. Locally they may be
//! absent, so an absent tool **skips** with a printed notice (keeping
//! `cargo test --workspace` green on a developer machine without Graphviz /
//! Node). In CI the tools are installed and the environment variable
//! `DAGR_REQUIRE_RENDER_TOOLS=1` is set, which turns an absent tool into a hard
//! **failure** — so the gate is mandatory exactly where the ticket requires it
//! (CI), and a missing tool can never silently pass the acceptance gate.
//!
//! The Mermaid parser is resolved through `DAGR_MERMAID_PARSE_DIR`: a directory
//! containing a `node_modules` with the pinned `mermaid` + `jsdom` packages
//! (CI sets it up and points the env var at it). Because Node resolves bare ESM
//! imports relative to the *script file's* location, the parse helper script is
//! written into that directory so `import 'mermaid'` / `import 'jsdom'` resolve
//! against the co-located `node_modules`. When the env var is unset (local,
//! nothing installed) the Mermaid gate skips (or hard-fails under
//! `DAGR_REQUIRE_RENDER_TOOLS=1`).

use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use dagr_render::overlay::{render_dot_overlay, render_mermaid_overlay, RunArtifact};
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

/// A run artifact covering every node of the 30-node fixture (all `succeeded`),
/// so the overlaid diagram styles every node and can be fed to the reference
/// tools. Built as a published-schema run artifact.
fn thirty_node_run() -> RunArtifact {
    let attempts: Vec<serde_json::Value> = thirty_node()
        .nodes()
        .iter()
        .map(|n| {
            serde_json::json!({
                "node": n.name(),
                "attempt": 1,
                "status": "succeeded",
                "phase_durations_ns": { "executing": 1234 },
                "worker": "worker-0",
            })
        })
        .collect();
    let v = serde_json::json!({
        "header": {
            "run_id": "018f4a1e-6c2a-7b3d-9e10-0123456789ab",
            "pipeline": "example-pipeline",
            "parameters": {},
            "data_interval": null,
            "captured_environment": {},
            "resume_lineage": null,
            "overall_outcome": "succeeded"
        },
        "attempts": attempts,
        "summary": null
    });
    RunArtifact::from_json_str(&v.to_string()).unwrap()
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

/// Outcome of feeding an input to a reference tool: whether it was accepted
/// (exit zero), plus the tool's captured stderr so a failure is legible in the
/// panic message rather than swallowed.
struct ToolRun {
    accepted: bool,
    stderr: String,
}

/// Feed `input` to `program args…` on stdin; capture stderr. Accepted == exit
/// zero.
fn accepts_on_stdin(program: &str, args: &[&str], input: &str) -> ToolRun {
    let child = Command::new(program)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn();
    let Ok(mut child) = child else {
        return ToolRun {
            accepted: false,
            stderr: format!("failed to spawn `{program}`"),
        };
    };
    if let Some(mut stdin) = child.stdin.take() {
        let _ = stdin.write_all(input.as_bytes());
        // drop stdin to send EOF
    }
    match child.wait_with_output() {
        Ok(out) => ToolRun {
            accepted: out.status.success(),
            stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
        },
        Err(e) => ToolRun {
            accepted: false,
            stderr: format!("`{program}` did not complete: {e}"),
        },
    }
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
    let good = accepts_on_stdin("dot", &["-Tcanon", "-o", null_device()], &dot);
    assert!(
        good.accepted,
        "`dot` must accept the rendered DOT output; dot stderr:\n{}",
        good.stderr
    );

    // Teeth: a deliberately-malformed diagram is rejected.
    let malformed = "digraph { a -> ";
    let bad = accepts_on_stdin("dot", &["-Tcanon", "-o", null_device()], malformed);
    assert!(
        !bad.accepted,
        "`dot` must reject a malformed diagram — the check has teeth"
    );
}

/// **Reference tool accepts Mermaid (CI gate).** The fixture Mermaid output is
/// run through **Mermaid's own parser** (`mermaid.parse()`, browserless under
/// Node), which accepts it without error; a deliberately-malformed diagram is
/// rejected (the check has teeth). No `mmdc`, no puppeteer, no Chromium.
#[test]
fn mermaid_reference_tool_accepts_the_rendered_mermaid() {
    let Some(parse_dir) = mermaid_parse_dir() else {
        assert!(
            !tools_required(),
            "Mermaid's parser (mermaid.parse via Node) is required in CI \
             (DAGR_REQUIRE_RENDER_TOOLS=1) but DAGR_MERMAID_PARSE_DIR/node was not usable"
        );
        eprintln!(
            "SKIP: Mermaid parser not available (set DAGR_MERMAID_PARSE_DIR to a dir with \
             `mermaid` + `jsdom` in node_modules); skipping the Mermaid reference-tool gate"
        );
        return;
    };

    let art = thirty_node();
    let mmd = render_mermaid(&art);

    let good = mermaid_parser_accepts(&parse_dir, &mmd);
    assert!(
        good.accepted,
        "Mermaid's parser (mermaid.parse) must accept the rendered Mermaid output; parser stderr:\n{}",
        good.stderr
    );

    // Teeth: a malformed flowchart is rejected.
    let malformed = "flowchart TB\n  a --> \n";
    let bad = mermaid_parser_accepts(&parse_dir, malformed);
    assert!(
        !bad.accepted,
        "Mermaid's parser must reject a malformed diagram — the check has teeth; parser stderr:\n{}",
        bad.stderr
    );
}

/// **Reference tool accepts OVERLAID DOT (CI gate, T47).** The 30-node fixture
/// rendered with a run overlay (state colouring + duration annotations) is piped
/// through `dot` in parse/validation mode; the overlay's styling additions must
/// still produce a diagram `dot` accepts (arch.md C24 line 520 extended to the
/// overlay).
#[test]
fn dot_reference_tool_accepts_the_overlaid_dot() {
    if !tool_available("dot", &["-V"]) {
        assert!(
            !tools_required(),
            "`dot` (Graphviz) is required in CI (DAGR_REQUIRE_RENDER_TOOLS=1) but was not found"
        );
        eprintln!(
            "SKIP: `dot` (Graphviz) not installed; skipping the overlaid-DOT reference-tool gate"
        );
        return;
    }

    let dot = render_dot_overlay(&thirty_node(), &thirty_node_run());
    let good = accepts_on_stdin("dot", &["-Tcanon", "-o", null_device()], &dot);
    assert!(
        good.accepted,
        "`dot` must accept the overlaid DOT output; dot stderr:\n{}",
        good.stderr
    );
}

/// **Reference tool accepts OVERLAID Mermaid (CI gate, T47).** The 30-node
/// fixture rendered with a run overlay (per-state `classDef`/`class` + duration
/// annotations) is run through Mermaid's own parser browserless; the overlay's
/// additions must still produce Mermaid the parser accepts.
#[test]
fn mermaid_reference_tool_accepts_the_overlaid_mermaid() {
    let Some(parse_dir) = mermaid_parse_dir() else {
        assert!(
            !tools_required(),
            "Mermaid's parser (mermaid.parse via Node) is required in CI \
             (DAGR_REQUIRE_RENDER_TOOLS=1) but DAGR_MERMAID_PARSE_DIR/node was not usable"
        );
        eprintln!(
            "SKIP: Mermaid parser not available; skipping the overlaid-Mermaid reference-tool gate"
        );
        return;
    };

    let mmd = render_mermaid_overlay(&thirty_node(), &thirty_node_run());
    let good = mermaid_parser_accepts(&parse_dir, &mmd);
    assert!(
        good.accepted,
        "Mermaid's parser must accept the overlaid Mermaid output; parser stderr:\n{}",
        good.stderr
    );
}

/// The directory whose `node_modules` holds the pinned `mermaid` + `jsdom`
/// packages, iff Mermaid's browserless parser can be exercised: the env var is
/// set to an existing directory AND `node` is on PATH. Returns `None` (→ skip /
/// hard-fail) otherwise.
fn mermaid_parse_dir() -> Option<PathBuf> {
    let dir = PathBuf::from(std::env::var("DAGR_MERMAID_PARSE_DIR").ok()?);
    if !dir.is_dir() {
        return None;
    }
    if !tool_available("node", &["--version"]) {
        return None;
    }
    Some(dir)
}

/// Run Mermaid's own parser over `input`, browserless, under Node.
///
/// The parse helper (`import 'mermaid'; import 'jsdom'; await mermaid.parse(src)`)
/// is written into `parse_dir` so Node resolves its bare ESM imports against the
/// `node_modules` co-located there (Node resolves bare specifiers relative to the
/// script file, not the cwd). `mermaid.parse` resolves on valid syntax and throws
/// on invalid; the script maps that to exit 0 / non-zero and prints the parse
/// error to stderr, which we capture and surface.
fn mermaid_parser_accepts(parse_dir: &Path, input: &str) -> ToolRun {
    // The helper script — kept inline so no JS file is checked in. It lives in
    // `parse_dir` next to node_modules so `import 'mermaid'`/`import 'jsdom'`
    // resolve. A minimal jsdom DOM satisfies mermaid's DOM/DOMPurify needs
    // without any browser.
    const PARSE_JS: &str = r"import { readFileSync } from 'node:fs';
import { JSDOM } from 'jsdom';
const dom = new JSDOM('<!DOCTYPE html><html><body></body></html>', { pretendToBeVisual: true });
globalThis.window = dom.window;
globalThis.document = dom.window.document;
const mermaid = (await import('mermaid')).default;
const src = readFileSync(process.argv[2], 'utf8');
try {
  await mermaid.parse(src);
} catch (e) {
  console.error('MERMAID_PARSE_REJECTED: ' + String((e && e.message) || e).split('\n')[0]);
  process.exit(1);
}
";

    let token = rand_token();
    let script_path = parse_dir.join(format!("dagr-mermaid-parse-{token}.mjs"));
    let in_path = parse_dir.join(format!("dagr-mermaid-in-{token}.mmd"));

    let cleanup = |script: &Path, mmd: &Path| {
        let _ = std::fs::remove_file(script);
        let _ = std::fs::remove_file(mmd);
    };

    if let Err(e) = std::fs::write(&script_path, PARSE_JS) {
        return ToolRun {
            accepted: false,
            stderr: format!(
                "failed to write parse helper to {}: {e}",
                script_path.display()
            ),
        };
    }
    if let Err(e) = std::fs::write(&in_path, input) {
        cleanup(&script_path, &in_path);
        return ToolRun {
            accepted: false,
            stderr: format!(
                "failed to write Mermaid input to {}: {e}",
                in_path.display()
            ),
        };
    }

    let output = Command::new("node")
        .arg(&script_path)
        .arg(&in_path)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output();

    cleanup(&script_path, &in_path);

    match output {
        Ok(out) => ToolRun {
            accepted: out.status.success(),
            stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
        },
        Err(e) => ToolRun {
            accepted: false,
            stderr: format!("`node` did not complete: {e}"),
        },
    }
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
