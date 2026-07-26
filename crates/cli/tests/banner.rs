//! Startup banner — black-box acceptance tests for the `dagr` reference binary.
//!
//! The banner is a cosmetic startup splash printed to **stderr** on every
//! invocation, suppressible with `--no-banner`, `DAGR_NO_BANNER`, or `NO_COLOR`.
//! These tests drive the real compiled `dagr` binary as a subprocess (exactly as
//! an operator sees it) and assert only on the observable contract: which stream
//! the banner lands on, that stdout is never touched by it, that each suppression
//! channel works (in any flag position), and that the output is deterministic.
//!
//! Every invocation runs with `NO_COLOR`/`DAGR_NO_BANNER` explicitly *removed* from
//! the child environment (except where a test sets one), so a suppressor inherited
//! from the surrounding CI shell can never make a "banner present" assertion flaky.

use std::process::{Command, Output, Stdio};

/// The compiled reference binary. Cargo sets `CARGO_BIN_EXE_dagr` when building
/// this integration test, so the path resolves at build time — the real binary.
const DAGR: &str = env!("CARGO_BIN_EXE_dagr");

/// A stable, banner-unique marker (the tagline). If the banner printed, this
/// substring appears on the stream it printed to.
const MARKER: &str = "deterministic · parallel DAG runner";

const SUCCESS: i32 = 0;

/// Run `dagr` with a clean environment: `NO_COLOR` and `DAGR_NO_BANNER` removed so
/// an inherited value can't perturb the result. Stdin is null (the verbs used here
/// never read it, but this keeps the child from ever blocking).
fn run(args: &[&str]) -> Output {
    Command::new(DAGR)
        .args(args)
        .env_remove("NO_COLOR")
        .env_remove("DAGR_NO_BANNER")
        .stdin(Stdio::null())
        .output()
        .expect("the dagr binary launches as a separate OS process and is reaped")
}

/// Run with a single environment variable set on top of the clean base.
fn run_env(args: &[&str], key: &str, val: &str) -> Output {
    Command::new(DAGR)
        .args(args)
        .env_remove("NO_COLOR")
        .env_remove("DAGR_NO_BANNER")
        .env(key, val)
        .stdin(Stdio::null())
        .output()
        .expect("the dagr binary launches as a separate OS process and is reaped")
}

fn code(out: &Output) -> i32 {
    out.status
        .code()
        .expect("the child exited with a code, not a signal")
}
fn stdout(out: &Output) -> String {
    String::from_utf8_lossy(&out.stdout).into_owned()
}
fn stderr(out: &Output) -> String {
    String::from_utf8_lossy(&out.stderr).into_owned()
}

/// By default the banner prints to **stderr**, never stdout, and the exit is clean.
/// stdout still carries the verb-listing help — proving the banner rides its own
/// stream and cannot contaminate machine-readable output.
#[test]
fn default_invocation_prints_banner_to_stderr_not_stdout() {
    let out = run(&[]);
    assert_eq!(code(&out), SUCCESS, "a bare invocation exits success");
    assert!(
        stderr(&out).contains(MARKER),
        "the banner is printed to stderr, got stderr: {:?}",
        stderr(&out)
    );
    assert!(
        !stdout(&out).contains(MARKER),
        "the banner never appears on stdout, got stdout: {:?}",
        stdout(&out)
    );
    assert!(
        stdout(&out).contains("Available verbs"),
        "the verb-listing help still goes to stdout"
    );
}

/// `--no-banner` before any verb suppresses the banner; help still prints.
#[test]
fn no_banner_flag_suppresses_in_leading_position() {
    let out = run(&["--no-banner"]);
    assert_eq!(code(&out), SUCCESS, "still a clean bare invocation");
    assert!(
        !stderr(&out).contains(MARKER),
        "`--no-banner` suppresses the banner, got stderr: {:?}",
        stderr(&out)
    );
    assert!(
        stdout(&out).contains("Available verbs"),
        "help output is unaffected by the toggle"
    );
}

/// `--no-banner` is position-independent: it works *after* a verb too (the flag is
/// stripped from argv before verb parsing, so its position is irrelevant).
#[test]
fn no_banner_flag_suppresses_after_a_verb() {
    // `graph` is a pipeline-bound verb the reference binary rejects without reading
    // stdin and without touching the run store — a clean vehicle for observing the
    // flag in trailing position. (`run` now routes through the flow registry and
    // drives a flow, so it would write a store; `graph` keeps this test store-free.)
    let leading = run(&["--no-banner", "graph"]);
    let trailing = run(&["graph", "--no-banner"]);
    for (label, out) in [("leading", &leading), ("trailing", &trailing)] {
        assert!(
            !stderr(out).contains(MARKER),
            "`--no-banner` in {label} position suppresses the banner, got stderr: {:?}",
            stderr(out)
        );
    }
    // Same verb, same outcome regardless of flag position — the strip is genuinely
    // position-independent (the flag carries no verb semantics).
    assert_eq!(
        code(&leading),
        code(&trailing),
        "flag position does not change the parsed verb / exit code"
    );
}

/// `DAGR_NO_BANNER` (non-empty) suppresses the banner.
#[test]
fn dagr_no_banner_env_suppresses() {
    let out = run_env(&[], "DAGR_NO_BANNER", "1");
    assert!(
        !stderr(&out).contains(MARKER),
        "DAGR_NO_BANNER=1 suppresses the banner, got stderr: {:?}",
        stderr(&out)
    );
}

/// The `NO_COLOR` convention (non-empty) also suppresses the banner.
#[test]
fn no_color_env_suppresses() {
    let out = run_env(&[], "NO_COLOR", "1");
    assert!(
        !stderr(&out).contains(MARKER),
        "NO_COLOR=1 suppresses the banner, got stderr: {:?}",
        stderr(&out)
    );
}

/// An **empty** suppressor variable does not suppress — the no-color.org
/// convention is "present AND non-empty".
#[test]
fn empty_suppressor_env_does_not_suppress() {
    let out = run_env(&[], "DAGR_NO_BANNER", "");
    assert!(
        stderr(&out).contains(MARKER),
        "an empty DAGR_NO_BANNER leaves the banner on, got stderr: {:?}",
        stderr(&out)
    );
}

/// Turning the banner on or off changes **only** stderr: stdout is byte-identical
/// with and without it. This is the load-bearing guarantee that the banner can
/// never corrupt a verb's machine-readable stdout.
#[test]
fn banner_never_changes_stdout() {
    let with = run(&[]);
    let without = run(&["--no-banner"]);
    assert_eq!(
        stdout(&with),
        stdout(&without),
        "the banner lives on stderr only; stdout is unchanged by the toggle"
    );
}

/// The banner is deterministic: two identical invocations produce byte-identical
/// stderr (static, colour-free constant — no timestamps, no TTY formatting).
#[test]
fn banner_is_deterministic() {
    let a = run(&[]);
    let b = run(&[]);
    assert_eq!(
        stderr(&a),
        stderr(&b),
        "the banner is a static constant, so stderr is byte-stable across runs"
    );
}
