//! **A source discriminator for config errors, and strict `DAGR_LOG_FORMAT`**
//! — integration tests. Written first, TDD.
//!
//! Two honesty defects, one suite. First: `EnvParseError` could only describe an
//! environment variable, so a bad value that came from a **flag** or from
//! `dagr.toml` produced a factually wrong diagnostic ("environment variable
//! `DAGR_HEADROOM` …" for a value the operator wrote in a file). This suite pins
//! the widened type: a **source discriminator** covering flag / env / file
//! (path, profile, key), each rendered correctly and specifically, with the
//! `Parse` → `InvalidUsage` / `OutOfRange` → `BootstrapFailure` split unchanged
//! and the **env wording byte-identical** to what shipped (a regression guard).
//! Second: `DAGR_LOG_FORMAT` silently swallowed bad values (`humann` →
//! structured logs, no complaint), the one knob outside the strict regime. This
//! suite pins the strict resolution (`--dagr.log-format` > `DAGR_LOG_FORMAT` >
//! the `log-format` file key > `structured`), the loud failure listing the
//! accepted values, and the documentation going back to being unconditional.
//!
//! # Two spawn styles, and why (mirrors `tests/config_file_and_profiles.rs`)
//!
//! - **Subprocess tests** run the `one_dag` example (a real registry-routed
//!   leaf binary) via `cargo run --example`, setting `DAGR_*` per-command — the
//!   child observes the scenario's environment and nothing inherited, and the
//!   numeric exit code is observable.
//! - **In-process tests** drive the resolvers directly. These mutate the real
//!   `DAGR_*` names, so they hold this suite's process-global env lock across
//!   the whole clear → set → read → clear window.
//!
//! The "no configuration at all → byte-identical event stream" guard already
//! lives in `tests/wire_precedence_run_path.rs`
//! (`empty_environment_streams_are_byte_identical`) and keeps covering this
//! ticket's no-regression clause; a light clean-run smoke test below asserts
//! the zero-config exit path is untouched.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Mutex, MutexGuard, OnceLock};

use dagr_cli::config::{
    ConfigSource, DAGR_EXECUTOR, DAGR_FAILURE_MODE, DAGR_FORCE_ROUNDTRIP, DAGR_GRACE,
    DAGR_HEADROOM, DAGR_MAX_PODS, DAGR_POOL_BLOCKING_THREADS, DAGR_POOL_COMPUTE_THREADS,
    DAGR_POOL_MEMORY, DAGR_STORE, DAGR_TEARDOWN_DEADLINE, EXECUTOR_FLAG, EnvParseError,
    EnvParseErrorKind, FAILURE_MODE_FLAG, FORCE_ROUNDTRIP_FLAG, GRACE_FLAG, HEADROOM_FLAG,
    LOG_FORMAT_FLAG, MAX_PODS_FLAG, POOL_BLOCKING_THREADS_FLAG, POOL_COMPUTE_THREADS_FLAG,
    POOL_MEMORY_FLAG, STORE_FLAG, TEARDOWN_DEADLINE_FLAG, resolve_headroom, resolve_log_format,
};
use dagr_cli::config_file::{FileTier, load_file_tier_from};
use dagr_cli::contract::{
    ExitCode, NO_BANNER_ENV, NO_BANNER_FLAG, ParamSpec, ParseOutcome, check_reserved_collision,
    parse_cli, reserved_flag_names,
};
use dagr_cli::driver::RunConfig;
use dagr_cli::logging::{LOG_FORMAT_ENV, OutputMode};
use dagr_core::test_kit::TempBase;

/// Every `DAGR_*` name a scenario here could observe, so the subprocess helper
/// scrubs inherited values and the in-process helper guarantees a clean slate.
const ENV_NAMES: [&str; 10] = [
    "DAGR_GRACE",
    "DAGR_TEARDOWN_DEADLINE",
    "DAGR_FAILURE_MODE",
    "DAGR_POOL_COMPUTE_THREADS",
    "DAGR_POOL_BLOCKING_THREADS",
    "DAGR_POOL_MEMORY",
    "DAGR_HEADROOM",
    "DAGR_STORE",
    "DAGR_PROFILE",
    "DAGR_LOG_FORMAT",
];

// ===========================================================================
// Subprocess plumbing (mirrors tests/config_file_and_profiles.rs)
// ===========================================================================

/// The repo root, two levels above this crate's manifest.
fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("crates/cli has a two-level ancestor (the repo root)")
        .to_path_buf()
}

/// The captured outcome of one example invocation.
struct Run {
    /// The process exit code (numeric, compared against `ExitCode::as_u8`).
    code: i32,
    /// Everything the example wrote to stdout (run diagnostics).
    stdout: String,
    /// Everything the example wrote to stderr (startup/bootstrap diagnostics).
    stderr: String,
}

impl Run {
    /// stdout and stderr concatenated — for diagnostics that may land on either.
    fn combined(&self) -> String {
        format!("{}{}", self.stdout, self.stderr)
    }
}

/// Run the compiled `one_dag` example (its sole flow is named `only`) with
/// `args`, after scrubbing every `DAGR_*` name above and setting exactly
/// `envs` — so the child observes the scenario's environment and nothing
/// inherited.
fn run_one_dag(envs: &[(&str, &str)], args: &[&str]) -> Run {
    let mut cmd = Command::new(env!("CARGO"));
    cmd.current_dir(repo_root()).args([
        "run",
        "--quiet",
        "-p",
        "dagr-cli",
        "--example",
        "one_dag",
        "--",
    ]);
    cmd.args(args);
    for name in ENV_NAMES {
        cmd.env_remove(name);
    }
    for (k, v) in envs {
        cmd.env(k, v);
    }
    let out = cmd
        .output()
        .unwrap_or_else(|e| panic!("failed to spawn `cargo run --example one_dag`: {e}"));
    Run {
        code: out.status.code().unwrap_or(-1),
        stdout: String::from_utf8_lossy(&out.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
    }
}

/// Write `content` to `<dir>/<name>` and return the absolute path as a string.
fn write_file(dir: &str, name: &str, content: &str) -> String {
    let path = Path::new(dir).join(name);
    std::fs::write(&path, content).expect("test config file writes");
    path.to_str().expect("temp paths are UTF-8").to_string()
}

// ===========================================================================
// In-process env plumbing
// ===========================================================================

fn env_lock() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Clear every `DAGR_*` name above, set `pairs`, run `body`, then clear again —
/// all under the env lock.
#[allow(
    unsafe_code,
    reason = "std::env::set_var/remove_var are unsafe fns in edition 2024; the \
              resolvers under test read the REAL process environment by name, so \
              a test cannot avoid mutating it — this helper is the only place \
              this suite does, and it holds the env lock for the whole window"
)]
fn with_env<T>(pairs: &[(&str, &str)], body: impl FnOnce() -> T) -> T {
    let _guard = env_lock();
    for k in ENV_NAMES {
        // SAFETY: mutating the environment races a concurrent `getenv`; `_guard`
        // holds this suite's process-global env lock for the whole
        // clear → set → read → clear window, so no other env-touching test here
        // can be reading while the array is updated.
        unsafe { std::env::remove_var(k) };
    }
    for (k, v) in pairs {
        // SAFETY: as above — still inside the `_guard` window.
        unsafe { std::env::set_var(k, v) };
    }
    let out = body();
    for k in ENV_NAMES {
        // SAFETY: as above — still inside the `_guard` window.
        unsafe { std::env::remove_var(k) };
    }
    out
}

/// Load a [`FileTier`] from `content` written under a temp base, with no
/// profile selection (the `default` table alone applies).
fn tier_from(base: &TempBase, content: &str) -> (String, FileTier) {
    let cfg = write_file(base.as_str(), "cfg.toml", content);
    let tier = with_env(&[], || {
        load_file_tier_from(Path::new(base.as_str()), Some(&cfg), None)
            .expect("a valid test config loads")
    });
    (cfg, tier)
}

// ===========================================================================
// The source discriminator — diagnostics name their own source
// ===========================================================================

/// The **env** wording is unchanged from what shipped, byte for byte — the
/// regression guard the ticket demands, for both kinds.
#[test]
fn an_env_sourced_error_message_is_unchanged_verbatim() {
    let parse = EnvParseError::parse("DAGR_T116_GUARD", "nope", "detail text");
    assert_eq!(
        parse.to_string(),
        "environment variable `DAGR_T116_GUARD` = `nope` could not be parsed: detail text \
         (arch.md C26 / ADR 089 — bad env values fail loudly and are never silently ignored)",
        "the env-sourced Parse wording is a stable operator contract"
    );
    let range = EnvParseError::out_of_range("DAGR_T116_GUARD", "1.5", "expected 0.0..=1.0");
    assert_eq!(
        range.to_string(),
        "environment variable `DAGR_T116_GUARD` = `1.5` is out of range: expected 0.0..=1.0 \
         (arch.md C26 / ADR 089 — bad env values fail loudly and are never silently ignored)",
        "the env-sourced OutOfRange wording is a stable operator contract"
    );
}

/// A bad value from a **flag** names the `--flag` spelling and does not claim
/// an environment variable supplied it. `validate_headroom` was the offender:
/// it passed a flag name into a field called `variable`.
#[test]
fn a_flag_sourced_error_names_the_flag_and_not_an_env_var() {
    let err = with_env(&[], || {
        resolve_headroom(Some(1.5), &FileTier::empty())
            .expect_err("an out-of-range flag headroom fails loudly")
    });
    assert_eq!(err.kind, EnvParseErrorKind::OutOfRange);
    assert_eq!(err.exit_code(), ExitCode::BootstrapFailure);
    let text = err.to_string();
    assert!(
        text.contains(HEADROOM_FLAG),
        "the diagnostic names the flag spelling: {text}"
    );
    assert!(
        !text.contains("environment variable"),
        "a flag-sourced diagnostic must not claim an env var supplied the value: {text}"
    );
}

/// Each source keeps the `Parse` → `InvalidUsage` / `OutOfRange` →
/// `BootstrapFailure` mapping, and each renders its own provenance: a flag by
/// its spelling, an env var by its name, a file value by path, profile, and
/// key.
#[test]
fn each_source_keeps_the_exit_code_split_and_renders_itself() {
    let sources = [
        ConfigSource::flag("--dagr.headroom-fraction"),
        ConfigSource::env("DAGR_HEADROOM"),
        ConfigSource::file("/tmp/dagr.toml", "prod", "pool.headroom-fraction"),
    ];
    for source in sources {
        let parse = EnvParseError::parse_from(source.clone(), "bad", "why");
        assert_eq!(parse.kind, EnvParseErrorKind::Parse);
        assert_eq!(parse.exit_code(), ExitCode::InvalidUsage);
        let range = EnvParseError::out_of_range_from(source, "1.5", "why");
        assert_eq!(range.kind, EnvParseErrorKind::OutOfRange);
        assert_eq!(range.exit_code(), ExitCode::BootstrapFailure);
    }

    let flag = EnvParseError::parse_from(ConfigSource::flag("--grace"), "soon", "why").to_string();
    assert!(flag.contains("--grace"), "flag rendering: {flag}");
    assert!(
        !flag.contains("environment variable"),
        "a flag source never claims to be an env var: {flag}"
    );

    let file = EnvParseError::parse_from(
        ConfigSource::file("/tmp/dagr.toml", "prod", "grace"),
        "soon",
        "why",
    )
    .to_string();
    assert!(
        file.contains("/tmp/dagr.toml") && file.contains("prod") && file.contains("grace"),
        "a file source names path, profile, and key: {file}"
    );
    assert!(
        !file.contains("environment variable"),
        "a file source never claims to be an env var: {file}"
    );
}

/// A bad value from a **file**, resolved through the real loader and the real
/// `RunConfig` fallback builder, names the file path, the profile, and the key
/// — and does not say "environment variable". This is the diagnostic T115 had
/// to approximate in the detail text.
#[test]
fn a_file_sourced_parse_error_names_file_profile_and_key() {
    let base = TempBase::new("t116-file-parse");
    let (cfg, tier) = tier_from(&base, "[default]\ngrace = \"soon\"\n");
    let err = with_env(&[], || {
        RunConfig::new(base.as_str())
            .grace_from_env(None, tier.get("grace"))
            .expect_err("an unparseable file grace fails loudly")
    });
    assert_eq!(err.kind, EnvParseErrorKind::Parse);
    assert_eq!(err.exit_code(), ExitCode::InvalidUsage);
    let text = err.to_string();
    assert!(
        text.contains(&cfg) && text.contains("default") && text.contains("grace"),
        "the diagnostic points at the line to edit — file, profile, key: {text}"
    );
    assert!(
        !text.contains("environment variable"),
        "a file-sourced diagnostic must not tell the operator to look at an env var: {text}"
    );
}

/// A file value that is **out of range** (a headroom of `1.5` in `dagr.toml`)
/// exits `BootstrapFailure` naming the file, the profile, and the key —
/// observed at the process boundary, exit code and all.
#[test]
fn a_file_out_of_range_headroom_exits_bootstrap_failure_naming_the_file() {
    let base = TempBase::new("t116-file-range");
    let cfg = write_file(
        base.as_str(),
        "cfg.toml",
        "[default.pool]\nheadroom-fraction = 1.5\n",
    );
    let run = run_one_dag(
        &[],
        &["run", "--dagr.config", &cfg, "--store", base.as_str()],
    );
    assert_eq!(
        run.code,
        i32::from(ExitCode::BootstrapFailure.as_u8()),
        "an out-of-range file value is the machine's fault at bootstrap, output:\n{}",
        run.combined()
    );
    let text = run.combined();
    assert!(
        text.contains(&cfg) && text.contains("default") && text.contains("pool.headroom-fraction"),
        "the refusal names file, profile, and key: {text}"
    );
    assert!(
        !text.contains("environment variable"),
        "the refusal must not point at an environment variable: {text}"
    );
}

// ===========================================================================
// `DAGR_LOG_FORMAT` is strict
// ===========================================================================

/// `DAGR_LOG_FORMAT=humann` fails loudly at the process boundary, naming the
/// variable and the accepted values — it does **not** produce structured logs
/// silently. This fails today: the resolver maps anything unrecognized to
/// `structured`.
#[test]
fn a_bad_log_format_env_fails_loudly_naming_variable_and_accepted_values() {
    let base = TempBase::new("t116-strict-env");
    let run = run_one_dag(
        &[(LOG_FORMAT_ENV, "humann")],
        &["run", "--store", base.as_str()],
    );
    assert_eq!(
        run.code,
        i32::from(ExitCode::InvalidUsage.as_u8()),
        "an unrecognized log format is invalid usage, never a silent structured run, output:\n{}",
        run.combined()
    );
    let text = run.combined();
    assert!(
        text.contains(LOG_FORMAT_ENV) && text.contains("humann"),
        "the refusal names the variable and the rejected value: {text}"
    );
    assert!(
        text.contains("human") && text.contains("structured"),
        "the refusal lists the accepted values: {text}"
    );
}

/// `human` and `structured` each select their mode through the resolver.
#[test]
fn log_format_env_selects_each_mode() {
    let human = with_env(&[(LOG_FORMAT_ENV, "human")], || {
        resolve_log_format(None, None).expect("`human` is accepted")
    });
    assert_eq!(human, OutputMode::Human);
    let structured = with_env(&[(LOG_FORMAT_ENV, "structured")], || {
        resolve_log_format(None, None).expect("`structured` is accepted")
    });
    assert_eq!(structured, OutputMode::Structured);
}

/// Unset or empty still resolves to `structured` — strictness applies to a
/// *supplied* value, not to absence.
#[test]
fn log_format_unset_or_empty_is_structured() {
    let unset = with_env(&[], || {
        resolve_log_format(None, None).expect("unset is not an error")
    });
    assert_eq!(unset, OutputMode::Structured);
    let empty = with_env(&[(LOG_FORMAT_ENV, "")], || {
        resolve_log_format(None, None).expect("an empty variable is treated as unset")
    });
    assert_eq!(empty, OutputMode::Structured);
}

/// The flag wins over the environment — including over an env value that would
/// not even parse, because a present flag never reads the environment.
#[test]
fn log_format_flag_beats_env() {
    let resolved = with_env(&[(LOG_FORMAT_ENV, "structured")], || {
        resolve_log_format(Some(OutputMode::Human), None).expect("the flag path never errors")
    });
    assert_eq!(resolved, OutputMode::Human, "the flag wins outright");

    // Process-level: a garbage env value under a present flag is never parsed.
    let base = TempBase::new("t116-flag-beats-env");
    let run = run_one_dag(
        &[(LOG_FORMAT_ENV, "humann")],
        &[
            "run",
            "--dagr.log-format",
            "human",
            "--store",
            base.as_str(),
        ],
    );
    assert_eq!(
        run.code,
        0,
        "a present flag wins without reading the env, output:\n{}",
        run.combined()
    );
}

/// A `dagr.toml` `log-format` applies at the `file` tier and is beaten by both
/// the environment and the flag.
#[test]
fn log_format_file_tier_applies_and_is_beaten_by_env_and_flag() {
    let base = TempBase::new("t116-file-tier");
    let (_cfg, tier) = tier_from(&base, "[default]\nlog-format = \"human\"\n");

    let from_file = with_env(&[], || {
        resolve_log_format(None, tier.get("log-format")).expect("the file value applies")
    });
    assert_eq!(from_file, OutputMode::Human, "the file tier applies");

    let env_beats = with_env(&[(LOG_FORMAT_ENV, "structured")], || {
        resolve_log_format(None, tier.get("log-format")).expect("the env value applies")
    });
    assert_eq!(env_beats, OutputMode::Structured, "env beats the file");

    let flag_beats = with_env(&[(LOG_FORMAT_ENV, "structured")], || {
        resolve_log_format(Some(OutputMode::Human), tier.get("log-format"))
            .expect("the flag path never errors")
    });
    assert_eq!(flag_beats, OutputMode::Human, "the flag beats both");
}

/// A bad `log-format` in the file fails at the process boundary naming the
/// file, the profile, and the key — the run path resolves the key strictly.
#[test]
fn a_bad_log_format_file_value_fails_naming_file_profile_and_key() {
    let base = TempBase::new("t116-file-bad");
    let cfg = write_file(base.as_str(), "cfg.toml", "[default]\nlog-format = \"humann\"\n");
    let run = run_one_dag(
        &[],
        &["run", "--dagr.config", &cfg, "--store", base.as_str()],
    );
    assert_eq!(
        run.code,
        i32::from(ExitCode::InvalidUsage.as_u8()),
        "an unrecognized file log format is invalid usage, output:\n{}",
        run.combined()
    );
    let text = run.combined();
    assert!(
        text.contains(&cfg) && text.contains("default") && text.contains("log-format"),
        "the refusal names file, profile, and key: {text}"
    );
    assert!(
        !text.contains("environment variable"),
        "the refusal must not point at an environment variable: {text}"
    );
}

/// A bad `--dagr.log-format` value fails at the process boundary naming the
/// flag spelling — the flag tier exists and is strict too.
#[test]
fn a_bad_log_format_flag_value_fails_naming_the_flag() {
    let base = TempBase::new("t116-flag-bad");
    let run = run_one_dag(
        &[],
        &[
            "run",
            "--dagr.log-format",
            "humann",
            "--store",
            base.as_str(),
        ],
    );
    assert_eq!(
        run.code,
        i32::from(ExitCode::InvalidUsage.as_u8()),
        "an unrecognized flag log format is invalid usage, output:\n{}",
        run.combined()
    );
    let text = run.combined();
    assert!(
        text.contains(LOG_FORMAT_FLAG),
        "the refusal names the flag spelling: {text}"
    );
    assert!(
        !text.contains("environment variable"),
        "a flag refusal must not point at an environment variable: {text}"
    );
}

/// `dagr.log-format` is a reserved library flag: a pipeline parameter of that
/// name is a hard `LibraryFlagCollision`, and the reserved namespace lists it.
#[test]
fn log_format_flag_is_reserved_and_collides() {
    assert!(
        reserved_flag_names().contains(&"dagr.log-format"),
        "the reserved namespace lists dagr.log-format"
    );
    let err = check_reserved_collision(&[ParamSpec::new(
        "dagr.log-format",
        "a pipeline parameter trying to shadow the log-format knob",
    )])
    .expect_err("a pipeline parameter named dagr.log-format is a hard collision");
    assert!(
        err.to_string().contains("dagr.log-format"),
        "the collision names the flag: {err}"
    );
}

/// `--dagr.log-format` is in `flag_takes_value`: its value token is never
/// mistaken for the flow-name positional.
#[test]
fn log_format_flag_takes_a_value_before_the_flow_name() {
    let outcome = parse_cli(["dagr", "run", "--dagr.log-format", "human", "only"]);
    match outcome {
        ParseOutcome::Parsed(cli) => assert_eq!(
            cli.flow_name.as_deref(),
            Some("only"),
            "`human` is the flag's value, `only` is the flow name"
        ),
        other => panic!("expected a parsed run verb, got {other:?}"),
    }
}

/// The resolved mode is carried on `RunConfig` (the driver installs the
/// subscriber from it — no env read inside the driver), defaulting to
/// structured.
#[test]
fn run_config_carries_the_resolved_output_mode() {
    assert_eq!(
        RunConfig::new("b").output_mode(),
        OutputMode::Structured,
        "the zero-config default mode is structured, unchanged"
    );
    let human = with_env(&[], || {
        RunConfig::new("b")
            .log_format_from_env(Some(OutputMode::Human), None)
            .expect("the flag path never errors")
    });
    assert_eq!(human.output_mode(), OutputMode::Human);
}

// ===========================================================================
// Documentation is true again
// ===========================================================================

/// Read a repo-relative documentation file.
fn read_doc(rel: &str) -> String {
    let path = repo_root().join(rel);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("cannot read {rel}: {e}"))
}

/// The C26 knob table's rows (each `| --flag | ENV | default | type | … |`),
/// located by its header line.
fn c26_table_rows() -> Vec<Vec<String>> {
    let arch = read_doc("docs/arch.md");
    let header = "| Flag | Env var | Default | Type | Validation |";
    let start = arch
        .find(header)
        .expect("arch.md C26 carries the knob table with its documented header");
    arch[start..]
        .lines()
        .skip(2) // the header and the |---| separator
        .take_while(|l| l.trim_start().starts_with('|'))
        .map(|l| {
            l.trim()
                .trim_matches('|')
                .split('|')
                .map(|cell| cell.trim().to_string())
                .collect()
        })
        .collect()
}

/// arch.md no longer records an exception to the never-silent rule: the
/// exception sentence the M10 truth pass added is gone, and no knob row is
/// env-only (`*(none)*` in the flag column).
#[test]
fn arch_md_no_longer_records_a_strictness_exception() {
    let arch = read_doc("docs/arch.md");
    assert!(
        !arch.contains("the one knob outside the strict regime"),
        "the C26 table must no longer flag DAGR_LOG_FORMAT as outside the strict regime"
    );
    assert!(
        !arch.contains("It is the sole exception"),
        "the exception sentence the truth pass added must be removed"
    );
    assert!(
        !arch.contains("resolves an unrecognized value to `structured` deterministically"),
        "arch.md must not describe a silent fallback for DAGR_LOG_FORMAT"
    );
    for row in c26_table_rows() {
        assert!(
            !row[0].contains("(none)"),
            "every out-of-band knob has a reserved flag — no env-only row: {row:?}"
        );
    }
}

/// The C26 table's row count matches the number of resolved knobs, each row's
/// flag/env pair matching the shipped spelling — compared against the
/// constants, not retyped.
#[test]
fn the_c26_table_rows_match_the_resolved_knobs() {
    // The shipped knob set: every reserved flag with an out-of-band env
    // fallback the run path resolves. The two metastore spellings are private
    // to the feature-gated module, so they are pinned here by their documented
    // literals (exactly as the docs-claims suites pin private spellings).
    let expected: Vec<(String, String)> = vec![
        (STORE_FLAG.into(), DAGR_STORE.into()),
        (GRACE_FLAG.into(), DAGR_GRACE.into()),
        (TEARDOWN_DEADLINE_FLAG.into(), DAGR_TEARDOWN_DEADLINE.into()),
        (FAILURE_MODE_FLAG.into(), DAGR_FAILURE_MODE.into()),
        (
            POOL_COMPUTE_THREADS_FLAG.into(),
            DAGR_POOL_COMPUTE_THREADS.into(),
        ),
        (
            POOL_BLOCKING_THREADS_FLAG.into(),
            DAGR_POOL_BLOCKING_THREADS.into(),
        ),
        (POOL_MEMORY_FLAG.into(), DAGR_POOL_MEMORY.into()),
        (HEADROOM_FLAG.into(), DAGR_HEADROOM.into()),
        ("--dagr.metastore".into(), "DAGR_METASTORE".into()),
        (FORCE_ROUNDTRIP_FLAG.into(), DAGR_FORCE_ROUNDTRIP.into()),
        (EXECUTOR_FLAG.into(), DAGR_EXECUTOR.into()),
        (MAX_PODS_FLAG.into(), DAGR_MAX_PODS.into()),
        (NO_BANNER_FLAG.into(), NO_BANNER_ENV.into()),
        (LOG_FORMAT_FLAG.into(), LOG_FORMAT_ENV.into()),
    ];

    let rows = c26_table_rows();
    assert_eq!(
        rows.len(),
        expected.len(),
        "the C26 table has exactly one row per resolved knob: {rows:?}"
    );
    for (flag, env) in &expected {
        let row = rows
            .iter()
            .find(|r| r[0].contains(flag.as_str()))
            .unwrap_or_else(|| panic!("the C26 table has a row for {flag}"));
        assert!(
            row[1].contains(env.as_str()),
            "the {flag} row names its env fallback {env}: {row:?}"
        );
    }

    // The new row is full: flag, env, and the structured default.
    let log_row = rows
        .iter()
        .find(|r| r[0].contains(LOG_FORMAT_FLAG))
        .expect("the C26 table has a full --dagr.log-format row");
    assert!(
        log_row[2].contains("structured"),
        "the log-format row documents the structured default: {log_row:?}"
    );
}

// ===========================================================================
// No regression — the zero-configuration path is untouched
// ===========================================================================

/// With no configuration at all the run still succeeds cleanly. The
/// byte-identical event-stream guard lives in
/// `tests/wire_precedence_run_path.rs::empty_environment_streams_are_byte_identical`.
#[test]
fn a_clean_run_with_no_configuration_still_succeeds() {
    let base = TempBase::new("t116-clean-run");
    let run = run_one_dag(&[], &["run", "--store", base.as_str()]);
    assert_eq!(
        run.code,
        0,
        "the zero-config run path is unchanged, output:\n{}",
        run.combined()
    );
}
