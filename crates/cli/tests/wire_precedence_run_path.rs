//! **The runtime knobs actually work on the run path** — integration tests.
//! Written first, TDD.
//!
//! The precedence machinery (`flag > env > default`, the opt-in `RunConfig`
//! env-fallback builders, the pool-pin/headroom resolvers) has existed since the
//! env-fallback ticket shipped — and nothing on the shipped run path called it:
//! `dagr run <flow>` built `RunConfig::new(base).run_id(run_id)` and every
//! documented knob was silently ignored. This suite drives the **run path
//! itself** — the registry's `run <flow>` dispatch and the one-call
//! `RunnableFlow::run_to_store` — and asserts the knobs are honoured, bad values
//! fail loudly with the documented exit-code split, and an empty environment
//! changes **nothing** (byte-identical streams).
//!
//! # Two spawn styles, and why
//!
//! - **Subprocess tests** run the `one_dag` example (a real registry-routed leaf
//!   binary) via `cargo run --example`, setting `DAGR_*` **per-command** — the
//!   child gets exactly the environment the scenario states, no process-global
//!   mutation, fully parallel-safe. This is where stderr (the startup
//!   shutdown-budget line) and numeric exit codes are observable.
//! - **In-process tests** drive `run_registry_to` / `run_to_store` directly where
//!   the scenario needs a purpose-built flow (a failing node, a declared-cost
//!   node held pending by a pinned pool). These mutate the real `DAGR_*` names,
//!   so they hold the same process-global env lock the other env-touching suites
//!   use.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Mutex, MutexGuard, OnceLock};

use dagr_cli::contract::{ExitCode, ParseOutcome, parse_cli};
use dagr_cli::registry::{FlowRegistry, run_registry_to};
use dagr_cli::run_flow::RunnableFlow;
use dagr_cli::run_store::{FileSink, TickClock};
use dagr_core::TaskError;
use dagr_core::assembly::NodePolicy;
use dagr_core::context::RunContext;
use dagr_core::stable_name::StableName;
use dagr_core::task::Task;
use dagr_core::test_kit::TempBase;

/// Every `DAGR_*` name a run-path scenario touches, so the subprocess helper can
/// scrub inherited values and the in-process helper can guarantee a clean slate.
const RUN_PATH_ENV_NAMES: [&str; 8] = [
    "DAGR_GRACE",
    "DAGR_TEARDOWN_DEADLINE",
    "DAGR_FAILURE_MODE",
    "DAGR_POOL_COMPUTE_THREADS",
    "DAGR_POOL_BLOCKING_THREADS",
    "DAGR_POOL_MEMORY",
    "DAGR_HEADROOM",
    "DAGR_STORE",
];

// ===========================================================================
// Subprocess plumbing (mirrors tests/dag_auto_discovery.rs)
// ===========================================================================

/// The repo root, two levels above this crate's manifest — `cargo run --example`
/// is invoked from here so the child resolves the same workspace.
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
    /// Everything the example wrote to stderr (the startup shutdown-budget line).
    stderr: String,
}

impl Run {
    /// stdout and stderr concatenated — for diagnostics that may land on either.
    fn combined(&self) -> String {
        format!("{}{}", self.stdout, self.stderr)
    }
}

/// Run the compiled `one_dag` example (its sole flow is named `only`) with `args`,
/// after scrubbing every run-path `DAGR_*` name and setting exactly `envs` — so
/// the child observes the scenario's environment and nothing inherited.
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
    for name in RUN_PATH_ENV_NAMES {
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

/// The single run directory under `<base>/<flow>/`, or [`None`] when the flow
/// never wrote one.
fn sole_run_dir(base: &str, flow: &str) -> Option<PathBuf> {
    let flow_dir = Path::new(base).join(flow);
    let mut runs: Vec<PathBuf> = std::fs::read_dir(&flow_dir)
        .ok()?
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| p.is_dir())
        .collect();
    runs.sort();
    runs.pop()
}

/// The event stream of the sole run under `<base>/<flow>/`, read as a string.
fn sole_run_stream(base: &str, flow: &str) -> Option<String> {
    let dir = sole_run_dir(base, flow)?;
    std::fs::read_to_string(dir.join("events.jsonl")).ok()
}

// ===========================================================================
// The knobs actually work now — startup banner + budget arithmetic
// ===========================================================================

/// `DAGR_GRACE=30s` through a real `dagr run` reflects in the run's shutdown
/// budget — the startup banner prints the 30 s grace in its worst-case
/// arithmetic. This fails today: the run path never reads `DAGR_GRACE`.
#[test]
fn grace_env_reaches_the_shutdown_budget_banner() {
    let base = TempBase::new("t114-grace-env");
    let run = run_one_dag(
        &[("DAGR_GRACE", "30s")],
        &["run", "--store", base.as_str()],
    );
    assert_eq!(run.code, 0, "the run succeeds, output:\n{}", run.combined());
    assert!(
        run.stderr
            .contains("shutdown budget: grace 30s + teardown-deadline 15s"),
        "the startup banner reflects DAGR_GRACE=30s in the budget arithmetic, \
         stderr:\n{}",
        run.stderr
    );
}

/// `--grace 5s` beats `DAGR_GRACE=30s`: the flag wins outright and the env value
/// is not read. Fails today: neither tier reaches the run path.
#[test]
fn grace_flag_beats_env_on_the_run_path() {
    let base = TempBase::new("t114-grace-flag");
    let run = run_one_dag(
        &[("DAGR_GRACE", "30s")],
        &["run", "--grace", "5s", "--store", base.as_str()],
    );
    assert_eq!(run.code, 0, "the run succeeds, output:\n{}", run.combined());
    assert!(
        run.stderr
            .contains("shutdown budget: grace 5s + teardown-deadline 15s"),
        "--grace 5s wins over DAGR_GRACE=30s, stderr:\n{}",
        run.stderr
    );
}

/// `DAGR_TEARDOWN_DEADLINE` bounds the teardown phase — visible in the printed
/// worst-case budget the teardown deadline feeds. Fails today.
#[test]
fn teardown_deadline_env_bounds_the_budget() {
    let base = TempBase::new("t114-teardown");
    let run = run_one_dag(
        &[("DAGR_TEARDOWN_DEADLINE", "40s")],
        &["run", "--store", base.as_str()],
    );
    assert_eq!(run.code, 0, "the run succeeds, output:\n{}", run.combined());
    assert!(
        run.stderr
            .contains("shutdown budget: grace 10s + teardown-deadline 40s"),
        "the teardown phase is bounded by DAGR_TEARDOWN_DEADLINE=40s, stderr:\n{}",
        run.stderr
    );
}

// ===========================================================================
// Loud failures — named variable, correct exit code, nothing left behind
// ===========================================================================

/// `DAGR_GRACE=nonsense` exits `InvalidUsage` naming the variable and its value —
/// never a default silently applied. Fails today: the value is ignored and the
/// run succeeds.
#[test]
fn unparseable_grace_env_is_invalid_usage_naming_the_variable() {
    let base = TempBase::new("t114-bad-grace");
    let run = run_one_dag(
        &[("DAGR_GRACE", "nonsense")],
        &["run", "--store", base.as_str()],
    );
    assert_eq!(
        run.code,
        i32::from(ExitCode::InvalidUsage.as_u8()),
        "an unparseable DAGR_GRACE is invalid usage, output:\n{}",
        run.combined()
    );
    let combined = run.combined();
    assert!(
        combined.contains("DAGR_GRACE") && combined.contains("nonsense"),
        "the diagnostic names DAGR_GRACE and the rejected value, got:\n{combined}"
    );
    assert!(
        !Path::new(base.as_str()).join("only").exists(),
        "a run that failed resolution leaves no run directory behind"
    );
}

/// `DAGR_HEADROOM=1.5` exits `BootstrapFailure` naming the variable — an
/// out-of-range value is the machine's fault at bootstrap, distinct from a parse
/// failure. Fails today.
#[test]
fn out_of_range_headroom_env_is_bootstrap_failure() {
    let base = TempBase::new("t114-bad-headroom");
    let run = run_one_dag(
        &[("DAGR_HEADROOM", "1.5")],
        &["run", "--store", base.as_str()],
    );
    assert_eq!(
        run.code,
        i32::from(ExitCode::BootstrapFailure.as_u8()),
        "an out-of-range DAGR_HEADROOM is a bootstrap failure, output:\n{}",
        run.combined()
    );
    assert!(
        run.combined().contains("DAGR_HEADROOM"),
        "the diagnostic names DAGR_HEADROOM, got:\n{}",
        run.combined()
    );
}

/// The settled duration bounds: grace `≥ 1 ms` and teardown-deadline `≥ 1 s` are
/// enforced as out-of-range **bootstrap failures** naming the variable and the
/// violated bound (the tables' documented validations, made true). Fails today:
/// `0` is silently accepted.
#[test]
fn zero_durations_violate_the_documented_bounds() {
    let base = TempBase::new("t114-zero-grace");
    let run = run_one_dag(&[("DAGR_GRACE", "0")], &["run", "--store", base.as_str()]);
    assert_eq!(
        run.code,
        i32::from(ExitCode::BootstrapFailure.as_u8()),
        "DAGR_GRACE=0 violates the >= 1 ms bound, output:\n{}",
        run.combined()
    );
    let combined = run.combined();
    assert!(
        combined.contains("DAGR_GRACE") && combined.contains("1 ms"),
        "the diagnostic names DAGR_GRACE and the 1 ms bound, got:\n{combined}"
    );

    let base = TempBase::new("t114-zero-teardown");
    let run = run_one_dag(
        &[("DAGR_TEARDOWN_DEADLINE", "0")],
        &["run", "--store", base.as_str()],
    );
    assert_eq!(
        run.code,
        i32::from(ExitCode::BootstrapFailure.as_u8()),
        "DAGR_TEARDOWN_DEADLINE=0 violates the >= 1 s bound, output:\n{}",
        run.combined()
    );
    let combined = run.combined();
    assert!(
        combined.contains("DAGR_TEARDOWN_DEADLINE") && combined.contains("1 s"),
        "the diagnostic names DAGR_TEARDOWN_DEADLINE and the 1 s bound, got:\n{combined}"
    );
}

// ===========================================================================
// DAGR_STORE — the one documented-but-missing variable this ticket adds
// ===========================================================================

/// `DAGR_STORE=<dir>` relocates the run store; `--store` still beats it. Fails
/// today: `DAGR_STORE` does not exist anywhere.
#[test]
fn dagr_store_env_is_the_store_fallback_and_the_flag_wins() {
    // Env only: the store is created under DAGR_STORE.
    let holder = TempBase::new("t114-store-env");
    let elsewhere = format!("{}/elsewhere", holder.as_str());
    let run = run_one_dag(&[("DAGR_STORE", &elsewhere)], &["run"]);
    assert_eq!(run.code, 0, "the run succeeds, output:\n{}", run.combined());
    assert!(
        sole_run_stream(&elsewhere, "only").is_some(),
        "the run store is created under DAGR_STORE ({elsewhere}/only/<run-id>/events.jsonl)"
    );

    // Flag and env: the flag wins and the env directory stays untouched.
    let holder = TempBase::new("t114-store-flag");
    let ignored = format!("{}/ignored", holder.as_str());
    let flagged = format!("{}/flagged", holder.as_str());
    let run = run_one_dag(&[("DAGR_STORE", &ignored)], &["run", "--store", &flagged]);
    assert_eq!(run.code, 0, "the run succeeds, output:\n{}", run.combined());
    assert!(
        sole_run_stream(&flagged, "only").is_some(),
        "--store wins: the stream lives under the flag's directory"
    );
    assert!(
        !Path::new(&ignored).exists(),
        "the DAGR_STORE directory is untouched when --store wins"
    );
}

// ===========================================================================
// The parsing bug — `--dagr.metastore-store` takes a value
// ===========================================================================

/// `dagr run --dagr.metastore-store ./x.db <flow>` selects `<flow>`; the flag's
/// path value is never mistaken for the flow name. Fails today:
/// `flag_takes_value` omits `dagr.metastore-store`, so `./x.db` is taken as the
/// first positional.
#[test]
fn metastore_store_value_is_not_mistaken_for_the_flow_name() {
    let outcome = parse_cli(["dagr", "run", "--dagr.metastore-store", "./x.db", "etl"]);
    match outcome {
        ParseOutcome::Parsed(cli) => assert_eq!(
            cli.flow_name.as_deref(),
            Some("etl"),
            "the flow name is `etl`, not the flag's path value"
        ),
        other => panic!("the invocation parses to its verb, got {other:?}"),
    }
}

// ===========================================================================
// In-process env plumbing (the same lock discipline as the other env suites)
// ===========================================================================

/// The process-global lock every in-process env-mutating test takes.
fn env_lock() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Clear every run-path `DAGR_*` name, set `pairs`, run `body`, then clear again
/// — all under the env lock.
#[allow(
    unsafe_code,
    reason = "std::env::set_var/remove_var are unsafe fns in edition 2024; the run \
              path under test reads the REAL process environment by name, so a test \
              cannot avoid mutating it — this helper is the only place this suite \
              does, and it holds the env lock for the whole window"
)]
fn with_env<T>(pairs: &[(&str, &str)], body: impl FnOnce() -> T) -> T {
    let _guard = env_lock();
    for k in RUN_PATH_ENV_NAMES {
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
    for k in RUN_PATH_ENV_NAMES {
        // SAFETY: as above — still inside the `_guard` window.
        unsafe { std::env::remove_var(k) };
    }
    out
}

// ===========================================================================
// Failure mode + memory pin — terminal states from a real registry run
// ===========================================================================

/// A value type with a stable name, so policy-carrying registrations stay
/// graph-emittable.
#[derive(Clone)]
struct Unit;
impl StableName for Unit {
    const STABLE_NAME: &'static str = "T114Unit";
}

/// Fails immediately — the first failure stop-on-first-failure reacts to.
struct Boom;
impl StableName for Boom {
    const STABLE_NAME: &'static str = "T114Boom";
}
impl Task for Boom {
    type Input = ();
    type Output = Unit;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Unit, TaskError> {
        Err(TaskError::permanent("induced failure"))
    }
}

/// Holds its (whole-pool) memory permit until the run token flips, then returns —
/// keeping the sibling deterministically pending across the pre-stop window.
/// Bounded spin, no sleeps.
struct HoldsPoolUntilCancelled;
impl StableName for HoldsPoolUntilCancelled {
    const STABLE_NAME: &'static str = "T114Holder";
}
impl Task for HoldsPoolUntilCancelled {
    type Input = ();
    type Output = Unit;
    async fn run(&mut self, c: &RunContext, _i: ()) -> Result<Unit, TaskError> {
        for _ in 0..1_000_000 {
            if c.cancellation().is_cancelled() {
                return Ok(Unit);
            }
            tokio::task::yield_now().await;
        }
        Ok(Unit)
    }
}

/// Would succeed instantly — but with the memory pool pinned to one node's worth
/// it stays pending behind the holder, so the stop settles it `cancelled`.
struct WouldSucceed;
impl StableName for WouldSucceed {
    const STABLE_NAME: &'static str = "T114WouldSucceed";
}
impl Task for WouldSucceed {
    type Input = ();
    type Output = Unit;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Unit, TaskError> {
        Ok(Unit)
    }
}

/// The declared whole-pool memory cost the pin scenario pivots on.
const POOL: u64 = 1024;

/// Build the pin/stop scenario flow: the holder fills the pinned pool, the gated
/// sibling waits on it, and the failing node triggers the stop.
fn build_pin_stop_flow() -> RunnableFlow {
    let mut flow = RunnableFlow::new();
    let _hold = flow.register_source_named_with(
        "a-holder",
        HoldsPoolUntilCancelled,
        NodePolicy::new().working_memory(POOL),
    );
    let _gated = flow.register_source_named_with(
        "b-gated",
        WouldSucceed,
        NodePolicy::new().working_memory(POOL),
    );
    let _boom = flow.register_source_named("c-boom", Boom);
    flow
}

/// `DAGR_FAILURE_MODE=stop-on-first-failure` stops the run — the failure settles
/// the pending sibling `cancelled` — and `DAGR_POOL_MEMORY` is the admission
/// ledger's capacity (the sibling was pending *because* the pool held exactly one
/// declared cost). Fails today on both counts: the env is ignored, the pool is
/// unconstrained, the sibling is admitted immediately and succeeds.
#[test]
fn failure_mode_and_memory_pin_env_reach_a_registry_run() {
    let base = TempBase::new("t114-stop");
    let registry = FlowRegistry::new().add("wired", build_pin_stop_flow);
    let mut out = Vec::new();
    let exit = with_env(
        &[
            ("DAGR_FAILURE_MODE", "stop-on-first-failure"),
            ("DAGR_POOL_MEMORY", &POOL.to_string()),
        ],
        || {
            run_registry_to(
                &registry,
                ["dagr", "run", "wired", "--store", base.as_str()],
                &mut out,
            )
        },
    );
    assert_eq!(
        exit,
        ExitCode::RunFailure,
        "the failing node is a run failure (never masked by the consequent stop), \
         diagnostics:\n{}",
        String::from_utf8_lossy(&out)
    );
    let stream = sole_run_stream(base.as_str(), "wired")
        .expect("the run wrote an event stream under the store");
    let terminal = |node: &str, state: &str| {
        stream.lines().any(|l| {
            l.contains("node-terminal") && l.contains(node) && l.contains(state)
        })
    };
    assert!(
        terminal("c-boom", "failed"),
        "the failing node ends failed, stream:\n{stream}"
    );
    assert!(
        terminal("b-gated", "cancelled"),
        "stop-on-first-failure settles the pending sibling cancelled — which also \
         proves DAGR_POOL_MEMORY reached the admission ledger (the sibling was \
         pending only because the pool held exactly one declared cost), stream:\n{stream}"
    );
}

// ===========================================================================
// run_to_store — the one-call path resolves the env tier too
// ===========================================================================

/// A bad `DAGR_GRACE` fails `run_to_store` loudly, naming the variable — the
/// one-call golden path resolves the same env tier the registry does. Fails
/// today: the env is never read and the run succeeds.
#[test]
fn run_to_store_resolves_the_env_tier() {
    let base = TempBase::new("t114-rts");
    let mut flow = RunnableFlow::new();
    let _h = flow.register_source_named("solo", WouldSucceed);
    let err = with_env(&[("DAGR_GRACE", "not-a-duration")], || {
        flow.run_to_store("t114-rts", base.as_str())
            .err()
            .map(|e| e.to_string())
    });
    let err = err.expect("a bad DAGR_GRACE fails run_to_store loudly");
    assert!(
        err.contains("DAGR_GRACE"),
        "the error names the offending variable, got: {err}"
    );
}

// ===========================================================================
// No behaviour change when nothing is set — byte-identical streams
// ===========================================================================

/// Two graph-emittable sources, registered in a fixed order — the
/// byte-identical fixture.
fn build_two_source_flow() -> RunnableFlow {
    let mut flow = RunnableFlow::new();
    let _a = flow.register_source_named("alpha", WouldSucceed);
    let _b = flow.register_source_named("beta", WouldSucceed);
    flow
}

/// Scrub the one wall-varying field (`"wall":"…"`) out of a stream so two runs
/// of the same pipeline compare byte-for-byte on everything the run *did*. The
/// wall stamp differs between any two runs — before this change as much as after
/// — so it is outside what "no behaviour change" can mean.
fn scrub_wall(stream: &[u8]) -> String {
    let text = String::from_utf8_lossy(stream);
    let mut out = String::with_capacity(text.len());
    let mut rest = text.as_ref();
    while let Some(start) = rest.find("\"wall\":\"") {
        let value_start = start + "\"wall\":\"".len();
        out.push_str(&rest[..value_start]);
        out.push_str("<wall>");
        let Some(end) = rest[value_start..].find('"') else {
            rest = "";
            break;
        };
        rest = &rest[value_start + end..];
    }
    out.push_str(rest);
    out
}

/// With no flags and an empty environment, a registry run's event stream is
/// **byte-identical** (outside the wall stamp, which varies between any two runs)
/// to one driven through the historical minimal construction
/// (`RunConfig::new(base).run_id(id)`), run-id for run-id. A guard: green before
/// this change, and it must stay green after the wiring lands.
#[test]
fn empty_environment_streams_are_byte_identical() {
    use dagr_cli::driver::RunConfig;

    // The wired path: a registry run with nothing set.
    let base_wired = TempBase::new("t114-ident-wired");
    let registry = FlowRegistry::new().add("ident", build_two_source_flow);
    let mut out = Vec::new();
    let exit = with_env(&[], || {
        run_registry_to(
            &registry,
            ["dagr", "run", "ident", "--store", base_wired.as_str()],
            &mut out,
        )
    });
    assert_eq!(exit, ExitCode::Success, "the wired run succeeds");
    let run_dir = sole_run_dir(base_wired.as_str(), "ident").expect("the wired run wrote a store");
    let wired = std::fs::read(run_dir.join("events.jsonl")).expect("wired stream reads");
    let run_id = run_dir
        .file_name()
        .and_then(|n| n.to_str())
        .expect("the run directory is the run id")
        .to_string();

    // The reference path: the historical minimal construction, same flow, same
    // run id, same deterministic clock.
    let base_ref = TempBase::new("t114-ident-ref");
    let stream_path = Path::new(base_ref.as_str())
        .join("ident")
        .join(&run_id)
        .join("events.jsonl");
    let sink = FileSink::create(&stream_path).expect("reference sink opens");
    let config = RunConfig::new(base_ref.as_str()).run_id(&run_id);
    let report = build_two_source_flow()
        .run("ident", &config, sink, TickClock::default())
        .expect("the reference flow assembles");
    assert_eq!(
        report.outcome(),
        dagr_artifact::event_stream::RunOutcome::Succeeded,
        "the reference run succeeds — so identical bytes mean matching terminal states"
    );

    let reference = std::fs::read(&stream_path).expect("reference stream reads");
    assert_eq!(
        scrub_wall(&wired),
        scrub_wall(&reference),
        "with nothing set, the wired run path's stream is byte-identical to the \
         historical minimal construction (wall stamps scrubbed — they vary \
         between any two runs)"
    );
}
