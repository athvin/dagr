//! **The `dagr.toml` loader, profile layering, and the `file` precedence tier**
//! (ADR 128) — integration tests. Written first, TDD.
//!
//! ADR 128 §2 decided a fourth precedence tier — `flag > env > file(profile) >
//! default` — and this suite drives it end to end: discovery (`--dagr.config`
//! explicit path > `./dagr.toml` > none), profile selection (`--dagr.profile` >
//! `DAGR_PROFILE` > `default`) with key-by-key layering over `[default]`, the
//! tri-state of the pool pins (a file that mentions no pool leaves it
//! **detected**, never defaulted), loud bootstrap failures naming the file, the
//! profile, and the key, and the two properties that can be silently broken:
//! the assembly verbs never read the file, and a run with no file is
//! byte-identical to one with an empty `[default]`.
//!
//! # Two spawn styles, and why (mirrors `tests/wire_precedence_run_path.rs`)
//!
//! - **Subprocess tests** run the `one_dag` example (a real registry-routed leaf
//!   binary) via `cargo run --example`, setting `DAGR_*` per-command and — for
//!   the discovery-by-cwd scenarios — pointing the child's working directory at
//!   a temp dir holding a `dagr.toml` (`--manifest-path` keeps cargo resolving
//!   the same workspace). The startup shutdown-budget line on stderr is where a
//!   resolved grace/teardown value is observable.
//! - **In-process tests** drive `run_registry_to` / the `config_file` seams
//!   directly where the scenario needs a purpose-built flow or a deterministic
//!   probe. These scrub the real `DAGR_*` names, so they hold a process-global
//!   env lock exactly as the other env-touching suites do.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Mutex, MutexGuard, OnceLock};

use dagr_cli::config_file::{
    CONFIG_FILE_NAME, DAGR_PROFILE, DEFAULT_PROFILE, FileTier, file_keys, load_file_tier_from,
};
use dagr_cli::contract::{ExitCode, ParseOutcome, parse_cli, reserved_flag_names};
use dagr_cli::registry::{FlowRegistry, run_registry_to};
use dagr_cli::run_flow::RunnableFlow;
use dagr_core::TaskError;
use dagr_core::context::RunContext;
use dagr_core::stable_name::StableName;
use dagr_core::task::Task;
use dagr_core::test_kit::TempBase;

/// Every `DAGR_*` name a file-tier scenario could observe, so the subprocess
/// helper can scrub inherited values and the in-process helper can guarantee a
/// clean slate.
const FILE_TIER_ENV_NAMES: [&str; 13] = [
    "DAGR_GRACE",
    "DAGR_TEARDOWN_DEADLINE",
    "DAGR_FAILURE_MODE",
    "DAGR_POOL_COMPUTE_THREADS",
    "DAGR_POOL_BLOCKING_THREADS",
    "DAGR_POOL_MEMORY",
    "DAGR_HEADROOM",
    "DAGR_STORE",
    "DAGR_PROFILE",
    "DAGR_EXECUTOR",
    "DAGR_MAX_PODS",
    "DAGR_FORCE_ROUNDTRIP",
    "DAGR_METASTORE",
];

// ===========================================================================
// Subprocess plumbing (mirrors tests/wire_precedence_run_path.rs)
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
    /// Everything the example wrote to stderr (the startup shutdown-budget line).
    stderr: String,
}

impl Run {
    /// stdout and stderr concatenated — for diagnostics that may land on either.
    fn combined(&self) -> String {
        format!("{}{}", self.stdout, self.stderr)
    }
}

/// Run the compiled `one_dag` example (its sole flow is named `only`) with
/// `args` from the working directory `cwd`, after scrubbing every file-tier
/// `DAGR_*` name and setting exactly `envs`. `--manifest-path` pins the
/// workspace, so `cwd` is free to be a temp dir holding a `dagr.toml` — which
/// is exactly how the discovery-by-cwd scenarios stay hermetic and parallel.
fn run_one_dag_in(cwd: &Path, envs: &[(&str, &str)], args: &[&str]) -> Run {
    let manifest = repo_root().join("Cargo.toml");
    let mut cmd = Command::new(env!("CARGO"));
    cmd.current_dir(cwd).args([
        "run",
        "--quiet",
        "--manifest-path",
        manifest.to_str().expect("the repo root path is UTF-8"),
        "-p",
        "dagr-cli",
        "--example",
        "one_dag",
        "--",
    ]);
    cmd.args(args);
    for name in FILE_TIER_ENV_NAMES {
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

/// [`run_one_dag_in`] from the repo root — for scenarios that do not need a
/// `dagr.toml` in the working directory (the repo root has none, by policy).
fn run_one_dag(envs: &[(&str, &str)], args: &[&str]) -> Run {
    run_one_dag_in(&repo_root(), envs, args)
}

/// Write `content` to `<dir>/<name>` and return the absolute path as a string.
fn write_file(dir: &str, name: &str, content: &str) -> String {
    let path = Path::new(dir).join(name);
    std::fs::write(&path, content).expect("test config file writes");
    path.to_str().expect("temp paths are UTF-8").to_string()
}

/// The single run directory under `<base>/<flow>/`, or [`None`].
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

// ===========================================================================
// Precedence — all four tiers, observable in the shutdown-budget banner
// ===========================================================================

/// A file setting grace, and nothing else set, reaches the run: the startup
/// banner prints the file's 25 s grace. Fails today: no loader exists.
#[test]
fn file_grace_reaches_the_run() {
    let base = TempBase::new("t115-file-grace");
    let cfg = write_file(base.as_str(), "cfg.toml", "[default]\ngrace = \"25s\"\n");
    let run = run_one_dag(
        &[],
        &["run", "--dagr.config", &cfg, "--store", base.as_str()],
    );
    assert_eq!(run.code, 0, "the run succeeds, output:\n{}", run.combined());
    assert!(
        run.stderr
            .contains("shutdown budget: grace 25s + teardown-deadline 15s"),
        "the file's grace reaches the budget arithmetic, stderr:\n{}",
        run.stderr
    );
}

/// The environment beats the file: `DAGR_GRACE=30s` wins over the file's 25 s.
#[test]
fn env_beats_file_for_grace() {
    let base = TempBase::new("t115-env-beats-file");
    let cfg = write_file(base.as_str(), "cfg.toml", "[default]\ngrace = \"25s\"\n");
    let run = run_one_dag(
        &[("DAGR_GRACE", "30s")],
        &["run", "--dagr.config", &cfg, "--store", base.as_str()],
    );
    assert_eq!(run.code, 0, "the run succeeds, output:\n{}", run.combined());
    assert!(
        run.stderr
            .contains("shutdown budget: grace 30s + teardown-deadline 15s"),
        "DAGR_GRACE=30s beats the file's 25s, stderr:\n{}",
        run.stderr
    );
}

/// The flag beats both: `--grace 5s` wins over `DAGR_GRACE=30s` and the file.
#[test]
fn flag_beats_env_and_file_for_grace() {
    let base = TempBase::new("t115-flag-beats-all");
    let cfg = write_file(base.as_str(), "cfg.toml", "[default]\ngrace = \"25s\"\n");
    let run = run_one_dag(
        &[("DAGR_GRACE", "30s")],
        &[
            "run",
            "--grace",
            "5s",
            "--dagr.config",
            &cfg,
            "--store",
            base.as_str(),
        ],
    );
    assert_eq!(run.code, 0, "the run succeeds, output:\n{}", run.combined());
    assert!(
        run.stderr
            .contains("shutdown budget: grace 5s + teardown-deadline 15s"),
        "--grace 5s beats DAGR_GRACE and the file, stderr:\n{}",
        run.stderr
    );
}

// ===========================================================================
// Profiles — layering, loud unknown-name failure, flag > env selection
// ===========================================================================

/// The layered profile file every profile scenario shares: `[default]` sets two
/// knobs; `[prod]` overrides one; `[dev]` overrides the same one differently.
const LAYERED: &str = "\
[default]
grace = \"25s\"
teardown-deadline = \"40s\"

[prod]
grace = \"7s\"

[dev]
grace = \"9s\"
";

/// `--dagr.profile prod` yields prod's grace and default's teardown deadline —
/// layering key by key, not table replacement.
#[test]
fn named_profile_layers_over_default() {
    let base = TempBase::new("t115-layering");
    let cfg = write_file(base.as_str(), "cfg.toml", LAYERED);
    let run = run_one_dag(
        &[],
        &[
            "run",
            "--dagr.config",
            &cfg,
            "--dagr.profile",
            "prod",
            "--store",
            base.as_str(),
        ],
    );
    assert_eq!(run.code, 0, "the run succeeds, output:\n{}", run.combined());
    assert!(
        run.stderr
            .contains("shutdown budget: grace 7s + teardown-deadline 40s"),
        "prod's grace overrides default's; default's teardown-deadline still \
         applies (layering, not replacement), stderr:\n{}",
        run.stderr
    );
}

/// An unknown profile is a loud failure naming the profile and listing the
/// profiles the file defines — never a silent fallback to `default`.
#[test]
fn unknown_profile_fails_loudly_listing_profiles() {
    let base = TempBase::new("t115-unknown-profile");
    let cfg = write_file(base.as_str(), "cfg.toml", LAYERED);
    let run = run_one_dag(
        &[],
        &[
            "run",
            "--dagr.config",
            &cfg,
            "--dagr.profile",
            "nope",
            "--store",
            base.as_str(),
        ],
    );
    assert_eq!(
        run.code,
        i32::from(ExitCode::InvalidUsage.as_u8()),
        "an unknown profile is invalid usage, output:\n{}",
        run.combined()
    );
    let combined = run.combined();
    assert!(
        combined.contains("nope"),
        "the diagnostic names the unknown profile, got:\n{combined}"
    );
    assert!(
        combined.contains("prod") && combined.contains("dev"),
        "the diagnostic lists the profiles the file defines, got:\n{combined}"
    );
    assert!(
        !Path::new(base.as_str()).join("only").exists(),
        "a run refused at bootstrap leaves no run directory behind"
    );
}

/// `--dagr.profile dev` beats `DAGR_PROFILE=prod` — the flag wins.
#[test]
fn profile_flag_beats_dagr_profile_env() {
    let base = TempBase::new("t115-profile-flag-wins");
    let cfg = write_file(base.as_str(), "cfg.toml", LAYERED);
    let run = run_one_dag(
        &[("DAGR_PROFILE", "prod")],
        &[
            "run",
            "--dagr.config",
            &cfg,
            "--dagr.profile",
            "dev",
            "--store",
            base.as_str(),
        ],
    );
    assert_eq!(run.code, 0, "the run succeeds, output:\n{}", run.combined());
    assert!(
        run.stderr
            .contains("shutdown budget: grace 9s + teardown-deadline 40s"),
        "--dagr.profile dev (grace 9s) beats DAGR_PROFILE=prod (grace 7s), \
         stderr:\n{}",
        run.stderr
    );
}

/// `DAGR_PROFILE=prod` selects the profile when no flag is given.
#[test]
fn dagr_profile_env_selects_when_no_flag() {
    let base = TempBase::new("t115-profile-env");
    let cfg = write_file(base.as_str(), "cfg.toml", LAYERED);
    let run = run_one_dag(
        &[("DAGR_PROFILE", "prod")],
        &["run", "--dagr.config", &cfg, "--store", base.as_str()],
    );
    assert_eq!(run.code, 0, "the run succeeds, output:\n{}", run.combined());
    assert!(
        run.stderr
            .contains("shutdown budget: grace 7s + teardown-deadline 40s"),
        "DAGR_PROFILE=prod selects prod when no flag is given, stderr:\n{}",
        run.stderr
    );
}

/// A file with only `[default]` and no profile selected applies as-is.
#[test]
fn default_profile_applies_with_no_selection() {
    let base = TempBase::new("t115-default-only");
    let cfg = write_file(
        base.as_str(),
        "cfg.toml",
        "[default]\nteardown-deadline = \"33s\"\n",
    );
    let run = run_one_dag(
        &[],
        &["run", "--dagr.config", &cfg, "--store", base.as_str()],
    );
    assert_eq!(run.code, 0, "the run succeeds, output:\n{}", run.combined());
    assert!(
        run.stderr
            .contains("shutdown budget: grace 10s + teardown-deadline 33s"),
        "the [default] table alone applies with no profile selected, stderr:\n{}",
        run.stderr
    );
}

/// Selecting a profile when no file was found is a loud failure, not silently
/// inert: the operator asked for `prod`'s settings and did not get them.
#[test]
fn selected_profile_with_no_file_is_a_loud_error() {
    let base = TempBase::new("t115-profile-no-file");
    // No --dagr.config and no ./dagr.toml at the repo root (by policy).
    let run = run_one_dag(
        &[],
        &["run", "--dagr.profile", "prod", "--store", base.as_str()],
    );
    assert_eq!(
        run.code,
        i32::from(ExitCode::InvalidUsage.as_u8()),
        "a selected profile with no file is invalid usage, output:\n{}",
        run.combined()
    );
    let combined = run.combined();
    assert!(
        combined.contains("prod"),
        "the diagnostic names the selected profile, got:\n{combined}"
    );
}

// ===========================================================================
// Discovery — explicit path > ./dagr.toml > none
// ===========================================================================

/// A missing explicit `--dagr.config` path is a hard error naming the path —
/// never a silent fallback to discovery.
#[test]
fn missing_explicit_config_path_is_a_hard_error() {
    let base = TempBase::new("t115-missing-explicit");
    let missing = format!("{}/does-not-exist.toml", base.as_str());
    let run = run_one_dag(
        &[],
        &["run", "--dagr.config", &missing, "--store", base.as_str()],
    );
    assert_eq!(
        run.code,
        i32::from(ExitCode::InvalidUsage.as_u8()),
        "a missing explicit config path is invalid usage, output:\n{}",
        run.combined()
    );
    assert!(
        run.combined().contains("does-not-exist.toml"),
        "the diagnostic names the missing path, got:\n{}",
        run.combined()
    );
    assert!(
        !Path::new(base.as_str()).join("only").exists(),
        "a run refused at bootstrap leaves no run directory behind"
    );
}

/// A `./dagr.toml` in the invocation's working directory is discovered.
#[test]
fn cwd_dagr_toml_is_discovered() {
    let base = TempBase::new("t115-cwd-discovery");
    let cwd = Path::new(base.as_str()).join("workdir");
    std::fs::create_dir_all(&cwd).expect("create the child working dir");
    std::fs::write(cwd.join(CONFIG_FILE_NAME), "[default]\ngrace = \"25s\"\n")
        .expect("write ./dagr.toml");
    let run = run_one_dag_in(&cwd, &[], &["run", "--store", base.as_str()]);
    assert_eq!(run.code, 0, "the run succeeds, output:\n{}", run.combined());
    assert!(
        run.stderr
            .contains("shutdown budget: grace 25s + teardown-deadline 15s"),
        "./dagr.toml in the working directory is discovered, stderr:\n{}",
        run.stderr
    );
}

/// An explicit `--dagr.config` beats a `./dagr.toml` that would also match.
#[test]
fn explicit_config_path_beats_cwd_discovery() {
    let base = TempBase::new("t115-explicit-beats-cwd");
    let cwd = Path::new(base.as_str()).join("workdir");
    std::fs::create_dir_all(&cwd).expect("create the child working dir");
    std::fs::write(cwd.join(CONFIG_FILE_NAME), "[default]\ngrace = \"25s\"\n")
        .expect("write ./dagr.toml");
    let explicit = write_file(
        base.as_str(),
        "explicit.toml",
        "[default]\ngrace = \"8s\"\n",
    );
    let run = run_one_dag_in(
        &cwd,
        &[],
        &["run", "--dagr.config", &explicit, "--store", base.as_str()],
    );
    assert_eq!(run.code, 0, "the run succeeds, output:\n{}", run.combined());
    assert!(
        run.stderr
            .contains("shutdown budget: grace 8s + teardown-deadline 15s"),
        "the explicit path (grace 8s) beats ./dagr.toml (grace 25s), stderr:\n{}",
        run.stderr
    );
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

/// Clear every file-tier `DAGR_*` name, set `pairs`, run `body`, then clear
/// again — all under the env lock.
#[allow(
    unsafe_code,
    reason = "std::env::set_var/remove_var are unsafe fns in edition 2024; the \
              loader and resolvers under test read the REAL process environment \
              by name, so a test cannot avoid mutating it — this helper is the \
              only place this suite does, and it holds the env lock for the \
              whole window"
)]
fn with_env<T>(pairs: &[(&str, &str)], body: impl FnOnce() -> T) -> T {
    let _guard = env_lock();
    for k in FILE_TIER_ENV_NAMES {
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
    for k in FILE_TIER_ENV_NAMES {
        // SAFETY: as above — still inside the `_guard` window.
        unsafe { std::env::remove_var(k) };
    }
    out
}

// ===========================================================================
// The tri-state — a file pin pins; an unmentioned pool stays DETECTED
// ===========================================================================

/// Load a [`FileTier`] from `content` written to a temp file, with an explicit
/// profile so the loader never consults `DAGR_PROFILE`.
fn tier_from(base: &TempBase, content: &str, profile: Option<&str>) -> FileTier {
    let cfg = write_file(base.as_str(), "seam.toml", content);
    load_file_tier_from(Path::new(base.as_str()), Some(&cfg), profile)
        .expect("the seam fixture loads")
}

/// A file that sets `pool.memory` but no thread pool pins the memory pool
/// verbatim and leaves both thread pools **detected** — the `resolve_opt`
/// tri-state, preserved through the file tier.
#[test]
fn file_memory_pin_leaves_thread_pools_detected() {
    use dagr_cli::config::{PoolPinFlags, resolve_pool_sizing};
    use dagr_core::limits::ContainerLimitProbe;

    let base = TempBase::new("t115-tri-state");
    let tier = tier_from(&base, "[default.pool]\nmemory = 2048\n", None);
    let sizing = with_env(&[], || {
        resolve_pool_sizing(PoolPinFlags::default(), None, &tier)
    })
    .expect("a file memory pin resolves");
    assert!(sizing.engaged(), "a file-supplied pin engages the probe");
    let caps = sizing
        .capacities(ContainerLimitProbe::from_root("/nonexistent-root-for-t115").with_host_cores(4))
        .expect("sizing never fails")
        .expect("engaged sizing yields capacities");
    assert_eq!(
        caps.total(dagr_core::admission::Pool::Memory),
        2048,
        "the file-pinned pool is the pin verbatim"
    );
    assert_eq!(
        caps.total(dagr_core::admission::Pool::ComputeThreads),
        3,
        "an unmentioned pool is DETECTED (4 cores at the 20% default headroom), \
         never defaulted"
    );
}

/// A file that sets no pools at all leaves admission exactly as a run with no
/// file: sizing is disengaged and the probe is never consulted.
#[test]
fn file_with_no_pools_leaves_admission_disengaged() {
    use dagr_cli::config::{PoolPinFlags, resolve_pool_sizing};
    use dagr_core::limits::ContainerLimitProbe;

    let base = TempBase::new("t115-no-pools");
    let tier = tier_from(&base, "[default]\ngrace = \"25s\"\n", None);
    let sizing = with_env(&[], || {
        resolve_pool_sizing(PoolPinFlags::default(), None, &tier)
    })
    .expect("a poolless file resolves");
    assert!(
        !sizing.engaged(),
        "a file with no pool keys leaves sizing disengaged"
    );
    let caps = sizing
        .capacities(ContainerLimitProbe::from_root("/nonexistent-root-for-t115"))
        .expect("sizing never fails");
    assert!(
        caps.is_none(),
        "disengaged sizing yields no capacity set — the admission ledger is \
         identical to a run with no file"
    );
}

/// A file-supplied headroom engages the probe and applies beneath the env tier.
#[test]
fn file_headroom_engages_and_env_beats_it() {
    use dagr_cli::config::{PoolPinFlags, resolve_pool_sizing};
    use dagr_core::limits::ContainerLimitProbe;

    let base = TempBase::new("t115-file-headroom");
    let tier = tier_from(&base, "[default.pool]\nheadroom-fraction = 0.5\n", None);

    // File alone: pools are sized at the file's 0.5 headroom.
    let sizing = with_env(&[], || {
        resolve_pool_sizing(PoolPinFlags::default(), None, &tier)
    })
    .expect("a file headroom resolves");
    assert!(
        sizing.engaged(),
        "a file-supplied headroom engages the probe"
    );
    let caps = sizing
        .capacities(ContainerLimitProbe::from_root("/nonexistent-root-for-t115").with_host_cores(8))
        .expect("sizing never fails")
        .expect("engaged sizing yields capacities");
    assert_eq!(
        caps.total(dagr_core::admission::Pool::ComputeThreads),
        4,
        "pools are sized to half the detected limit under the file's 0.5"
    );

    // Env beats file: DAGR_HEADROOM=0.25 wins over the file's 0.5.
    let sizing = with_env(&[("DAGR_HEADROOM", "0.25")], || {
        resolve_pool_sizing(PoolPinFlags::default(), None, &tier)
    })
    .expect("the env headroom resolves");
    let caps = sizing
        .capacities(ContainerLimitProbe::from_root("/nonexistent-root-for-t115").with_host_cores(8))
        .expect("sizing never fails")
        .expect("engaged sizing yields capacities");
    assert_eq!(
        caps.total(dagr_core::admission::Pool::ComputeThreads),
        6,
        "DAGR_HEADROOM=0.25 beats the file's 0.5 (env > file): 8 cores keep \
         8 * (1 - 0.25) = 6, not the 4 the file's 0.5 would leave"
    );
}

// ===========================================================================
// Loud failures — file, profile, and key named; exit-code split honoured
// ===========================================================================

/// A single trivially-succeeding flow for the in-process registry scenarios.
#[derive(Clone)]
struct Unit;
impl StableName for Unit {
    const STABLE_NAME: &'static str = "T115Unit";
}
struct Quick;
impl StableName for Quick {
    const STABLE_NAME: &'static str = "T115Quick";
}
impl Task for Quick {
    type Input = ();
    type Output = Unit;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Unit, TaskError> {
        Ok(Unit)
    }
}

/// Build the one-node in-process fixture flow.
fn build_quick_flow() -> RunnableFlow {
    let mut flow = RunnableFlow::new();
    let _q = flow.register_source_named("quick", Quick);
    flow
}

/// Drive an in-process registry `run` with `args` appended after the flow name,
/// returning the exit code and the captured diagnostics.
fn registry_run(base: &TempBase, args: &[&str]) -> (ExitCode, String) {
    let registry = FlowRegistry::new().add("t115", build_quick_flow);
    let mut invocation: Vec<String> = vec!["dagr".into(), "run".into(), "t115".into()];
    invocation.extend(args.iter().map(ToString::to_string));
    invocation.extend(["--store".to_string(), base.as_str().to_string()]);
    let mut out = Vec::new();
    let exit = with_env(&[], || run_registry_to(&registry, invocation, &mut out));
    (exit, String::from_utf8_lossy(&out).into_owned())
}

/// Malformed TOML fails at bootstrap naming the file, with `InvalidUsage`.
#[test]
fn malformed_toml_fails_at_bootstrap_naming_the_file() {
    let base = TempBase::new("t115-malformed");
    let cfg = write_file(base.as_str(), "bad.toml", "not == toml [\n");
    let (exit, out) = registry_run(&base, &["--dagr.config", &cfg]);
    assert_eq!(
        exit,
        ExitCode::InvalidUsage,
        "malformed TOML is invalid usage, diagnostics:\n{out}"
    );
    assert!(
        out.contains("bad.toml"),
        "the diagnostic names the file, got:\n{out}"
    );
    assert!(
        !Path::new(base.as_str()).join("t115").exists(),
        "a run refused at bootstrap leaves no run directory behind"
    );
}

/// An unknown key fails at bootstrap naming the file, the profile, and the key.
#[test]
fn unknown_key_fails_naming_file_profile_and_key() {
    let base = TempBase::new("t115-unknown-key");
    let cfg = write_file(
        base.as_str(),
        "cfg.toml",
        "[default]\ngrace-period = \"10s\"\n",
    );
    let (exit, out) = registry_run(&base, &["--dagr.config", &cfg]);
    assert_eq!(
        exit,
        ExitCode::InvalidUsage,
        "an unknown key is invalid usage, diagnostics:\n{out}"
    );
    assert!(
        out.contains("cfg.toml") && out.contains(DEFAULT_PROFILE) && out.contains("grace-period"),
        "the diagnostic names the file, the profile, and the key, got:\n{out}"
    );
}

/// A wrong-typed value (an array where a scalar belongs) fails at bootstrap
/// naming the file, the profile, and the key.
#[test]
fn wrong_typed_value_fails_naming_file_profile_and_key() {
    let base = TempBase::new("t115-wrong-type");
    let cfg = write_file(base.as_str(), "cfg.toml", "[default]\ngrace = [\"25s\"]\n");
    let (exit, out) = registry_run(&base, &["--dagr.config", &cfg]);
    assert_eq!(
        exit,
        ExitCode::InvalidUsage,
        "a wrong-typed value is invalid usage, diagnostics:\n{out}"
    );
    assert!(
        out.contains("cfg.toml") && out.contains(DEFAULT_PROFILE) && out.contains("grace"),
        "the diagnostic names the file, the profile, and the key, got:\n{out}"
    );
}

/// A scalar of the wrong kind (a boolean where a duration belongs) fails at
/// resolution naming the file, the profile, and the key in the detail.
#[test]
fn unparseable_file_value_names_file_profile_and_key() {
    let base = TempBase::new("t115-bad-scalar");
    let cfg = write_file(base.as_str(), "cfg.toml", "[default]\ngrace = true\n");
    let (exit, out) = registry_run(&base, &["--dagr.config", &cfg]);
    assert_eq!(
        exit,
        ExitCode::InvalidUsage,
        "an unparseable file value is invalid usage (the EnvParseError split), \
         diagnostics:\n{out}"
    );
    assert!(
        out.contains("cfg.toml") && out.contains(DEFAULT_PROFILE) && out.contains("grace"),
        "the diagnostic names the file, the profile, and the key, got:\n{out}"
    );
}

/// An out-of-range file value keeps the `EnvParseError` exit-code split: a
/// headroom of 1.5 from the file is a **bootstrap failure**, not invalid usage.
#[test]
fn out_of_range_file_headroom_is_bootstrap_failure() {
    let base = TempBase::new("t115-oob-headroom");
    let cfg = write_file(
        base.as_str(),
        "cfg.toml",
        "[default.pool]\nheadroom-fraction = 1.5\n",
    );
    let (exit, out) = registry_run(&base, &["--dagr.config", &cfg]);
    assert_eq!(
        exit,
        ExitCode::BootstrapFailure,
        "an out-of-range file value is a bootstrap failure (the exit-code split \
         is honoured), diagnostics:\n{out}"
    );
    assert!(
        out.contains("headroom-fraction"),
        "the diagnostic names the key, got:\n{out}"
    );
}

// ===========================================================================
// Boundaries — no graph reach, reserved flags, key-set hygiene
// ===========================================================================

/// The file cannot select a flow: there is no key for it, and writing one is a
/// loud unknown-key failure — never a quiet no-op.
#[test]
fn file_cannot_select_a_flow() {
    assert!(
        !file_keys().contains(&"flow") && !file_keys().contains(&"run"),
        "the closed key set has no flow-selecting key (the graph stays code): {:?}",
        file_keys()
    );
    let base = TempBase::new("t115-no-flow-key");
    let cfg = write_file(base.as_str(), "cfg.toml", "[default]\nflow = \"other\"\n");
    let (exit, out) = registry_run(&base, &["--dagr.config", &cfg]);
    assert_eq!(
        exit,
        ExitCode::InvalidUsage,
        "a flow key is an unknown key, diagnostics:\n{out}"
    );
    assert!(
        out.contains("flow"),
        "the diagnostic names the rejected key, got:\n{out}"
    );
}

/// `dagr.profile` and `dagr.config` are reserved library flags, and both take a
/// value: their value token is never mistaken for the flow name.
#[test]
fn profile_and_config_flags_are_reserved_and_take_values() {
    assert!(
        reserved_flag_names().contains(&"dagr.profile"),
        "dagr.profile is reserved"
    );
    assert!(
        reserved_flag_names().contains(&"dagr.config"),
        "dagr.config is reserved"
    );
    for args in [
        ["dagr", "run", "--dagr.profile", "prod", "etl"],
        ["dagr", "run", "--dagr.config", "./x.toml", "etl"],
    ] {
        match parse_cli(args) {
            ParseOutcome::Parsed(cli) => assert_eq!(
                cli.flow_name.as_deref(),
                Some("etl"),
                "the flow name is `etl`, not the flag's value token"
            ),
            other => panic!("the invocation parses to its verb, got {other:?}"),
        }
    }
}

/// The assembly verbs never read the file: `graph` and `validate` succeed with
/// `--dagr.config` pointing at a path that would be a hard error if the loader
/// ran — while `run` with the same flag refuses. Discovery is bootstrap-only.
#[test]
fn assembly_verbs_never_read_the_config_file() {
    let missing = "/nonexistent-t115/definitely-missing.toml";
    let registry = FlowRegistry::new().add("t115", build_quick_flow);

    let mut out = Vec::new();
    let exit = run_registry_to(
        &registry,
        ["dagr", "graph", "t115", "--dagr.config", missing],
        &mut out,
    );
    assert_eq!(
        exit,
        ExitCode::Success,
        "graph never opens the config file, diagnostics:\n{}",
        String::from_utf8_lossy(&out)
    );
    assert!(!out.is_empty(), "graph emitted its artifact");

    let mut out = Vec::new();
    let exit = run_registry_to(
        &registry,
        ["dagr", "validate", "t115", "--dagr.config", missing],
        &mut out,
    );
    assert_eq!(
        exit,
        ExitCode::Success,
        "validate never opens the config file, diagnostics:\n{}",
        String::from_utf8_lossy(&out)
    );

    // The contrast: the run verb DOES resolve discovery, and refuses loudly.
    let base = TempBase::new("t115-run-contrast");
    let (exit, out) = registry_run(&base, &["--dagr.config", missing]);
    assert_eq!(
        exit,
        ExitCode::InvalidUsage,
        "run with the same missing explicit path refuses, diagnostics:\n{out}"
    );
}

// ===========================================================================
// The loader seams — selection without reading the env, executor/max-pods keys
// ===========================================================================

/// A present profile flag wins without reading `DAGR_PROFILE` at all: garbage
/// in the environment does not perturb a flag-selected load.
#[test]
fn profile_flag_wins_without_reading_the_env() {
    let base = TempBase::new("t115-flag-no-env-read");
    let tier = with_env(&[(DAGR_PROFILE, "not-a-profile-anywhere")], || {
        tier_from(&base, LAYERED, Some("prod"))
    });
    let grace = tier.get("grace").expect("prod layers grace over default");
    assert_eq!(grace.raw(), "7s", "the flag-selected profile applied");
}

/// The effective map records which profile supplied each key, and the value's
/// provenance names the file, the profile, and the key.
#[test]
fn file_values_carry_their_provenance() {
    let base = TempBase::new("t115-provenance");
    let tier = tier_from(&base, LAYERED, Some("prod"));
    let grace = tier.get("grace").expect("prod supplies grace");
    assert!(
        grace.provenance().contains("seam.toml")
            && grace.provenance().contains("prod")
            && grace.provenance().contains("grace"),
        "provenance names the file, the profile, and the key: {}",
        grace.provenance()
    );
    let teardown = tier
        .get("teardown-deadline")
        .expect("default supplies teardown-deadline");
    assert!(
        teardown.provenance().contains(DEFAULT_PROFILE),
        "a key layered up from [default] records default as its source: {}",
        teardown.provenance()
    );
    assert!(
        tier.get("pool.memory").is_none(),
        "an unmentioned key is absent — the tri-state's 'absent' leg"
    );
}

/// The `executor` and `max-pods` keys resolve through the file tier beneath the
/// environment (the ADR 128 §3 worked example's keys).
#[test]
fn executor_and_max_pods_resolve_through_the_file_tier() {
    use dagr_cli::config::{resolve_executor, resolve_max_pods};
    use dagr_cli::executor::ExecutorKind;

    let base = TempBase::new("t115-executor-key");
    let tier = tier_from(
        &base,
        "[default]\nexecutor = \"k8s\"\nmax-pods = 50\n",
        None,
    );
    let (executor, max_pods) = with_env(&[], || {
        (
            resolve_executor(None, tier.get("executor")),
            resolve_max_pods(None, tier.get("max-pods")),
        )
    });
    assert_eq!(
        executor.expect("the file's executor resolves"),
        ExecutorKind::Kubernetes,
        "the file's executor key reaches selection"
    );
    assert_eq!(
        max_pods.expect("the file's max-pods resolves"),
        50,
        "the file's max-pods key reaches the remote-slot ceiling"
    );

    // Env beats file for both.
    let executor = with_env(&[("DAGR_EXECUTOR", "local")], || {
        resolve_executor(None, tier.get("executor"))
    });
    assert_eq!(
        executor.expect("the env executor resolves"),
        ExecutorKind::Local,
        "DAGR_EXECUTOR beats the file's executor key"
    );
}

/// The metastore toggle's file key resolves beneath the environment (the
/// resolver is compiled unconditionally; only the tee wiring is feature-gated).
#[test]
fn metastore_toggle_resolves_through_the_file_tier() {
    use dagr_cli::config::resolve_metastore_toggle;

    let base = TempBase::new("t115-metastore-key");
    let tier = tier_from(&base, "[default]\nmetastore = true\n", None);
    let on = with_env(&[], || {
        resolve_metastore_toggle(None, tier.get("metastore"))
    });
    assert!(
        on.expect("the file's metastore toggle resolves"),
        "the file's metastore key turns the toggle on"
    );
    let off = with_env(&[("DAGR_METASTORE", "0")], || {
        resolve_metastore_toggle(None, tier.get("metastore"))
    });
    assert!(
        !off.expect("the env toggle resolves"),
        "DAGR_METASTORE=0 beats the file's metastore = true"
    );
}

// ===========================================================================
// No behaviour change — an empty [default] is byte-identical to no file
// ===========================================================================

/// Scrub the wall stamp and the run id out of a stream so two *different* runs
/// of the same pipeline compare byte-for-byte on everything the run did.
fn scrub_stream(stream: &str, run_id: &str) -> String {
    let mut text = stream.replace(run_id, "<run-id>");
    let mut out = String::with_capacity(text.len());
    while let Some(start) = text.find("\"wall\":\"") {
        let value_start = start + "\"wall\":\"".len();
        out.push_str(&text[..value_start]);
        out.push_str("<wall>");
        let Some(end) = text[value_start..].find('"') else {
            text.clear();
            break;
        };
        text = text.split_off(value_start + end);
    }
    out.push_str(&text);
    out
}

/// Run the in-process fixture and return the scrubbed stream.
fn scrubbed_registry_stream(base: &TempBase, args: &[&str]) -> String {
    let (exit, out) = registry_run(base, args);
    assert_eq!(exit, ExitCode::Success, "the run succeeds, out:\n{out}");
    let dir = sole_run_dir(base.as_str(), "t115").expect("the run wrote a store");
    let run_id = dir
        .file_name()
        .and_then(|n| n.to_str())
        .expect("the run directory is the run id")
        .to_string();
    let stream = std::fs::read_to_string(dir.join("events.jsonl")).expect("the event stream reads");
    scrub_stream(&stream, &run_id)
}

/// A run with a config file whose selected profile sets nothing is
/// **byte-identical** (run id and wall stamps scrubbed — they vary between any
/// two runs) to a run with no file at all. Together with the standing
/// pre-M11 byte-identity guard in `tests/wire_precedence_run_path.rs`, this pins
/// "no file present changes nothing".
#[test]
fn empty_default_file_is_byte_identical_to_no_file() {
    let base_none = TempBase::new("t115-ident-none");
    let none = scrubbed_registry_stream(&base_none, &[]);

    let base_file = TempBase::new("t115-ident-file");
    let cfg = write_file(base_file.as_str(), "cfg.toml", "[default]\n");
    let with_file = scrubbed_registry_stream(&base_file, &["--dagr.config", &cfg]);

    assert_eq!(
        none, with_file,
        "an empty [default] table changes no byte of the event stream"
    );
}
