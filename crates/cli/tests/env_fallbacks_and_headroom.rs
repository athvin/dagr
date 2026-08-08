//! **`DAGR_*` env fallbacks + the headroom knob** — integration tests.
//! Written first, TDD.
//!
//! This exercises the CLI-side env-fallback wiring: the opt-in, fallible
//! `RunConfig` env-fallback builder methods (grace / teardown-deadline /
//! failure-mode), the CLI pool-pinning layer that resolves `DAGR_POOL_*` and
//! `DAGR_HEADROOM` and hands parsed values to `PinnedPools` /
//! `ContainerLimitProbe`, and the loud-failure exit-code mapping (an unparseable
//! env value → `InvalidUsage`, an out-of-range value → `BootstrapFailure`). The
//! whole `flag > env > default` precedence is proven per knob, and an end-to-end
//! run proves the env path is behaviourally identical to the flag path.
//!
//! `dagr-core` reads no environment — every `DAGR_*` variable is resolved here in
//! `dagr-cli` and passed inward as a parsed value.
//!
//! # Hermeticity
//!
//! These tests read the **real** `DAGR_*` variable names (the ticket's test plan
//! names them literally), so every env-mutating test takes a shared process-global
//! [`ENV_LOCK`] and sets/removes the variable **inside** the guard — the
//! process-global hardening: no two tests ever race over the same OS-global variable, even under
//! cargo's default parallel runner. Under edition 2024 `set_var`/`remove_var` are
//! `unsafe fn`s (the update can reallocate `environ` under a concurrent `getenv`),
//! which is exactly the hazard that lock already closes; the two helpers below are
//! the only places this suite mutates the environment, and each carries its own
//! `// SAFETY:` note.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::Duration;

use dagr_artifact::event_stream::{EventSink, MonotonicClock, RunOutcome};
use dagr_cli::config::{
    DAGR_FAILURE_MODE, DAGR_GRACE, DAGR_HEADROOM, DAGR_POOL_BLOCKING_THREADS,
    DAGR_POOL_COMPUTE_THREADS, DAGR_POOL_MEMORY, DAGR_TEARDOWN_DEADLINE, EnvParseErrorKind,
    PoolPinFlags, resolve_headroom, resolve_pool_pins,
};
use dagr_cli::contract::ExitCode;
use dagr_cli::driver::{
    DEFAULT_GRACE, DEFAULT_TEARDOWN_DEADLINE, NodeRunner, RunConfig, RunPlan, drive,
};
use dagr_core::TaskError;
use dagr_core::admission::PoolCapacities;
use dagr_core::context::{RunContext, TerminalState};
use dagr_core::execution::{AttemptEventSink, run_attempt_caught};
use dagr_core::flow::{FailureMode, Flow, Pipeline};
use dagr_core::limits::ContainerLimitProbe;
use dagr_core::slot::{ResidencyLedger, Slot};
use dagr_core::task::Task;
use dagr_core::test_kit::TempBase;

/// The process-global lock every env-mutating test takes, so setting the real
/// `DAGR_*` names never races across parallel tests.
fn env_lock() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Set `key` to `value`, run `body`, then remove `key` — all under the env lock so
/// no other test observes the variable.
#[allow(
    unsafe_code,
    reason = "std::env::set_var/remove_var are unsafe fns in edition 2024; the \
              resolvers under test read the REAL process environment by name, so a \
              test cannot avoid mutating it — with_env/with_clean_env are the only \
              two places this suite does, and both hold the env lock"
)]
fn with_env<T>(pairs: &[(&str, &str)], body: impl FnOnce() -> T) -> T {
    let _guard = env_lock();
    for (k, _) in pairs {
        // SAFETY: mutating the environment races a concurrent `getenv`; `_guard`
        // holds this suite's process-global env lock for the whole
        // remove → set → read → remove window, so no other env-touching test here
        // can be reading while the array is updated.
        unsafe { std::env::remove_var(k) };
    }
    for (k, v) in pairs {
        // SAFETY: as above — still inside the `_guard` window.
        unsafe { std::env::set_var(k, v) };
    }
    let out = body();
    for (k, _) in pairs {
        // SAFETY: as above — still inside the `_guard` window.
        unsafe { std::env::remove_var(k) };
    }
    out
}

/// Run `body` with every `DAGR_*` name this suite touches guaranteed **unset**.
#[allow(
    unsafe_code,
    reason = "std::env::remove_var is an unsafe fn in edition 2024; see with_env"
)]
fn with_clean_env<T>(body: impl FnOnce() -> T) -> T {
    let _guard = env_lock();
    for k in [
        DAGR_GRACE,
        DAGR_TEARDOWN_DEADLINE,
        DAGR_FAILURE_MODE,
        DAGR_POOL_COMPUTE_THREADS,
        DAGR_POOL_BLOCKING_THREADS,
        DAGR_POOL_MEMORY,
        DAGR_HEADROOM,
    ] {
        // SAFETY: `_guard` holds the suite's process-global env lock across the
        // whole unset → read window, as in `with_env` above.
        unsafe { std::env::remove_var(k) };
    }
    body()
}

// ===========================================================================
// Precedence per knob — flag > env > default
// ===========================================================================

#[test]
fn grace_flag_beats_env() {
    let temp_base = TempBase::new("t77");
    let cfg = with_env(&[(DAGR_GRACE, "30s")], || {
        RunConfig::new(temp_base.as_str())
            .grace_from_env(Some(Duration::from_secs(5)))
            .expect("valid flag path never errors")
    });
    assert_eq!(
        cfg.effective_grace(),
        Duration::from_secs(5),
        "a present --grace flag wins outright over DAGR_GRACE"
    );
}

#[test]
fn grace_env_used_when_no_flag() {
    let temp_base = TempBase::new("t77");
    let cfg = with_env(&[(DAGR_GRACE, "30s")], || {
        RunConfig::new(temp_base.as_str())
            .grace_from_env(None)
            .expect("a valid DAGR_GRACE parses")
    });
    assert_eq!(
        cfg.effective_grace(),
        Duration::from_secs(30),
        "with no flag, DAGR_GRACE is used"
    );
}

#[test]
fn grace_default_when_neither() {
    let temp_base = TempBase::new("t77");
    let cfg = with_clean_env(|| {
        RunConfig::new(temp_base.as_str())
            .grace_from_env(None)
            .expect("the default path never errors")
    });
    assert_eq!(
        cfg.effective_grace(),
        DEFAULT_GRACE,
        "neither flag nor env → the 10s default"
    );
}

#[test]
fn teardown_deadline_precedence() {
    let temp_base = TempBase::new("t77");
    // flag beats env
    let cfg = with_env(&[(DAGR_TEARDOWN_DEADLINE, "40s")], || {
        RunConfig::new(temp_base.as_str())
            .teardown_deadline_from_env(Some(Duration::from_secs(7)))
            .expect("flag path")
    });
    assert_eq!(cfg.effective_teardown_deadline(), Duration::from_secs(7));

    // env used when no flag
    let cfg = with_env(&[(DAGR_TEARDOWN_DEADLINE, "40s")], || {
        RunConfig::new(temp_base.as_str())
            .teardown_deadline_from_env(None)
            .expect("env path")
    });
    assert_eq!(cfg.effective_teardown_deadline(), Duration::from_secs(40));

    // default when neither
    let cfg = with_clean_env(|| {
        RunConfig::new(temp_base.as_str())
            .teardown_deadline_from_env(None)
            .expect("default path")
    });
    assert_eq!(cfg.effective_teardown_deadline(), DEFAULT_TEARDOWN_DEADLINE);
}

#[test]
fn failure_mode_precedence() {
    let temp_base = TempBase::new("t77");
    // flag beats env
    let cfg = with_env(&[(DAGR_FAILURE_MODE, "stop-on-first-failure")], || {
        RunConfig::new(temp_base.as_str())
            .failure_mode_from_env(Some(FailureMode::ContinueIndependent))
            .expect("flag path")
    });
    assert_eq!(
        cfg.effective_failure_mode(),
        FailureMode::ContinueIndependent
    );

    // env used when no flag
    let cfg = with_env(&[(DAGR_FAILURE_MODE, "stop-on-first-failure")], || {
        RunConfig::new(temp_base.as_str())
            .failure_mode_from_env(None)
            .expect("env path")
    });
    assert_eq!(
        cfg.effective_failure_mode(),
        FailureMode::StopOnFirstFailure
    );

    // default when neither (continue-independent)
    let cfg = with_clean_env(|| {
        RunConfig::new(temp_base.as_str())
            .failure_mode_from_env(None)
            .expect("default path")
    });
    assert_eq!(
        cfg.effective_failure_mode(),
        FailureMode::ContinueIndependent
    );
}

// ===========================================================================
// Pools — DAGR_POOL_* resolved at the CLI layer, passed to PinnedPools
// ===========================================================================

#[test]
fn pool_env_values_reflected_when_no_flag() {
    let pins = with_env(
        &[
            (DAGR_POOL_COMPUTE_THREADS, "3"),
            (DAGR_POOL_BLOCKING_THREADS, "5"),
            (DAGR_POOL_MEMORY, "4096"),
        ],
        || resolve_pool_pins(PoolPinFlags::default()).expect("valid pool env parses"),
    );
    assert_eq!(pins.compute_threads_pin(), Some(3));
    assert_eq!(pins.blocking_threads_pin(), Some(5));
    assert_eq!(pins.memory_pin(), Some(4096));
}

#[test]
fn pool_flag_beats_env() {
    let pins = with_env(&[(DAGR_POOL_COMPUTE_THREADS, "9")], || {
        resolve_pool_pins(PoolPinFlags {
            compute_threads: Some(2),
            ..PoolPinFlags::default()
        })
        .expect("flag path")
    });
    assert_eq!(
        pins.compute_threads_pin(),
        Some(2),
        "a --dagr.pool.compute-threads flag wins over DAGR_POOL_COMPUTE_THREADS"
    );
}

#[test]
fn pool_unset_env_and_flag_yields_no_pin() {
    let pins = with_clean_env(|| resolve_pool_pins(PoolPinFlags::default()).expect("no pins"));
    assert_eq!(pins.compute_threads_pin(), None);
    assert_eq!(pins.blocking_threads_pin(), None);
    assert_eq!(pins.memory_pin(), None);
}

#[test]
fn pool_env_reaches_the_probe_derived_capacities() {
    // The env is resolved in dagr-cli and passed to the core probe as a parsed
    // pin; dagr-core reads no environment. A pinned pool's total is the pin
    // verbatim, overriding detection/host-fallback.
    let pins = with_env(&[(DAGR_POOL_MEMORY, "2048")], || {
        resolve_pool_pins(PoolPinFlags::default()).expect("env parses")
    });
    let caps = ContainerLimitProbe::from_root("/nonexistent-root-for-t77")
        .with_host_cores(4)
        .with_pins(pins)
        .detect()
        .expect("sizing never fails");
    assert_eq!(
        caps.total(dagr_core::admission::Pool::Memory),
        2048,
        "the DAGR_POOL_MEMORY pin (resolved in the CLI) is the memory pool total"
    );
}

// ===========================================================================
// Headroom — DAGR_HEADROOM / --dagr.headroom-fraction (default 0.20, 0.0..=1.0)
// ===========================================================================

#[test]
fn headroom_default_is_twenty_percent() {
    let h = with_clean_env(|| resolve_headroom(None).expect("default"));
    assert!(
        (h - 0.20).abs() < f64::EPSILON,
        "default headroom is 0.20, got {h}"
    );
}

#[test]
fn headroom_env_used_when_no_flag() {
    let h = with_env(&[(DAGR_HEADROOM, "0.5")], || {
        resolve_headroom(None).expect("valid env")
    });
    assert!(
        (h - 0.5).abs() < f64::EPSILON,
        "DAGR_HEADROOM=0.5 is used, got {h}"
    );
}

#[test]
fn headroom_flag_beats_env() {
    let h = with_env(&[(DAGR_HEADROOM, "0.5")], || {
        resolve_headroom(Some(0.1)).expect("flag path")
    });
    assert!(
        (h - 0.1).abs() < f64::EPSILON,
        "--dagr.headroom-fraction 0.1 wins over DAGR_HEADROOM=0.5, got {h}"
    );
}

#[test]
fn headroom_env_of_half_sizes_pools_and_floors_at_one() {
    // With a 50% headroom applied to a known detected total, every pool gets
    // half — and the at-least-one-unit floor still holds (a tiny total floors
    // to one, never zero).
    let h = with_env(&[(DAGR_HEADROOM, "0.5")], || {
        resolve_headroom(None).expect("valid env")
    });
    let caps = ContainerLimitProbe::from_root("/nonexistent-root-for-t77")
        .with_host_cores(8)
        .with_headroom(h)
        .detect()
        .expect("sizing never fails");
    // 8 cores * (1 - 0.5) = 4 threads per thread pool.
    assert_eq!(caps.total(dagr_core::admission::Pool::ComputeThreads), 4);
    // The memory pool floors at one unit even under a large headroom (host
    // fallback total * 0.5, still ≥ 1).
    assert!(caps.total(dagr_core::admission::Pool::Memory) >= 1);

    // headroom = 1.0 still yields at least one unit per pool.
    let full = ContainerLimitProbe::from_root("/nonexistent-root-for-t77")
        .with_host_cores(8)
        .with_headroom(1.0)
        .detect()
        .expect("sizing never fails");
    assert_eq!(
        full.total(dagr_core::admission::Pool::ComputeThreads),
        1,
        "headroom 1.0 floors every pool at one unit"
    );
}

#[test]
fn headroom_out_of_range_env_is_bootstrap_failure_naming_the_variable() {
    let err = with_env(&[(DAGR_HEADROOM, "1.5")], || {
        resolve_headroom(None).expect_err("1.5 is out of 0.0..=1.0 and must fail loudly")
    });
    assert_eq!(err.kind, EnvParseErrorKind::OutOfRange);
    assert_eq!(
        err.exit_code(),
        ExitCode::BootstrapFailure,
        "an out-of-range headroom is the machine's fault at bootstrap"
    );
    assert!(
        err.to_string().contains(DAGR_HEADROOM),
        "the diagnostic names DAGR_HEADROOM, got: {err}"
    );
}

// ===========================================================================
// Loud failure — a bad env value is never silently ignored or clamped
// ===========================================================================

#[test]
fn unparseable_grace_env_is_invalid_usage_naming_the_variable() {
    let temp_base = TempBase::new("t77");
    let err = with_env(&[(DAGR_GRACE, "notaduration")], || {
        RunConfig::new(temp_base.as_str())
            .grace_from_env(None)
            .expect_err("a bad DAGR_GRACE must fail loudly")
    });
    assert_eq!(err.kind, EnvParseErrorKind::Parse);
    assert_eq!(err.exit_code(), ExitCode::InvalidUsage);
    assert!(err.to_string().contains(DAGR_GRACE));
}

#[test]
fn zero_grace_flag_is_out_of_range_naming_the_flag() {
    // The documented >= 1 ms bound, applied to the FLAG path: the two paths agree
    // and the diagnostic names whichever source supplied the value.
    let temp_base = TempBase::new("t77");
    let err = with_clean_env(|| {
        RunConfig::new(temp_base.as_str())
            .grace_from_env(Some(Duration::ZERO))
            .expect_err("a zero --grace violates the 1 ms bound")
    });
    assert_eq!(err.kind, EnvParseErrorKind::OutOfRange);
    assert_eq!(err.exit_code(), ExitCode::BootstrapFailure);
    assert!(err.to_string().contains("--grace"), "names the flag: {err}");
}

#[test]
fn zero_teardown_deadline_env_is_out_of_range_naming_the_variable() {
    // The documented >= 1 s bound, applied to the ENV path.
    let temp_base = TempBase::new("t77");
    let err = with_env(&[(DAGR_TEARDOWN_DEADLINE, "0")], || {
        RunConfig::new(temp_base.as_str())
            .teardown_deadline_from_env(None)
            .expect_err("a zero DAGR_TEARDOWN_DEADLINE violates the 1 s bound")
    });
    assert_eq!(err.kind, EnvParseErrorKind::OutOfRange);
    assert_eq!(err.exit_code(), ExitCode::BootstrapFailure);
    assert!(
        err.to_string().contains(DAGR_TEARDOWN_DEADLINE),
        "names the variable: {err}"
    );
}

#[test]
fn unparseable_pool_env_is_invalid_usage_naming_the_variable() {
    let err = with_env(&[(DAGR_POOL_COMPUTE_THREADS, "lots")], || {
        resolve_pool_pins(PoolPinFlags::default()).expect_err("a bad pool env must fail loudly")
    });
    assert_eq!(err.kind, EnvParseErrorKind::Parse);
    assert_eq!(err.exit_code(), ExitCode::InvalidUsage);
    assert!(err.to_string().contains(DAGR_POOL_COMPUTE_THREADS));
}

#[test]
fn unparseable_headroom_env_is_invalid_usage() {
    // A value that is not a float at all is a *parse* failure (InvalidUsage),
    // distinct from an out-of-range float (BootstrapFailure).
    let err = with_env(&[(DAGR_HEADROOM, "half")], || {
        resolve_headroom(None).expect_err("a non-float DAGR_HEADROOM must fail")
    });
    assert_eq!(err.kind, EnvParseErrorKind::Parse);
    assert_eq!(err.exit_code(), ExitCode::InvalidUsage);
    assert!(err.to_string().contains(DAGR_HEADROOM));
}

// ===========================================================================
// End-to-end — the env path is behaviourally identical to the flag path
// ===========================================================================

// A trivial two-node chain we can drive twice (once via env, once via flags).

struct Produce(u64);
impl Task for Produce {
    type Input = ();
    type Output = u64;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<u64, TaskError> {
        Ok(self.0)
    }
}

struct SourceRunner {
    name: String,
    task: Option<Produce>,
    slot: Arc<Slot<u64>>,
}

impl NodeRunner for SourceRunner {
    fn name(&self) -> &str {
        &self.name
    }
    fn run<'a>(
        &'a mut self,
        ctx: &'a RunContext,
        sink: &'a mut (dyn AttemptEventSink + Send),
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = TerminalState> + Send + 'a>> {
        let name = self.name.clone();
        let mut task = self.task.take().expect("runs once");
        let slot = Arc::clone(&self.slot);
        Box::pin(async move {
            run_attempt_caught(&mut task, &name, ctx, &slot, sink)
                .await
                .terminal_state()
        })
    }
}

fn slot_for(name: &str) -> Arc<Slot<u64>> {
    Arc::new(Slot::new(
        dagr_core::handle::NodeId::from_name(name),
        name,
        0,
        false,
        0,
        ResidencyLedger::new(),
    ))
}

fn two_source_plan() -> (Pipeline, RunPlan) {
    let mut flow = Flow::new();
    let _a = flow.register_source("a", &Produce(1));
    let _b = flow.register_source("b", &Produce(2));
    let pipeline = flow.finish();
    pipeline.assemble().expect("assembles");
    let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
    runners.insert(
        "a".into(),
        Box::new(SourceRunner {
            name: "a".into(),
            task: Some(Produce(1)),
            slot: slot_for("a"),
        }),
    );
    runners.insert(
        "b".into(),
        Box::new(SourceRunner {
            name: "b".into(),
            task: Some(Produce(2)),
            slot: slot_for("b"),
        }),
    );
    (pipeline.clone(), RunPlan::new(pipeline, runners))
}

#[derive(Clone, Default)]
struct MemorySink {
    lines: Arc<Mutex<Vec<u8>>>,
}
impl EventSink for MemorySink {
    fn append_line(&mut self, line: &[u8]) -> std::io::Result<()> {
        self.lines.lock().unwrap().extend_from_slice(line);
        Ok(())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
#[derive(Default)]
struct TickClock {
    n: AtomicU64,
}
impl MonotonicClock for TickClock {
    fn elapsed_ns(&self) -> u64 {
        self.n.fetch_add(1, Ordering::SeqCst)
    }
}

/// Build the run config for the **env** path: several `DAGR_*` set, no flags.
fn config_via_env(base: &str) -> RunConfig {
    let pins = resolve_pool_pins(PoolPinFlags::default()).expect("pool env parses");
    RunConfig::new(base)
        .run_id("t77-env")
        .grace_from_env(None)
        .expect("grace env")
        .failure_mode_from_env(None)
        .expect("failure-mode env")
        .capacities(PoolCapacities::new().memory(pins.memory_pin().expect("memory pinned via env")))
}

/// Build the run config for the **flag** path: the equivalent flags, no env.
fn config_via_flags(base: &str) -> RunConfig {
    RunConfig::new(base)
        .run_id("t77-flag")
        .grace_from_env(Some(Duration::from_secs(20)))
        .expect("grace flag")
        .failure_mode_from_env(Some(FailureMode::StopOnFirstFailure))
        .expect("failure-mode flag")
        .capacities(PoolCapacities::new().memory(1_000_000))
}

#[test]
fn env_configured_run_reflects_env_values() {
    let (_p, plan) = two_source_plan();
    let sink = MemorySink::default();
    let temp_base = TempBase::new("t77-e2e");
    let report = with_env(
        &[
            (DAGR_GRACE, "20s"),
            (DAGR_FAILURE_MODE, "stop-on-first-failure"),
            (DAGR_POOL_MEMORY, "1000000"),
        ],
        || {
            let config = config_via_env(temp_base.as_str());
            // The effective knobs observed by the run reflect the env values.
            assert_eq!(config.effective_grace(), Duration::from_secs(20));
            assert_eq!(
                config.effective_failure_mode(),
                FailureMode::StopOnFirstFailure
            );
            drive(
                &config,
                "t77",
                Ok(plan),
                &[],
                sink.clone(),
                TickClock::default(),
            )
        },
    );
    assert_eq!(report.outcome, RunOutcome::Succeeded);
    assert_eq!(
        report.terminal_states.get("a").copied(),
        Some(TerminalState::Succeeded)
    );
    assert_eq!(
        report.terminal_states.get("b").copied(),
        Some(TerminalState::Succeeded)
    );
}

#[test]
fn env_path_and_flag_path_are_behaviourally_identical() {
    // Same pipeline, once with knobs via DAGR_*, once via the equivalent flags:
    // the terminal states and the overall outcome match.
    let (_p1, plan_env) = two_source_plan();
    let sink_env = MemorySink::default();
    let temp_base = TempBase::new("t77-e2e");
    let env_report = with_env(
        &[
            (DAGR_GRACE, "20s"),
            (DAGR_FAILURE_MODE, "stop-on-first-failure"),
            (DAGR_POOL_MEMORY, "1000000"),
        ],
        || {
            let config = config_via_env(temp_base.as_str());
            drive(
                &config,
                "t77",
                Ok(plan_env),
                &[],
                sink_env.clone(),
                TickClock::default(),
            )
        },
    );

    let (_p2, plan_flag) = two_source_plan();
    let sink_flag = MemorySink::default();
    let flag_report = with_clean_env(|| {
        let config = config_via_flags(temp_base.as_str());
        drive(
            &config,
            "t77",
            Ok(plan_flag),
            &[],
            sink_flag.clone(),
            TickClock::default(),
        )
    });

    assert_eq!(
        env_report.outcome, flag_report.outcome,
        "same overall outcome"
    );
    assert_eq!(
        env_report.terminal_states, flag_report.terminal_states,
        "same per-node terminal states via env and via flags"
    );
}
