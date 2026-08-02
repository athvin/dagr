//! T108 · **`--dagr.pod-launch-retries`**, the operator-set *infrastructure* retry
//! budget. Written first (TDD).
//!
//! It is a separate knob from `NodePolicy::retries` because the two count different
//! things: a cluster at capacity would otherwise burn a node's entire retry budget
//! without ever executing it (ADR 115 §6). It follows the repo's one precedence
//! rule — **flag > env > default** — and its name is reserved in the `dagr.*`
//! namespace so a pipeline parameter can never shadow it.
//!
//! Its own test binary, so the environment mutation below cannot race another
//! suite's.

use std::ffi::OsString;

use dagr_cli::config::{
    DAGR_POD_LAUNCH_RETRIES, POD_LAUNCH_RETRIES_DEFAULT, POD_LAUNCH_RETRIES_FLAG,
    parse_pod_launch_retries_flag, resolve_pod_launch_retries,
};

fn argv(args: &[&str]) -> Vec<OsString> {
    args.iter().map(OsString::from).collect()
}

/// SAFETY: this binary's tests all mutate the same variable and run in one
/// process; they are serialized by `--test-threads` only when asked, so the
/// variable is set and cleared inside each test under a shared mutex.
fn env_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    LOCK.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[test]
fn the_flag_name_lives_in_the_reserved_dagr_namespace() {
    assert_eq!(POD_LAUNCH_RETRIES_FLAG, "--dagr.pod-launch-retries");
    assert!(
        dagr_cli::contract::reserved_flag_names().contains(&"dagr.pod-launch-retries"),
        "the knob is reserved, so a pipeline parameter can never shadow it"
    );
}

#[test]
fn both_flag_grammars_parse() {
    assert_eq!(
        parse_pod_launch_retries_flag(&argv(["run", "--dagr.pod-launch-retries", "4"].as_slice()))
            .expect("parses"),
        Some(4)
    );
    assert_eq!(
        parse_pod_launch_retries_flag(&argv(["run", "--dagr.pod-launch-retries=7"].as_slice()))
            .expect("parses"),
        Some(7)
    );
    assert_eq!(
        parse_pod_launch_retries_flag(&argv(["run"].as_slice())).expect("parses"),
        None
    );
    parse_pod_launch_retries_flag(&argv(["run", "--dagr.pod-launch-retries"].as_slice()))
        .expect_err("a value-less flag is an error, never a silent default");
}

#[test]
fn precedence_is_flag_then_env_then_default() {
    let _guard = env_lock();

    // SAFETY: single-threaded within this guard; the variable is restored below.
    unsafe { std::env::remove_var(DAGR_POD_LAUNCH_RETRIES) };
    assert_eq!(
        resolve_pod_launch_retries(None).expect("default"),
        POD_LAUNCH_RETRIES_DEFAULT,
        "unset falls through to the default"
    );

    // SAFETY: as above.
    unsafe { std::env::set_var(DAGR_POD_LAUNCH_RETRIES, "5") };
    assert_eq!(
        resolve_pod_launch_retries(None).expect("env"),
        5,
        "the env var is read when no flag is present"
    );
    assert_eq!(
        resolve_pod_launch_retries(Some(2)).expect("flag"),
        2,
        "the flag wins outright and the env var is never read"
    );

    // SAFETY: as above.
    unsafe { std::env::set_var(DAGR_POD_LAUNCH_RETRIES, "not-a-number") };
    resolve_pod_launch_retries(None).expect_err("an unparseable value is an error, not a default");

    // SAFETY: as above.
    unsafe { std::env::remove_var(DAGR_POD_LAUNCH_RETRIES) };
}

#[test]
fn the_default_is_a_small_finite_budget_rather_than_unbounded() {
    assert!(
        POD_LAUNCH_RETRIES_DEFAULT >= 1 && POD_LAUNCH_RETRIES_DEFAULT <= 10,
        "an infrastructure budget that never gives up is a hang with extra steps; \
         one that is zero makes a single unlucky scheduling decision fail a node"
    );
}
