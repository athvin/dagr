//! T108 · **`--dagr.pod-launch-retries`**, the operator-set *infrastructure* retry
//! budget. Written first (TDD).
//!
//! It is a separate knob from `NodePolicy::retries` because the two count different
//! things: a cluster at capacity would otherwise burn a node's entire retry budget
//! without ever executing it (ADR 115 §6). It follows the repo's one precedence
//! rule — **flag > env > default** — and its name is reserved in the `dagr.*`
//! namespace so a pipeline parameter can never shadow it.
//!
//! The env-precedence half lives in `config.rs`'s own test module, where the
//! process-global env lock and the `set_env` / `unset_env` wrappers already confine
//! the edition-2024 unsafety to one place. What is asserted here is everything that
//! needs no environment: the reserved name, both flag grammars, and the default's
//! shape.

use std::ffi::OsString;

use dagr_cli::config::{
    POD_LAUNCH_RETRIES_DEFAULT, POD_LAUNCH_RETRIES_FLAG, parse_pod_launch_retries_flag,
};

fn argv(args: &[&str]) -> Vec<OsString> {
    args.iter().map(OsString::from).collect()
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
        parse_pod_launch_retries_flag(&argv(&["run", "--dagr.pod-launch-retries", "4"]))
            .expect("parses"),
        Some(4)
    );
    assert_eq!(
        parse_pod_launch_retries_flag(&argv(&["run", "--dagr.pod-launch-retries=7"]))
            .expect("parses"),
        Some(7)
    );
    assert_eq!(
        parse_pod_launch_retries_flag(&argv(&["run"])).expect("parses"),
        None,
        "absent falls through to the env, then the default"
    );
    parse_pod_launch_retries_flag(&argv(&["run", "--dagr.pod-launch-retries"]))
        .expect_err("a value-less flag is an error, never a silent default");
    parse_pod_launch_retries_flag(&argv(&["run", "--dagr.pod-launch-retries=nope"]))
        .expect_err("an unparseable value is an error, never a silent default");
}

#[test]
fn the_default_is_a_small_finite_budget_rather_than_unbounded() {
    assert!(
        POD_LAUNCH_RETRIES_DEFAULT >= 1 && POD_LAUNCH_RETRIES_DEFAULT <= 10,
        "an infrastructure budget that never gives up is a hang with extra steps; \
         one that is zero fails a node on a single unlucky scheduling decision — \
         exactly the transient the separate budget exists to absorb"
    );
}

#[test]
fn the_launch_budget_is_not_the_nodes_retry_budget() {
    // The two are separate values with separate defaults, which is the whole point
    // of ADR 115 §6: a pod that never started executed nothing, so charging it to
    // `NodePolicy::retries` would let a cluster at capacity spend a node's budget
    // without running the task once.
    assert_eq!(
        dagr_core::assembly::NodePolicy::new()
            .retry_config()
            .max_attempts(),
        1,
        "a node's default is one attempt and no retries"
    );
    assert_ne!(
        u32::from(POD_LAUNCH_RETRIES_DEFAULT == 0),
        1,
        "…and the infrastructure budget defaults to something else entirely"
    );
}
