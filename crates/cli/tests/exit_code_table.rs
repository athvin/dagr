//! C26 · **Exit-code table** tests — ticket T55 (068). Written first, TDD.
//!
//! The crux of C26 is the exit-code table: every run outcome / error class maps
//! to a **specific numbered exit code**, by cause, with precedence (arch.md
//! `### C26 · Command-line contract`). These tests pin that mapping
//! **exhaustively** and table-driven: every `ExitCode` variant has a fixed
//! number, every number is distinct, and every cause the CLI can surface maps to
//! the documented code — including the load-bearing precedence rule
//! (**run failure beats consequent cancellation**).
//!
//! The numbers are fixed here so a change to any of them is a review-visible test
//! diff (arch.md C26: *"documented in one table and never changes within a major
//! version"*).

use dagr_cli::contract::{exit_code_for_run, ExitCode};
use dagr_cli::driver::{OverallOutcome, RunReport, ShutdownExit};
use dagr_core::context::{CancellationOrigin, TerminalState};

use std::collections::BTreeMap;

/// The documented C26 numbering. This is the single authoritative table the code
/// and every orchestrator agree on; if the code changes a number, this test
/// fails, which is the point (stability within a major version).
fn documented_number(code: ExitCode) -> u8 {
    match code {
        ExitCode::Success => 0,
        ExitCode::RunFailure => 1,
        ExitCode::InvalidUsage => 2,
        ExitCode::AssemblyFailure => 3,
        ExitCode::BootstrapFailure => 4,
        ExitCode::Cancelled => 5,
        ExitCode::ResumeRefusal => 6,
        ExitCode::SinkFailure => 7,
    }
}

/// Every `ExitCode` variant maps to its documented number, and the mapping is
/// what the code exposes (`as_u8`). Table-driven over every variant.
#[test]
fn every_exit_code_has_its_documented_number() {
    for code in ExitCode::ALL {
        assert_eq!(
            code.as_u8(),
            documented_number(code),
            "exit code {code:?} must carry its documented C26 number"
        );
    }
}

/// The numbers are all distinct — no two causes share a code (the table is a
/// bijection over its causes, arch.md C26: "distinct codes exist for …").
#[test]
fn every_exit_code_is_distinct() {
    let mut seen = std::collections::BTreeSet::new();
    for code in ExitCode::ALL {
        assert!(
            seen.insert(code.as_u8()),
            "exit code {code:?} number {} collides with another cause",
            code.as_u8()
        );
    }
    assert_eq!(seen.len(), ExitCode::ALL.len(), "every variant is enumerated");
}

/// Success is exactly zero — the Unix convention every orchestrator relies on.
#[test]
fn success_is_zero() {
    assert_eq!(ExitCode::Success.as_u8(), 0);
}

// ===========================================================================
// Outcome → exit-code mapping, table-driven and exhaustive over `RunReport`.
// ===========================================================================

/// Build a `RunReport` for a run whose overall outcome, cancellation origin, and
/// shutdown-exit selection are fixed, so the mapping can be asserted purely from
/// the report the driver produces.
fn report(
    outcome: OverallOutcome,
    cancellation_origin: Option<CancellationOrigin>,
    shutdown_exit: ShutdownExit,
) -> RunReport {
    RunReport {
        outcome,
        terminal_states: BTreeMap::new(),
        run_id: "r".into(),
        stream_path: "s".into(),
        cancellation_origin,
        shutdown_exit,
    }
}

/// The full outcome → exit-code table, exhaustive over every cause the driver
/// reports, including the precedence rules. Each row is one documented mapping.
#[test]
fn run_outcome_maps_to_the_documented_exit_code() {
    let cases: &[(RunReport, ExitCode, &str)] = &[
        // Plain success.
        (
            report(OverallOutcome::Succeeded, None, ShutdownExit::Success),
            ExitCode::Success,
            "a clean run exits success",
        ),
        // A run failure (a non-teardown node ended failed/timed-out).
        (
            report(OverallOutcome::Failed, None, ShutdownExit::RunFailure),
            ExitCode::RunFailure,
            "a failed node exits run-failure",
        ),
        // Assembly failed before execution — distinct code.
        (
            report(OverallOutcome::AssemblyFailed, None, ShutdownExit::Success),
            ExitCode::AssemblyFailure,
            "assembly failure has its own code",
        ),
        // Bootstrap failed a fail-fast startup check — distinct code.
        (
            report(OverallOutcome::BootstrapFailed, None, ShutdownExit::Success),
            ExitCode::BootstrapFailure,
            "bootstrap failure has its own code",
        ),
        // External cancellation with NO run failure → the cancellation code.
        (
            report(
                OverallOutcome::Cancelled,
                Some(CancellationOrigin::ExternalInterrupt),
                ShutdownExit::Cancelled,
            ),
            ExitCode::Cancelled,
            "external cancellation with no run failure exits cancelled",
        ),
        // PRECEDENCE: a run failure that TRIGGERED a self-inflicted cancellation
        // (stop-on-first-failure) must still exit run-failure — the consequent
        // cancellation does not mask it.
        (
            report(
                OverallOutcome::Failed,
                Some(CancellationOrigin::FailureUnderStop),
                ShutdownExit::RunFailure,
            ),
            ExitCode::RunFailure,
            "run failure beats the consequent cancellation it triggered",
        ),
        // A sink failure at shutdown with no run failure → the distinct sink code.
        (
            report(OverallOutcome::Cancelled, None, ShutdownExit::SinkFailure),
            ExitCode::SinkFailure,
            "an unwritable sink at shutdown has its own code",
        ),
        // PRECEDENCE: a run failure beats a sink failure at shutdown.
        (
            report(OverallOutcome::Failed, None, ShutdownExit::RunFailure),
            ExitCode::RunFailure,
            "run failure beats sink failure",
        ),
    ];

    for (rep, expected, why) in cases {
        assert_eq!(
            exit_code_for_run(rep),
            *expected,
            "{why}: outcome={:?} origin={:?} shutdown={:?}",
            rep.outcome,
            rep.cancellation_origin,
            rep.shutdown_exit,
        );
    }
}

/// A skip-only run (every node skip-family, none failed/timed-out) is a
/// **successful** run and exits success (arch.md Vocabulary + C26).
#[test]
fn skip_only_run_exits_success() {
    let mut terminals = BTreeMap::new();
    terminals.insert("a".to_string(), TerminalState::Skipped);
    terminals.insert("b".to_string(), TerminalState::UpstreamSkipped);
    let rep = RunReport {
        outcome: OverallOutcome::Succeeded,
        terminal_states: terminals,
        run_id: "r".into(),
        stream_path: "s".into(),
        cancellation_origin: None,
        shutdown_exit: ShutdownExit::Success,
    };
    assert_eq!(exit_code_for_run(&rep), ExitCode::Success);
}
