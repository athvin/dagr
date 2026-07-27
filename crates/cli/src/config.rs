//! C12/C26 · **Runtime-knob precedence** — the `flag > env > default` resolver,
//! the parsers it needs, a strict never-silent error type, the `DAGR_*` env-name
//! constants, and the extended reserved-flag namespace (ADR 089).
//!
//! (Stub — the implementation lands in the commit after the failing tests. This
//! declares the public surface so the test suite compiles and fails.)

use std::str::FromStr;
use std::time::Duration;

use dagr_core::flow::FailureMode;

use crate::contract::ExitCode;

/// Environment fallback for `--grace` (stub).
pub const DAGR_GRACE: &str = "";
/// Environment fallback for `--teardown-deadline` (stub).
pub const DAGR_TEARDOWN_DEADLINE: &str = "";
/// Environment fallback for `--failure-mode` (stub).
pub const DAGR_FAILURE_MODE: &str = "";
/// Environment fallback for `--dagr.pool.compute-threads` (stub).
pub const DAGR_POOL_COMPUTE_THREADS: &str = "";
/// Environment fallback for `--dagr.pool.blocking-threads` (stub).
pub const DAGR_POOL_BLOCKING_THREADS: &str = "";
/// Environment fallback for `--dagr.pool.memory` (stub).
pub const DAGR_POOL_MEMORY: &str = "";
/// Environment fallback for `--dagr.headroom-fraction` (stub).
pub const DAGR_HEADROOM: &str = "";

/// The kind of failure an [`EnvParseError`] records (stub).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnvParseErrorKind {
    /// A syntactic parse failure.
    Parse,
    /// A semantic out-of-range value.
    OutOfRange,
}

/// A `DAGR_*` environment variable carried an unusable value (stub).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnvParseError {
    /// The offending environment variable's name.
    pub variable: String,
    /// The rejected value verbatim.
    pub value: String,
    /// The failure kind.
    pub kind: EnvParseErrorKind,
    /// A human-readable detail.
    pub detail: String,
}

impl EnvParseError {
    /// A parse failure (stub).
    #[must_use]
    pub fn parse(
        _variable: impl Into<String>,
        _value: impl Into<String>,
        _detail: impl Into<String>,
    ) -> Self {
        todo!("EnvParseError::parse lands with the implementation commit")
    }

    /// An out-of-range failure (stub).
    #[must_use]
    pub fn out_of_range(
        _variable: impl Into<String>,
        _value: impl Into<String>,
        _detail: impl Into<String>,
    ) -> Self {
        todo!("EnvParseError::out_of_range lands with the implementation commit")
    }

    /// The C26 exit code this error maps to (stub).
    #[must_use]
    pub fn exit_code(&self) -> ExitCode {
        todo!("EnvParseError::exit_code lands with the implementation commit")
    }
}

impl std::fmt::Display for EnvParseError {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!("EnvParseError Display lands with the implementation commit")
    }
}

impl std::error::Error for EnvParseError {}

/// Resolve a runtime knob by `flag > env > default` (stub).
///
/// # Errors
/// Returns [`EnvParseError`] when the env value fails to parse.
pub fn resolve<T>(_flag: Option<T>, _env_key: &str, _default: T) -> Result<T, EnvParseError>
where
    T: FromStr,
    T::Err: std::fmt::Display,
{
    todo!("resolve lands with the implementation commit")
}

/// Parse a `10` / `10s` / `10ms` duration (stub).
///
/// # Errors
/// Returns [`DurationParseError`] for a non-accepted form.
pub fn parse_duration(_input: &str) -> Result<Duration, DurationParseError> {
    todo!("parse_duration lands with the implementation commit")
}

/// A string was not one of the accepted duration forms (stub).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurationParseError {
    /// The offending input verbatim.
    pub input: String,
}

impl std::fmt::Display for DurationParseError {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!("DurationParseError Display lands with the implementation commit")
    }
}

impl std::error::Error for DurationParseError {}

/// A newtype over [`Duration`] with a `10`/`10s`/`10ms` [`FromStr`] (stub).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EnvDuration(pub Duration);

impl EnvDuration {
    /// The wrapped [`Duration`].
    #[must_use]
    pub fn into_inner(self) -> Duration {
        self.0
    }
}

impl From<EnvDuration> for Duration {
    fn from(value: EnvDuration) -> Self {
        value.0
    }
}

impl FromStr for EnvDuration {
    type Err = DurationParseError;

    fn from_str(_s: &str) -> Result<Self, Self::Err> {
        todo!("EnvDuration::from_str lands with the implementation commit")
    }
}

/// Parse a [`FailureMode`] from its kebab-case token (stub).
///
/// # Errors
/// Returns [`FailureModeParseError`] for an unknown token.
pub fn parse_failure_mode(_input: &str) -> Result<FailureMode, FailureModeParseError> {
    todo!("parse_failure_mode lands with the implementation commit")
}

/// A string was not one of the accepted [`FailureMode`] tokens (stub).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailureModeParseError {
    /// The offending input verbatim.
    pub input: String,
}

impl std::fmt::Display for FailureModeParseError {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!("FailureModeParseError Display lands with the implementation commit")
    }
}

impl std::error::Error for FailureModeParseError {}

/// A newtype over [`FailureMode`] with a token [`FromStr`] (stub).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EnvFailureMode(pub FailureMode);

impl EnvFailureMode {
    /// The wrapped [`FailureMode`].
    #[must_use]
    pub fn into_inner(self) -> FailureMode {
        self.0
    }
}

impl From<EnvFailureMode> for FailureMode {
    fn from(value: EnvFailureMode) -> Self {
        value.0
    }
}

impl FromStr for EnvFailureMode {
    type Err = FailureModeParseError;

    fn from_str(_s: &str) -> Result<Self, Self::Err> {
        todo!("EnvFailureMode::from_str lands with the implementation commit")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Every env-touching test uses a UNIQUE variable name (never a real `DAGR_*`
    // name), set and removed within the test, so tests never race over a shared
    // process-global variable even under cargo's default parallel runner (the
    // T35-class hardening: hermetic, no shared mutable OS state). Edition 2021,
    // so `set_var`/`remove_var` are safe.

    // --- Precedence -------------------------------------------------------

    #[test]
    fn flag_present_wins_and_env_is_never_read() {
        let key = "DAGR_TEST_FLAG_WINS";
        std::env::set_var(key, "999");
        // Flag present → the env value (999) must be ignored entirely.
        let got = resolve::<u32>(Some(7), key, 0).expect("flag path never errors");
        std::env::remove_var(key);
        assert_eq!(got, 7, "a present flag must win outright over the env var");
    }

    #[test]
    fn no_flag_uses_parsed_env_value() {
        let key = "DAGR_TEST_ENV_USED";
        std::env::set_var(key, "42");
        let got = resolve::<u32>(None, key, 0).expect("a valid env value parses");
        std::env::remove_var(key);
        assert_eq!(got, 42, "with no flag, the env value is parsed and returned");
    }

    #[test]
    fn neither_flag_nor_env_returns_default() {
        let key = "DAGR_TEST_DEFAULT_UNSET";
        std::env::remove_var(key); // ensure unset
        let got = resolve::<u32>(None, key, 13).expect("the default path never errors");
        assert_eq!(got, 13, "with neither flag nor env, the default is returned");
    }

    #[test]
    fn empty_env_is_treated_as_unset() {
        let key = "DAGR_TEST_EMPTY_ENV";
        std::env::set_var(key, "");
        let got = resolve::<u32>(None, key, 5).expect("an empty env var is not-supplied");
        std::env::remove_var(key);
        assert_eq!(got, 5, "an empty env var behaves as unset → default");
    }

    // --- Parsing & errors -------------------------------------------------

    #[test]
    fn unparseable_env_value_is_invalid_usage_and_names_the_variable() {
        let key = "DAGR_TEST_BAD_PARSE";
        std::env::set_var(key, "not-a-number");
        let err = resolve::<u32>(None, key, 0).expect_err("a bad env value must error");
        std::env::remove_var(key);
        assert_eq!(err.exit_code(), ExitCode::InvalidUsage);
        assert!(
            err.to_string().contains(key),
            "the Display must name the offending variable, got: {err}"
        );
    }

    #[test]
    fn out_of_range_maps_to_bootstrap_failure_and_names_the_variable() {
        // A syntactically valid but out-of-range value (headroom 1.5 vs 0.0..=1.0).
        // The bound is the caller's (T77) to apply; this exercises the error the
        // resolver's consumer produces for the out-of-range case.
        let parsed: f64 = "1.5".parse().expect("1.5 is a valid f64");
        let err = if (0.0..=1.0).contains(&parsed) {
            panic!("1.5 is out of range; the test setup is wrong")
        } else {
            EnvParseError::out_of_range(DAGR_HEADROOM, "1.5", "expected 0.0..=1.0")
        };
        assert_eq!(err.exit_code(), ExitCode::BootstrapFailure);
        assert!(
            err.to_string().contains(DAGR_HEADROOM),
            "the Display must name the offending variable, got: {err}"
        );
    }

    #[test]
    fn duration_parses_bare_seconds_and_millis() {
        assert_eq!(parse_duration("10").expect("bare"), Duration::from_secs(10));
        assert_eq!(parse_duration("10s").expect("s"), Duration::from_secs(10));
        assert_eq!(parse_duration("10ms").expect("ms"), Duration::from_millis(10));
    }

    #[test]
    fn duration_rejects_garbage() {
        assert!(parse_duration("").is_err());
        assert!(parse_duration("abc").is_err());
        assert!(parse_duration("10m").is_err(), "minutes are not an accepted form");
        assert!(parse_duration("1.5s").is_err(), "only integer magnitudes");
    }

    #[test]
    fn env_duration_composes_with_resolve() {
        let key = "DAGR_TEST_DURATION";
        std::env::set_var(key, "250ms");
        let got = resolve::<EnvDuration>(None, key, EnvDuration(Duration::from_secs(1)))
            .expect("valid duration");
        std::env::remove_var(key);
        assert_eq!(got.into_inner(), Duration::from_millis(250));
    }

    #[test]
    fn env_duration_bad_value_is_invalid_usage() {
        let key = "DAGR_TEST_DURATION_BAD";
        std::env::set_var(key, "notaduration");
        let err = resolve::<EnvDuration>(None, key, EnvDuration(Duration::from_secs(1)))
            .expect_err("a bad duration must error");
        std::env::remove_var(key);
        assert_eq!(err.exit_code(), ExitCode::InvalidUsage);
        assert!(err.to_string().contains(key));
    }

    #[test]
    fn failure_mode_parses_both_tokens() {
        assert_eq!(
            parse_failure_mode("stop-on-first-failure").expect("stop"),
            FailureMode::StopOnFirstFailure
        );
        assert_eq!(
            parse_failure_mode("continue-independent").expect("continue"),
            FailureMode::ContinueIndependent
        );
    }

    #[test]
    fn failure_mode_rejects_unknown_token() {
        assert!(parse_failure_mode("halt").is_err());
        assert!(parse_failure_mode("").is_err());
    }

    #[test]
    fn unknown_failure_mode_through_resolve_is_invalid_usage() {
        let key = "DAGR_TEST_FAILURE_MODE_BAD";
        std::env::set_var(key, "halt");
        let err = resolve::<EnvFailureMode>(
            None,
            key,
            EnvFailureMode(FailureMode::ContinueIndependent),
        )
        .expect_err("an unknown failure mode must error");
        std::env::remove_var(key);
        assert_eq!(err.exit_code(), ExitCode::InvalidUsage);
        assert!(err.to_string().contains(key));
    }

    #[test]
    fn env_failure_mode_composes_with_resolve() {
        let key = "DAGR_TEST_FAILURE_MODE_OK";
        std::env::set_var(key, "stop-on-first-failure");
        let got = resolve::<EnvFailureMode>(
            None,
            key,
            EnvFailureMode(FailureMode::ContinueIndependent),
        )
        .expect("valid failure mode");
        std::env::remove_var(key);
        assert_eq!(got.into_inner(), FailureMode::StopOnFirstFailure);
    }

    // --- The DAGR_* constants --------------------------------------------

    #[test]
    fn dagr_env_name_constants_match_adr_089() {
        assert_eq!(DAGR_GRACE, "DAGR_GRACE");
        assert_eq!(DAGR_TEARDOWN_DEADLINE, "DAGR_TEARDOWN_DEADLINE");
        assert_eq!(DAGR_FAILURE_MODE, "DAGR_FAILURE_MODE");
        assert_eq!(DAGR_POOL_COMPUTE_THREADS, "DAGR_POOL_COMPUTE_THREADS");
        assert_eq!(DAGR_POOL_BLOCKING_THREADS, "DAGR_POOL_BLOCKING_THREADS");
        assert_eq!(DAGR_POOL_MEMORY, "DAGR_POOL_MEMORY");
        assert_eq!(DAGR_HEADROOM, "DAGR_HEADROOM");
    }
}
