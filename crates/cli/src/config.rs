//! C12/C26 · **Runtime-knob precedence** — the `flag > env > default` resolver,
//! the parsers it needs, a strict never-silent error type, the `DAGR_*` env-name
//! constants, and the extended reserved-flag namespace (ADR 089).
//!
//! # What this module owns
//!
//! ADR 089 records the operator-requested `flag > env > default` story for every
//! runtime knob, and — having read the code — refutes the assumption that a single
//! flag-parsing choke point exists: [`parse_cli`](crate::contract::parse_cli)
//! returns only the verb, runtime-flag parsing is ad-hoc per pipeline binary,
//! `RunConfig::new()` is infallible and env-free, and `dagr_core::limits`
//! deliberately reads no environment. The ADR's answer is **reusable cli-level
//! pieces wired at the binary layer**, keeping `dagr-core` environment-free. This
//! module ships the isolated building blocks of that answer:
//!
//! - [`resolve`] — the `flag > env > default` resolver over any [`FromStr`] type:
//!   a present flag wins outright (the environment is never read); with no flag,
//!   the env var is read and parsed; with neither, the `default` is returned.
//! - [`parse_duration`] — a parser for the bare `10` / `10s` / `10ms` forms (no
//!   [`FromStr`] exists for [`Duration`] on these); and
//!   [`parse_failure_mode`] — a parser for `continue-independent` /
//!   `stop-on-first-failure`. [`EnvDuration`] / [`EnvFailureMode`] wrap each so it
//!   composes with the generic [`resolve`].
//! - [`EnvParseError`] — a strict, never-silent error that carries the offending
//!   variable name and maps to a C26 exit code: a **parse failure** →
//!   [`ExitCode::InvalidUsage`], an **out-of-range** value →
//!   [`ExitCode::BootstrapFailure`] (reusing the exit-code table from
//!   [`crate::contract`]).
//! - the `DAGR_*` env-name constants ([`DAGR_GRACE`], …) for every knob in ADR
//!   089's table, alongside the existing
//!   [`DAGR_NO_BANNER`](crate::contract::NO_BANNER_ENV).
//!
//! # What this module does NOT own
//!
//! - Wiring any of this into `RunConfig`, the opt-in env-fallback builder methods,
//!   the `DAGR_POOL_*` pool-pinning, and the `--dagr.headroom-fraction` /
//!   `ContainerLimitProbe::with_headroom` knob — **T77** owns all of it and
//!   consumes exactly the surface this module lands. No `DAGR_*` variable is
//!   *read* at run time here; the constants are declared, not consumed.
//! - Any env read inside `dagr-core` — a permanent scope boundary: the core reads
//!   the host once and is injectable for tests; the CLI parses env and passes
//!   already-parsed values inward.

use std::str::FromStr;
use std::time::Duration;

use dagr_core::flow::FailureMode;

use crate::contract::ExitCode;

// ===========================================================================
// The DAGR_* environment-variable names (ADR 089's table)
// ===========================================================================

/// Environment fallback for `--grace` (the cancellation grace period, C16).
/// Snake-case per the env convention (flags stay kebab-case); documented
/// alongside [`DAGR_NO_BANNER`](crate::contract::NO_BANNER_ENV) (ADR 089).
pub const DAGR_GRACE: &str = "DAGR_GRACE";

/// Environment fallback for `--teardown-deadline` (the teardown deadline, C17).
pub const DAGR_TEARDOWN_DEADLINE: &str = "DAGR_TEARDOWN_DEADLINE";

/// Environment fallback for `--failure-mode` (the run-level failure mode, C15).
pub const DAGR_FAILURE_MODE: &str = "DAGR_FAILURE_MODE";

/// Environment fallback for `--dagr.pool.compute-threads` (the compute-thread
/// pool pin, C12).
pub const DAGR_POOL_COMPUTE_THREADS: &str = "DAGR_POOL_COMPUTE_THREADS";

/// Environment fallback for `--dagr.pool.blocking-threads` (the blocking-thread
/// pool pin, C12).
pub const DAGR_POOL_BLOCKING_THREADS: &str = "DAGR_POOL_BLOCKING_THREADS";

/// Environment fallback for `--dagr.pool.memory` (the memory pool pin, C12).
pub const DAGR_POOL_MEMORY: &str = "DAGR_POOL_MEMORY";

/// Environment fallback for `--dagr.headroom-fraction` (the admission headroom
/// fraction, C12; default `0.20`, validated `0.0..=1.0`).
pub const DAGR_HEADROOM: &str = "DAGR_HEADROOM";

// ===========================================================================
// The strict, never-silent parse error
// ===========================================================================

/// The kind of failure an [`EnvParseError`] records — the crux of ADR 089's
/// "bad env values fail loudly": a syntactic parse failure and a semantic
/// (validated) out-of-range value are **distinct causes** with distinct C26 exit
/// codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnvParseErrorKind {
    /// The value could not be parsed into the target type (a syntactic failure) —
    /// invalid usage. Maps to [`ExitCode::InvalidUsage`].
    Parse,
    /// The value parsed but failed a semantic bounds check (e.g. a headroom of
    /// `1.5` against `0.0..=1.0`) — the machine's fault at bootstrap. Maps to
    /// [`ExitCode::BootstrapFailure`].
    OutOfRange,
}

/// A `DAGR_*` environment variable carried a value that could not be used
/// (ADR 089). It **names the offending variable** so the diagnostic is actionable
/// and maps to a specific C26 exit code by [kind](EnvParseErrorKind) — an env
/// value is **never silently ignored or clamped**.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnvParseError {
    /// The offending environment variable's name (e.g. `DAGR_GRACE`).
    pub variable: String,
    /// The rejected value verbatim (for the diagnostic).
    pub value: String,
    /// Whether this was a syntactic parse failure or a semantic out-of-range
    /// value — selects the exit code.
    pub kind: EnvParseErrorKind,
    /// A human-readable detail (the underlying parse message, or the violated
    /// bound).
    pub detail: String,
}

impl EnvParseError {
    /// A **parse failure** for `variable` carrying `value` and a `detail` message
    /// — maps to [`ExitCode::InvalidUsage`].
    #[must_use]
    pub fn parse(
        variable: impl Into<String>,
        value: impl Into<String>,
        detail: impl Into<String>,
    ) -> Self {
        Self {
            variable: variable.into(),
            value: value.into(),
            kind: EnvParseErrorKind::Parse,
            detail: detail.into(),
        }
    }

    /// An **out-of-range** failure for `variable` carrying `value` and a `detail`
    /// naming the violated bound — maps to [`ExitCode::BootstrapFailure`]. Callers
    /// (T77) that apply a semantic bound to an already-parsed value produce this.
    #[must_use]
    pub fn out_of_range(
        variable: impl Into<String>,
        value: impl Into<String>,
        detail: impl Into<String>,
    ) -> Self {
        Self {
            variable: variable.into(),
            value: value.into(),
            kind: EnvParseErrorKind::OutOfRange,
            detail: detail.into(),
        }
    }

    /// The C26 [`ExitCode`] this error maps to (reusing the exit-code table in
    /// [`crate::contract`]): a [parse failure](EnvParseErrorKind::Parse) →
    /// [`ExitCode::InvalidUsage`], an
    /// [out-of-range](EnvParseErrorKind::OutOfRange) value →
    /// [`ExitCode::BootstrapFailure`].
    #[must_use]
    pub fn exit_code(&self) -> ExitCode {
        match self.kind {
            EnvParseErrorKind::Parse => ExitCode::InvalidUsage,
            EnvParseErrorKind::OutOfRange => ExitCode::BootstrapFailure,
        }
    }
}

impl std::fmt::Display for EnvParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let cause = match self.kind {
            EnvParseErrorKind::Parse => "could not be parsed",
            EnvParseErrorKind::OutOfRange => "is out of range",
        };
        write!(
            f,
            "environment variable `{}` = `{}` {}: {} (arch.md C26 / ADR 089 — bad env \
             values fail loudly and are never silently ignored)",
            self.variable, self.value, cause, self.detail
        )
    }
}

impl std::error::Error for EnvParseError {}

// ===========================================================================
// The flag > env > default resolver
// ===========================================================================

/// Resolve a runtime knob by the ADR 089 precedence **`flag > env > default`**.
///
/// - A **present flag** wins outright — the environment is **never read**
///   (`env_key` is ignored entirely).
/// - With **no flag**, the environment variable `env_key` is read and, if present
///   and non-empty, parsed via [`T::from_str`](FromStr::from_str); a value that
///   fails to parse yields an [`EnvParseError`] (a parse failure →
///   [`ExitCode::InvalidUsage`]) naming `env_key`.
/// - With **neither** a flag nor the env var (or the env var set to an empty
///   string), the supplied `default` is returned unchanged.
///
/// The environment is read (via [`std::env::var`]) only on the no-flag path, so a
/// binary that already has a flag value never touches the process environment.
/// This is a **cli-level** helper (ADR 089): `dagr-core` never reads the
/// environment; a pipeline binary parses its flag as it does today, then calls
/// this to fold in the env fallback before constructing `RunConfig` / pool pins.
///
/// # Errors
///
/// Returns [`EnvParseError`] (kind [`Parse`](EnvParseErrorKind::Parse), mapping to
/// [`ExitCode::InvalidUsage`]) naming `env_key` when the no-flag env value fails
/// `T::from_str`. Semantic bounds are the caller's (T77) to apply against the
/// parsed value via [`EnvParseError::out_of_range`].
pub fn resolve<T>(flag: Option<T>, env_key: &str, default: T) -> Result<T, EnvParseError>
where
    T: FromStr,
    T::Err: std::fmt::Display,
{
    // Flag wins outright: never consult the environment.
    if let Some(value) = flag {
        return Ok(value);
    }
    // No flag: fall back to the environment. An unset OR empty variable is "not
    // supplied" and yields the default (an empty string parses to nothing useful
    // and would only produce a confusing error).
    match std::env::var(env_key) {
        Ok(raw) if !raw.is_empty() => {
            T::from_str(&raw).map_err(|e| EnvParseError::parse(env_key, raw, e.to_string()))
        }
        _ => Ok(default),
    }
}

// ===========================================================================
// Parsers the resolver needs (no FromStr exists for these forms)
// ===========================================================================

/// Parse a duration in the bare `10` / `10s` / `10ms` forms into a [`Duration`]
/// (ADR 089). No [`FromStr`] exists for [`Duration`] on these forms, so this
/// supplies one:
///
/// - a bare integer or an `Ns` suffix → that many **seconds** (`10` and `10s`
///   both yield [`Duration::from_secs(10)`](Duration::from_secs));
/// - an `Nms` suffix → that many **milliseconds** (`10ms` →
///   [`Duration::from_millis(10)`](Duration::from_millis)).
///
/// The `ms` suffix is checked before the `s` suffix (so `10ms` is milliseconds,
/// not `10m` seconds). The numeric part must be a non-negative integer.
///
/// # Errors
///
/// Returns a [`DurationParseError`] describing why the string is not one of the
/// accepted forms (an unknown suffix, a non-integer numeric part, or an empty
/// numeric part).
pub fn parse_duration(input: &str) -> Result<Duration, DurationParseError> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err(DurationParseError {
            input: input.to_string(),
        });
    }
    // Order matters: `ms` must be tested before the single-char `s` so `10ms` is
    // milliseconds, never `10m` + a stray `s`.
    let (digits, to_duration): (&str, fn(u64) -> Duration) =
        if let Some(rest) = trimmed.strip_suffix("ms") {
            (rest, Duration::from_millis)
        } else if let Some(rest) = trimmed.strip_suffix('s') {
            (rest, Duration::from_secs)
        } else {
            (trimmed, Duration::from_secs)
        };
    match digits.parse::<u64>() {
        Ok(n) => Ok(to_duration(n)),
        Err(_) => Err(DurationParseError {
            input: input.to_string(),
        }),
    }
}

/// A string was not one of the accepted duration forms (`10` / `10s` / `10ms`).
/// Implements [`Display`](std::fmt::Display) so it flows through [`resolve`] into
/// an [`EnvParseError`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurationParseError {
    /// The offending input verbatim.
    pub input: String,
}

impl std::fmt::Display for DurationParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "`{}` is not a duration — expected one of `10`, `10s`, or `10ms` (a \
             non-negative integer of seconds, or an `s`/`ms` suffix)",
            self.input
        )
    }
}

impl std::error::Error for DurationParseError {}

/// A newtype over [`Duration`] whose [`FromStr`] accepts the `10` / `10s` /
/// `10ms` forms via [`parse_duration`], so it composes with the generic
/// [`resolve`] helper (`resolve::<EnvDuration>(…)`). Read the inner value with
/// [`into_inner`](EnvDuration::into_inner) or the [`From`] impl.
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

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse_duration(s).map(EnvDuration)
    }
}

/// Parse a [`FailureMode`] from its kebab-case token (ADR 089): accepts exactly
/// `continue-independent` → [`FailureMode::ContinueIndependent`] and
/// `stop-on-first-failure` → [`FailureMode::StopOnFirstFailure`]; any other token
/// is a parse error.
///
/// The tokens are the canonical Vocabulary spellings (arch.md C15) and match the
/// `--failure-mode` flag spelling exactly.
///
/// # Errors
///
/// Returns a [`FailureModeParseError`] naming the accepted tokens when `input` is
/// neither `continue-independent` nor `stop-on-first-failure`.
pub fn parse_failure_mode(input: &str) -> Result<FailureMode, FailureModeParseError> {
    match input.trim() {
        "continue-independent" => Ok(FailureMode::ContinueIndependent),
        "stop-on-first-failure" => Ok(FailureMode::StopOnFirstFailure),
        _ => Err(FailureModeParseError {
            input: input.to_string(),
        }),
    }
}

/// A string was not one of the accepted [`FailureMode`] tokens. Implements
/// [`Display`](std::fmt::Display) so it flows through [`resolve`] into an
/// [`EnvParseError`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailureModeParseError {
    /// The offending input verbatim.
    pub input: String,
}

impl std::fmt::Display for FailureModeParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "`{}` is not a failure mode — expected `continue-independent` or \
             `stop-on-first-failure`",
            self.input
        )
    }
}

impl std::error::Error for FailureModeParseError {}

/// A newtype over [`FailureMode`] whose [`FromStr`] delegates to
/// [`parse_failure_mode`], so it composes with the generic [`resolve`] helper
/// (`resolve::<EnvFailureMode>(…)`).
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

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse_failure_mode(s).map(EnvFailureMode)
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
        assert_eq!(
            got, 42,
            "with no flag, the env value is parsed and returned"
        );
    }

    #[test]
    fn neither_flag_nor_env_returns_default() {
        let key = "DAGR_TEST_DEFAULT_UNSET";
        std::env::remove_var(key); // ensure unset
        let got = resolve::<u32>(None, key, 13).expect("the default path never errors");
        assert_eq!(
            got, 13,
            "with neither flag nor env, the default is returned"
        );
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
        assert_eq!(
            parse_duration("10ms").expect("ms"),
            Duration::from_millis(10)
        );
    }

    #[test]
    fn duration_rejects_garbage() {
        assert!(parse_duration("").is_err());
        assert!(parse_duration("abc").is_err());
        assert!(
            parse_duration("10m").is_err(),
            "minutes are not an accepted form"
        );
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
        let err =
            resolve::<EnvFailureMode>(None, key, EnvFailureMode(FailureMode::ContinueIndependent))
                .expect_err("an unknown failure mode must error");
        std::env::remove_var(key);
        assert_eq!(err.exit_code(), ExitCode::InvalidUsage);
        assert!(err.to_string().contains(key));
    }

    #[test]
    fn env_failure_mode_composes_with_resolve() {
        let key = "DAGR_TEST_FAILURE_MODE_OK";
        std::env::set_var(key, "stop-on-first-failure");
        let got =
            resolve::<EnvFailureMode>(None, key, EnvFailureMode(FailureMode::ContinueIndependent))
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
