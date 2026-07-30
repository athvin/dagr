//! **Runtime-knob precedence** — the `flag > env > default` resolver,
//! the parsers it needs, a strict never-silent error type, the `DAGR_*` env-name
//! constants, and the extended reserved-flag namespace.
//!
//! # What this module owns
//!
//! The `flag > env > default` story applies to every runtime knob. There is no
//! single flag-parsing choke point: [`parse_cli`](crate::contract::parse_cli)
//! returns only the verb, runtime-flag parsing is ad-hoc per pipeline binary,
//! `RunConfig::new()` is infallible and env-free, and `dagr_core::limits`
//! deliberately reads no environment. The answer is **reusable cli-level pieces
//! wired at the binary layer**, keeping `dagr-core` environment-free. This module
//! ships the isolated building blocks of that answer:
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
//!   variable name and maps to an exit code: a **parse failure** →
//!   [`ExitCode::InvalidUsage`], an **out-of-range** value →
//!   [`ExitCode::BootstrapFailure`] (reusing the exit-code table from
//!   [`crate::contract`]).
//! - the `DAGR_*` env-name constants ([`DAGR_GRACE`], …) for every knob, alongside
//!   the existing [`DAGR_NO_BANNER`](crate::contract::NO_BANNER_ENV).
//!
//! # What this module does NOT own
//!
//! - Wiring any of this into `RunConfig`, the opt-in env-fallback builder methods,
//!   the `DAGR_POOL_*` pool-pinning, and the `--dagr.headroom-fraction` /
//!   `ContainerLimitProbe::with_headroom` knob — the binary layer consumes exactly
//!   the surface this module lands. No `DAGR_*` variable is *read* at run time here;
//!   the constants are declared, not consumed.
//! - Any env read inside `dagr-core` — a permanent scope boundary: the core reads
//!   the host once and is injectable for tests; the CLI parses env and passes
//!   already-parsed values inward.

use std::str::FromStr;
use std::time::Duration;

use dagr_core::flow::FailureMode;
use dagr_core::limits::PinnedPools;

use crate::contract::ExitCode;

// ===========================================================================
// The DAGR_* environment-variable names
// ===========================================================================

/// Environment fallback for `--grace` (the cancellation grace period).
/// Snake-case per the env convention (flags stay kebab-case); documented
/// alongside [`DAGR_NO_BANNER`](crate::contract::NO_BANNER_ENV).
pub const DAGR_GRACE: &str = "DAGR_GRACE";

/// Environment fallback for `--teardown-deadline` (the teardown deadline).
pub const DAGR_TEARDOWN_DEADLINE: &str = "DAGR_TEARDOWN_DEADLINE";

/// Environment fallback for `--failure-mode` (the run-level failure mode).
pub const DAGR_FAILURE_MODE: &str = "DAGR_FAILURE_MODE";

/// Environment fallback for `--dagr.pool.compute-threads` (the compute-thread
/// pool pin).
pub const DAGR_POOL_COMPUTE_THREADS: &str = "DAGR_POOL_COMPUTE_THREADS";

/// Environment fallback for `--dagr.pool.blocking-threads` (the blocking-thread
/// pool pin).
pub const DAGR_POOL_BLOCKING_THREADS: &str = "DAGR_POOL_BLOCKING_THREADS";

/// Environment fallback for `--dagr.pool.memory` (the memory pool pin).
pub const DAGR_POOL_MEMORY: &str = "DAGR_POOL_MEMORY";

/// Environment fallback for `--dagr.headroom-fraction` (the admission headroom
/// fraction; default `0.20`, validated `0.0..=1.0`).
pub const DAGR_HEADROOM: &str = "DAGR_HEADROOM";

/// Environment fallback for `--dagr.metastore` (the M7 live run-index tee toggle,
/// T86). A truthy value turns the guaranteed live metastore tee sink on; the
/// **default is off** (no `libsql` activity, no behavior change). Resolved by the
/// standard `flag > env > default` precedence ([`resolve_metastore_toggle`]); the
/// wiring itself is behind the default-off `metastore` cargo feature.
pub const DAGR_METASTORE: &str = "DAGR_METASTORE";

// ===========================================================================
// The strict, never-silent parse error
// ===========================================================================

/// The kind of failure an [`EnvParseError`] records — the crux of "bad env values
/// fail loudly": a syntactic parse failure and a semantic (validated) out-of-range
/// value are **distinct causes** with distinct exit codes.
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

/// A `DAGR_*` environment variable carried a value that could not be used. It
/// **names the offending variable** so the diagnostic is actionable and maps to a
/// specific exit code by [kind](EnvParseErrorKind) — an env value is **never
/// silently ignored or clamped**.
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
    /// that apply a semantic bound to an already-parsed value produce this.
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

    /// The [`ExitCode`] this error maps to (reusing the exit-code table in
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

/// Resolve a runtime knob by the precedence **`flag > env > default`**.
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
/// This is a **cli-level** helper: `dagr-core` never reads the environment; a
/// pipeline binary parses its flag as it does today, then calls this to fold in the
/// env fallback before constructing `RunConfig` / pool pins.
///
/// # Errors
///
/// Returns [`EnvParseError`] (kind [`Parse`](EnvParseErrorKind::Parse), mapping to
/// [`ExitCode::InvalidUsage`]) naming `env_key` when the no-flag env value fails
/// `T::from_str`. Semantic bounds are the caller's to apply against the parsed value
/// via [`EnvParseError::out_of_range`].
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

/// Parse a duration in the bare `10` / `10s` / `10ms` forms into a [`Duration`].
/// No [`FromStr`] exists for [`Duration`] on these forms, so this supplies one:
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

/// Parse a [`FailureMode`] from its kebab-case token: accepts exactly
/// `continue-independent` → [`FailureMode::ContinueIndependent`] and
/// `stop-on-first-failure` → [`FailureMode::StopOnFirstFailure`]; any other token
/// is a parse error.
///
/// The tokens are the canonical spellings and match the `--failure-mode` flag
/// spelling exactly.
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

// ===========================================================================
// The CLI pool-pinning layer — DAGR_POOL_* → PinnedPools
// ===========================================================================

/// The already-parsed **pool-pin flags** a pipeline binary passes to
/// [`resolve_pool_pins`].
///
/// Each field is the flag value if the operator supplied one, else [`None`]. A
/// present flag wins outright over the matching `DAGR_POOL_*` variable (the
/// `flag > env > default` rule); with no flag, the environment is consulted; with
/// neither, that pool is left un-pinned (it derives from the container-limit
/// probe). This mirrors what a binary already does for its other flags — it parses
/// them, then folds in the env fallback here — keeping `dagr-core` environment-free
/// (the resolved values are handed to the core [`PinnedPools`] as parsed pins).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PoolPinFlags {
    /// The `--dagr.pool.compute-threads` flag value, if supplied.
    pub compute_threads: Option<u32>,
    /// The `--dagr.pool.blocking-threads` flag value, if supplied.
    pub blocking_threads: Option<u32>,
    /// The `--dagr.pool.memory` flag value (bytes), if supplied.
    pub memory: Option<u64>,
}

/// Resolve the three `DAGR_POOL_*` pins by the precedence
/// **`flag > env > default`** and fold them into a core [`PinnedPools`].
///
/// For each pool: a present flag wins outright (the env is never read); with no
/// flag, `DAGR_POOL_COMPUTE_THREADS` / `DAGR_POOL_BLOCKING_THREADS` /
/// `DAGR_POOL_MEMORY` is read and parsed; with neither, the pool is left un-pinned
/// (it will derive from the [`ContainerLimitProbe`](dagr_core::limits::ContainerLimitProbe)).
/// The environment is resolved **here in `dagr-cli`** and handed to the core
/// [`PinnedPools`] as parsed pins — `dagr-core` reads no environment (a load-bearing
/// boundary).
///
/// # Errors
///
/// Returns an [`EnvParseError`] (kind [`Parse`](EnvParseErrorKind::Parse) →
/// [`ExitCode::InvalidUsage`]) naming the offending `DAGR_POOL_*` variable when its
/// value fails to parse as a non-negative integer — a bad env value is **never**
/// silently ignored or clamped.
pub fn resolve_pool_pins(flags: PoolPinFlags) -> Result<PinnedPools, EnvParseError> {
    let mut pins = PinnedPools::new();
    if let Some(compute) = resolve_opt::<u32>(flags.compute_threads, DAGR_POOL_COMPUTE_THREADS)? {
        pins = pins.compute_threads(compute);
    }
    if let Some(blocking) = resolve_opt::<u32>(flags.blocking_threads, DAGR_POOL_BLOCKING_THREADS)?
    {
        pins = pins.blocking_threads(blocking);
    }
    if let Some(memory) = resolve_opt::<u64>(flags.memory, DAGR_POOL_MEMORY)? {
        pins = pins.memory(memory);
    }
    Ok(pins)
}

/// Resolve an **optional** knob by `flag > env > (nothing)`: a present flag wins
/// outright; with no flag the env var `env_key` is read and parsed (an unset or
/// empty variable is "not supplied" → [`None`], so the caller leaves the pool
/// un-pinned); a value that fails to parse is a loud [`EnvParseError`].
///
/// This is the "no default" sibling of [`resolve`]: a pool with neither a flag nor
/// an env value has *no* pin (it derives from detection), which a plain
/// `resolve(_, _, default)` cannot express.
fn resolve_opt<T>(flag: Option<T>, env_key: &str) -> Result<Option<T>, EnvParseError>
where
    T: FromStr,
    T::Err: std::fmt::Display,
{
    if let Some(value) = flag {
        return Ok(Some(value));
    }
    match std::env::var(env_key) {
        Ok(raw) if !raw.is_empty() => T::from_str(&raw)
            .map(Some)
            .map_err(|e| EnvParseError::parse(env_key, raw, e.to_string())),
        _ => Ok(None),
    }
}

// ===========================================================================
// The headroom knob — DAGR_HEADROOM / --dagr.headroom-fraction
// ===========================================================================

/// The default admission **headroom fraction** (20%), mirrored here so the CLI
/// resolves `--dagr.headroom-fraction` / `DAGR_HEADROOM` to the same default
/// `dagr-core`'s [`HEADROOM_DEFAULT`](dagr_core::limits::HEADROOM_DEFAULT) applies
/// when no knob is set.
pub const HEADROOM_DEFAULT: f64 = 0.20;

/// Resolve the admission **headroom fraction** by `flag > env > default` and
/// **validate it to `0.0..=1.0`**. The resolved value is handed to
/// [`ContainerLimitProbe::with_headroom`](dagr_core::limits::ContainerLimitProbe::with_headroom);
/// the existing at-least-one-unit floor is unchanged, so even a `1.0` headroom
/// still yields one unit per pool.
///
/// - A present `flag` wins outright (the env is never read).
/// - With no flag, `DAGR_HEADROOM` is read and parsed as an `f64`.
/// - With neither, the [`HEADROOM_DEFAULT`] (0.20) is returned.
///
/// # Errors
///
/// Two **distinct** loud failures, each naming `DAGR_HEADROOM`:
/// - a value that is not a float → an [`EnvParseError`] of kind
///   [`Parse`](EnvParseErrorKind::Parse), mapping to [`ExitCode::InvalidUsage`];
/// - a float **outside `0.0..=1.0`** → an [`EnvParseError`] of kind
///   [`OutOfRange`](EnvParseErrorKind::OutOfRange), mapping to
///   [`ExitCode::BootstrapFailure`] — the value is never silently clamped.
///
/// A bad **flag** value is validated the same way (out-of-range → an
/// `OutOfRange` error naming `--dagr.headroom-fraction`), so the two paths agree.
pub fn resolve_headroom(flag: Option<f64>) -> Result<f64, EnvParseError> {
    // A present flag wins outright; validate its range against the same bound.
    if let Some(fraction) = flag {
        return validate_headroom("--dagr.headroom-fraction", fraction, &fraction.to_string());
    }
    // No flag: fall back to DAGR_HEADROOM (unset/empty → the default).
    match std::env::var(DAGR_HEADROOM) {
        Ok(raw) if !raw.is_empty() => {
            let parsed: f64 = raw.parse().map_err(|e: std::num::ParseFloatError| {
                EnvParseError::parse(DAGR_HEADROOM, &raw, e.to_string())
            })?;
            validate_headroom(DAGR_HEADROOM, parsed, &raw)
        }
        _ => Ok(HEADROOM_DEFAULT),
    }
}

/// Check `fraction` is in `0.0..=1.0`; otherwise an [`out_of_range`](EnvParseError::out_of_range)
/// error naming `source` (`BootstrapFailure`).
fn validate_headroom(source: &str, fraction: f64, raw: &str) -> Result<f64, EnvParseError> {
    if (0.0..=1.0).contains(&fraction) {
        Ok(fraction)
    } else {
        Err(EnvParseError::out_of_range(
            source,
            raw,
            "expected a fraction in 0.0..=1.0",
        ))
    }
}

// ===========================================================================
// The metastore live-tee toggle — DAGR_METASTORE / --dagr.metastore
// ===========================================================================

/// A boolean runtime toggle parsed from its env/flag string via the standard
/// truthy set. Wraps `bool` so it composes with the generic [`resolve`] helper
/// (`resolve::<EnvBool>(…)`); read the inner value with
/// [`into_inner`](EnvBool::into_inner).
///
/// Accepted (case-insensitive): `1`/`true`/`yes`/`on` → `true`;
/// `0`/`false`/`no`/`off` → `false`. Any other token is a loud parse error (never
/// silently treated as false), per the never-silent env contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EnvBool(pub bool);

impl EnvBool {
    /// The wrapped `bool`.
    #[must_use]
    pub fn into_inner(self) -> bool {
        self.0
    }
}

impl From<EnvBool> for bool {
    fn from(value: EnvBool) -> Self {
        value.0
    }
}

impl FromStr for EnvBool {
    type Err = BoolParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Ok(EnvBool(true)),
            "0" | "false" | "no" | "off" => Ok(EnvBool(false)),
            _ => Err(BoolParseError {
                input: s.to_string(),
            }),
        }
    }
}

/// A string was not one of the accepted boolean tokens. Implements
/// [`Display`](std::fmt::Display) so it flows through [`resolve`] into an
/// [`EnvParseError`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoolParseError {
    /// The offending input verbatim.
    pub input: String,
}

impl std::fmt::Display for BoolParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "`{}` is not a boolean — expected one of `1`/`true`/`yes`/`on` or \
             `0`/`false`/`no`/`off`",
            self.input
        )
    }
}

impl std::error::Error for BoolParseError {}

/// Resolve the **metastore live-tee toggle** by `flag > env > default` (default
/// **off**): a present `--dagr.metastore` flag wins outright (the env is never
/// read); with no flag, `DAGR_METASTORE` is read and parsed as a boolean; with
/// neither, the toggle is off. A bad env value fails loudly (never silently
/// treated as off).
///
/// This resolves the *toggle*; the store path is resolved separately (default
/// under the run store), and the whole wiring is behind the default-off
/// `metastore` cargo feature — so `--no-default-features` omits it entirely.
///
/// # Errors
/// Returns an [`EnvParseError`] (kind [`Parse`](EnvParseErrorKind::Parse) →
/// [`ExitCode::InvalidUsage`]) naming `DAGR_METASTORE` when its value is not a
/// recognized boolean token.
pub fn resolve_metastore_toggle(flag: Option<bool>) -> Result<bool, EnvParseError> {
    let resolved = resolve::<EnvBool>(flag.map(EnvBool), DAGR_METASTORE, EnvBool(false))?;
    Ok(resolved.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;

    // # Env hermeticity, and why mutating it needs `unsafe`
    //
    // Most env-touching tests below use a UNIQUE variable name (never a real
    // `DAGR_*` name), set and removed within the test, so no two tests contend
    // logically over the same variable under cargo's parallel runner. The pool /
    // headroom / metastore resolvers hardcode the REAL names, so those tests
    // cannot use a unique key.
    //
    // Neither of those addresses what `std::env::set_var` is actually unsafe about
    // in edition 2024, which is not *logical* interference between tests but a data
    // race: mutating the process environment can reallocate the `environ` array
    // under a concurrent `getenv` in another thread, whatever variable that thread
    // is reading. So every mutation goes through [`set_env`] / [`unset_env`], which
    // take the module's [`env_lock`] guard **by reference** — the compiler will not
    // let a mutation happen without the lock held — and each test holds that guard
    // across its whole set → read → remove window, so no reader in this module can
    // observe a half-updated environment.
    //
    // The residual, stated rather than hidden: another module's test in the same
    // `dagr-cli` test binary that *reads* the environment (`OutputMode::from_env`,
    // the driver's allowlist lookup) does not take this lock, so a mutation here
    // can in principle race it. That is accepted for test-only code; nothing in
    // the shipped binary mutates the environment at all.

    /// The guard type [`set_env`] / [`unset_env`] demand, so "the lock is held"
    /// is a type-checked precondition rather than a comment.
    type EnvGuard = std::sync::MutexGuard<'static, ()>;

    /// The single process-global lock every env-mutating test in this module holds
    /// across its set → read → remove window.
    fn env_lock() -> EnvGuard {
        static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
        LOCK.get_or_init(|| std::sync::Mutex::new(()))
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Set `key` to `value` while the caller holds the env lock.
    #[expect(
        unsafe_code,
        reason = "std::env::set_var is an unsafe fn in edition 2024; the resolver \
                  under test reads the real process environment, so a test cannot \
                  avoid mutating it — this wrapper confines the unsafety to one place"
    )]
    fn set_env(_locked: &EnvGuard, key: &str, value: &str) {
        // SAFETY: `set_var` is unsafe because the update can reallocate `environ`
        // under a concurrent `getenv`. `_locked` proves the caller holds the
        // module's process-global env lock, and every env *read* in this module
        // happens inside that same guarded window, so no reader here can see a
        // half-updated environment. This is test-only code; the shipped binary
        // never mutates the environment.
        unsafe { std::env::set_var(key, value) };
    }

    /// Remove `key` while the caller holds the env lock.
    #[expect(
        unsafe_code,
        reason = "std::env::remove_var is an unsafe fn in edition 2024; see set_env"
    )]
    fn unset_env(_locked: &EnvGuard, key: &str) {
        // SAFETY: identical to `set_env` above — `_locked` proves the env lock is
        // held, and every read in this module is inside the same guarded window.
        unsafe { std::env::remove_var(key) };
    }

    // --- Precedence -------------------------------------------------------

    #[test]
    fn flag_present_wins_and_env_is_never_read() {
        let g = env_lock();
        let key = "DAGR_TEST_FLAG_WINS";
        set_env(&g, key, "999");
        // Flag present → the env value (999) must be ignored entirely.
        let got = resolve::<u32>(Some(7), key, 0).expect("flag path never errors");
        unset_env(&g, key);
        assert_eq!(got, 7, "a present flag must win outright over the env var");
    }

    #[test]
    fn no_flag_uses_parsed_env_value() {
        let g = env_lock();
        let key = "DAGR_TEST_ENV_USED";
        set_env(&g, key, "42");
        let got = resolve::<u32>(None, key, 0).expect("a valid env value parses");
        unset_env(&g, key);
        assert_eq!(
            got, 42,
            "with no flag, the env value is parsed and returned"
        );
    }

    #[test]
    fn neither_flag_nor_env_returns_default() {
        let g = env_lock();
        let key = "DAGR_TEST_DEFAULT_UNSET";
        unset_env(&g, key); // ensure unset
        let got = resolve::<u32>(None, key, 13).expect("the default path never errors");
        assert_eq!(
            got, 13,
            "with neither flag nor env, the default is returned"
        );
    }

    #[test]
    fn empty_env_is_treated_as_unset() {
        let g = env_lock();
        let key = "DAGR_TEST_EMPTY_ENV";
        set_env(&g, key, "");
        let got = resolve::<u32>(None, key, 5).expect("an empty env var is not-supplied");
        unset_env(&g, key);
        assert_eq!(got, 5, "an empty env var behaves as unset → default");
    }

    // --- Parsing & errors -------------------------------------------------

    #[test]
    fn unparseable_env_value_is_invalid_usage_and_names_the_variable() {
        let g = env_lock();
        let key = "DAGR_TEST_BAD_PARSE";
        set_env(&g, key, "not-a-number");
        let err = resolve::<u32>(None, key, 0).expect_err("a bad env value must error");
        unset_env(&g, key);
        assert_eq!(err.exit_code(), ExitCode::InvalidUsage);
        assert!(
            err.to_string().contains(key),
            "the Display must name the offending variable, got: {err}"
        );
    }

    #[test]
    fn out_of_range_maps_to_bootstrap_failure_and_names_the_variable() {
        // A syntactically valid but out-of-range value (headroom 1.5 vs 0.0..=1.0).
        // The bound is the caller's to apply; this exercises the error the
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
        let g = env_lock();
        let key = "DAGR_TEST_DURATION";
        set_env(&g, key, "250ms");
        let got = resolve::<EnvDuration>(None, key, EnvDuration(Duration::from_secs(1)))
            .expect("valid duration");
        unset_env(&g, key);
        assert_eq!(got.into_inner(), Duration::from_millis(250));
    }

    #[test]
    fn env_duration_bad_value_is_invalid_usage() {
        let g = env_lock();
        let key = "DAGR_TEST_DURATION_BAD";
        set_env(&g, key, "notaduration");
        let err = resolve::<EnvDuration>(None, key, EnvDuration(Duration::from_secs(1)))
            .expect_err("a bad duration must error");
        unset_env(&g, key);
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
        let g = env_lock();
        let key = "DAGR_TEST_FAILURE_MODE_BAD";
        set_env(&g, key, "halt");
        let err =
            resolve::<EnvFailureMode>(None, key, EnvFailureMode(FailureMode::ContinueIndependent))
                .expect_err("an unknown failure mode must error");
        unset_env(&g, key);
        assert_eq!(err.exit_code(), ExitCode::InvalidUsage);
        assert!(err.to_string().contains(key));
    }

    #[test]
    fn env_failure_mode_composes_with_resolve() {
        let g = env_lock();
        let key = "DAGR_TEST_FAILURE_MODE_OK";
        set_env(&g, key, "stop-on-first-failure");
        let got =
            resolve::<EnvFailureMode>(None, key, EnvFailureMode(FailureMode::ContinueIndependent))
                .expect("valid failure mode");
        unset_env(&g, key);
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

    // --- Pool pins + headroom resolvers ----------------------------------
    //
    // These read the REAL DAGR_POOL_* / DAGR_HEADROOM names, so they cannot use a
    // unique key the way the generic-resolver tests above do; they rely on
    // `env_lock` alone (which every test in this module holds anyway).

    #[test]
    fn pool_pins_flag_beats_env_and_env_fills_the_rest() {
        let g = env_lock();
        set_env(&g, DAGR_POOL_COMPUTE_THREADS, "9");
        set_env(&g, DAGR_POOL_MEMORY, "4096");
        unset_env(&g, DAGR_POOL_BLOCKING_THREADS);
        let pins = resolve_pool_pins(PoolPinFlags {
            compute_threads: Some(2),
            ..PoolPinFlags::default()
        })
        .expect("valid pins");
        unset_env(&g, DAGR_POOL_COMPUTE_THREADS);
        unset_env(&g, DAGR_POOL_MEMORY);
        assert_eq!(pins.compute_threads_pin(), Some(2), "flag wins over env");
        assert_eq!(pins.memory_pin(), Some(4096), "env fills memory");
        assert_eq!(pins.blocking_threads_pin(), None, "unset stays un-pinned");
    }

    #[test]
    fn pool_pins_bad_env_is_invalid_usage_naming_the_variable() {
        let g = env_lock();
        set_env(&g, DAGR_POOL_MEMORY, "notanumber");
        let err = resolve_pool_pins(PoolPinFlags::default()).expect_err("bad pool env fails");
        unset_env(&g, DAGR_POOL_MEMORY);
        assert_eq!(err.kind, EnvParseErrorKind::Parse);
        assert_eq!(err.exit_code(), ExitCode::InvalidUsage);
        assert!(err.to_string().contains(DAGR_POOL_MEMORY));
    }

    #[test]
    fn headroom_flag_env_default_precedence() {
        let g = env_lock();
        // default when neither
        unset_env(&g, DAGR_HEADROOM);
        assert!((resolve_headroom(None).expect("default") - HEADROOM_DEFAULT).abs() < f64::EPSILON);
        // env used when no flag
        set_env(&g, DAGR_HEADROOM, "0.5");
        assert!((resolve_headroom(None).expect("env") - 0.5).abs() < f64::EPSILON);
        // flag beats env
        assert!((resolve_headroom(Some(0.1)).expect("flag") - 0.1).abs() < f64::EPSILON);
        unset_env(&g, DAGR_HEADROOM);
    }

    #[test]
    fn headroom_out_of_range_is_bootstrap_failure() {
        let g = env_lock();
        set_env(&g, DAGR_HEADROOM, "1.5");
        let err = resolve_headroom(None).expect_err("1.5 is out of range");
        unset_env(&g, DAGR_HEADROOM);
        assert_eq!(err.kind, EnvParseErrorKind::OutOfRange);
        assert_eq!(err.exit_code(), ExitCode::BootstrapFailure);
        assert!(err.to_string().contains(DAGR_HEADROOM));
    }

    #[test]
    fn headroom_out_of_range_flag_is_bootstrap_failure_naming_the_flag() {
        let g = env_lock();
        unset_env(&g, DAGR_HEADROOM);
        let err = resolve_headroom(Some(-0.5)).expect_err("negative headroom is out of range");
        assert_eq!(err.kind, EnvParseErrorKind::OutOfRange);
        assert_eq!(err.exit_code(), ExitCode::BootstrapFailure);
        assert!(err.to_string().contains("--dagr.headroom-fraction"));
    }

    #[test]
    fn headroom_non_float_env_is_parse_failure() {
        let g = env_lock();
        set_env(&g, DAGR_HEADROOM, "half");
        let err = resolve_headroom(None).expect_err("a non-float is a parse failure");
        unset_env(&g, DAGR_HEADROOM);
        assert_eq!(err.kind, EnvParseErrorKind::Parse);
        assert_eq!(err.exit_code(), ExitCode::InvalidUsage);
    }

    // --- The metastore live-tee toggle (T86) -----------------------------

    #[test]
    fn env_bool_parses_the_truthy_and_falsy_tokens() {
        for t in ["1", "true", "TRUE", "yes", "on", " On "] {
            assert!(t.parse::<EnvBool>().expect("truthy").into_inner(), "{t}");
        }
        for f in ["0", "false", "FALSE", "no", "off"] {
            assert!(!f.parse::<EnvBool>().expect("falsy").into_inner(), "{f}");
        }
        assert!(
            "maybe".parse::<EnvBool>().is_err(),
            "garbage is a loud error"
        );
    }

    #[test]
    fn metastore_toggle_defaults_off() {
        let g = env_lock();
        unset_env(&g, DAGR_METASTORE);
        assert!(
            !resolve_metastore_toggle(None).expect("default off"),
            "the toggle defaults OFF (no flag, no env)"
        );
    }

    #[test]
    fn metastore_toggle_env_used_when_no_flag() {
        let g = env_lock();
        set_env(&g, DAGR_METASTORE, "1");
        let on = resolve_metastore_toggle(None).expect("env used");
        unset_env(&g, DAGR_METASTORE);
        assert!(on, "with no flag, the env value turns the toggle on");
    }

    #[test]
    fn metastore_toggle_flag_beats_env() {
        let g = env_lock();
        // Env says ON, flag says OFF — the flag must win (env never read on the
        // flag path).
        set_env(&g, DAGR_METASTORE, "1");
        let resolved = resolve_metastore_toggle(Some(false)).expect("flag wins");
        unset_env(&g, DAGR_METASTORE);
        assert!(!resolved, "a present flag wins outright over the env var");
    }

    #[test]
    fn metastore_toggle_bad_env_is_invalid_usage_naming_the_variable() {
        let g = env_lock();
        set_env(&g, DAGR_METASTORE, "notabool");
        let err = resolve_metastore_toggle(None).expect_err("a bad env value fails loudly");
        unset_env(&g, DAGR_METASTORE);
        assert_eq!(err.exit_code(), ExitCode::InvalidUsage);
        assert!(err.to_string().contains(DAGR_METASTORE));
    }
}
