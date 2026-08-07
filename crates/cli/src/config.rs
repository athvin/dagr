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

/// Environment fallback for `--store` (the run-store base). The store base is the
/// one piece of infrastructure the tool asks an operator to supply, which makes it
/// the knob that most needs to be settable once in an orchestrator's environment;
/// the flag still wins outright and the default
/// ([`DEFAULT_STORE_BASE`](crate::run_store::DEFAULT_STORE_BASE)) is unchanged.
pub const DAGR_STORE: &str = "DAGR_STORE";

/// Environment fallback for `--dagr.metastore` (the M7 live run-index tee toggle,
/// T86). A truthy value turns the guaranteed live metastore tee sink on; the
/// **default is off** (no `libsql` activity, no behavior change). Resolved by the
/// standard `flag > env > default` precedence ([`resolve_metastore_toggle`]); the
/// wiring itself is behind the default-off `metastore` cargo feature.
pub const DAGR_METASTORE: &str = "DAGR_METASTORE";

/// Environment fallback for `--dagr.force-roundtrip` (the M10 local codec check,
/// T103). A truthy value makes every `Payload`-bounded handoff on the **local**
/// executor encode and decode, so a codec bug is catchable without a cluster; the
/// **default is off**, which leaves the in-memory fast path untouched (no encode
/// call at all). Resolved by the standard `flag > env > default` precedence
/// ([`resolve_force_roundtrip`]).
pub const DAGR_FORCE_ROUNDTRIP: &str = "DAGR_FORCE_ROUNDTRIP";

/// Environment fallback for `--dagr.executor` (which executor runs this
/// invocation's node attempts, M10, T105). Accepts the closed set
/// [`ExecutorKind::ALL`](crate::executor::ExecutorKind::ALL) — `local` | `k8s` —
/// and **defaults to `local`**, so a binary with no knob set runs entirely
/// in-process exactly as it always has. An unrecognized value fails loudly
/// ([`resolve_executor`]); it is never resolved to the default.
pub const DAGR_EXECUTOR: &str = "DAGR_EXECUTOR";

/// Environment fallback for `--dagr.max-pods` (the flat operator ceiling on
/// concurrently-placed node attempts, M10, T105 — the
/// [`RemoteSlots`](dagr_core::admission::Pool::RemoteSlots) pool's capacity).
///
/// The default is [`MAX_PODS_DEFAULT`] — **no dagr-side ceiling**. This is a flat
/// operator limit, not a model of anyone's cluster: dagr does not know the capacity
/// on the other side and does not second-guess it, so it invents no number of its
/// own and leaves the pool unconstrained until an operator states one.
pub const DAGR_MAX_PODS: &str = "DAGR_MAX_PODS";

/// Environment fallback for `--dagr.pod-launch-retries` (the **infrastructure**
/// retry budget, M10, T108 — ADR 115 §6).
///
/// It counts a different thing from `NodePolicy::retries`, which is why it is a
/// different knob. A pod that never *started* — unschedulable, an image that will
/// not pull, a quota rejection — executed nothing, and charging it against the
/// node's retry budget would let a cluster at capacity burn that budget without
/// running the task once. Launches charged here emit **no user-visible attempt**:
/// the driver never sees a failed attempt and the artifact shows no phantom try.
///
/// The default is [`POD_LAUNCH_RETRIES_DEFAULT`].
pub const DAGR_POD_LAUNCH_RETRIES: &str = "DAGR_POD_LAUNCH_RETRIES";

/// Environment fallback for `--dagr.blob.endpoint` (M10, T110): the base URL of
/// the S3-compatible object store. Unset means AWS's endpoint for the configured
/// region, so an S3-compatible store that is **not** AWS works by pointing this at
/// it, with no code change.
pub const DAGR_BLOB_ENDPOINT: &str = "DAGR_BLOB_ENDPOINT";

/// Environment fallback for `--dagr.blob.bucket` (M10, T110): the bucket
/// intermediate blobs live in. There is no default — a bucket is a name only the
/// operator knows.
pub const DAGR_BLOB_BUCKET: &str = "DAGR_BLOB_BUCKET";

/// Environment fallback for `--dagr.blob.region` (M10, T110): the region requests
/// are **signed** for. Defaults to [`BLOB_REGION_DEFAULT`], which every
/// S3-compatible implementation accepts when it has no regions of its own.
pub const DAGR_BLOB_REGION: &str = "DAGR_BLOB_REGION";

/// Environment fallback for `--dagr.blob.prefix` (M10, T110): the key prefix
/// within the bucket. Empty by default; a prefix lets one bucket hold more than
/// one dagr installation without either seeing the other's blobs.
pub const DAGR_BLOB_PREFIX: &str = "DAGR_BLOB_PREFIX";

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

// ===========================================================================
// The local codec check — DAGR_FORCE_ROUNDTRIP / --dagr.force-roundtrip
// ===========================================================================

/// The library-owned flag that forces the local codec round trip
/// (`--dagr.force-roundtrip`). Reserved in [`reserved_flag_names`](crate::contract::reserved_flag_names),
/// so a pipeline parameter can never shadow it.
pub const FORCE_ROUNDTRIP_FLAG: &str = "--dagr.force-roundtrip";

/// Resolve the **force-round-trip toggle** by `flag > env > default` (default
/// **off**): a present `--dagr.force-roundtrip` flag wins outright (the env is never
/// read); with no flag, [`DAGR_FORCE_ROUNDTRIP`] is read and parsed as a boolean;
/// with neither, the toggle is off. A bad env value fails loudly (never silently
/// treated as off).
///
/// On, every `Payload`-bounded handoff the run-a-Flow path drives encodes and decodes
/// locally, so a codec defect surfaces as a failed node here rather than in a cluster.
/// Off — the default — nothing is encoded and the in-memory fast path is exactly what
/// it was.
///
/// # Errors
/// Returns an [`EnvParseError`] (kind [`Parse`](EnvParseErrorKind::Parse) →
/// [`ExitCode::InvalidUsage`]) naming [`DAGR_FORCE_ROUNDTRIP`] when its value is not a
/// recognized boolean token.
pub fn resolve_force_roundtrip(flag: Option<bool>) -> Result<bool, EnvParseError> {
    let resolved = resolve::<EnvBool>(flag.map(EnvBool), DAGR_FORCE_ROUNDTRIP, EnvBool(false))?;
    Ok(resolved.into_inner())
}

/// Parse the `--dagr.force-roundtrip` flag out of a raw invocation.
///
/// Absent → [`None`] (fall through to the env/default); a **bare**
/// `--dagr.force-roundtrip` → `Some(true)`; `--dagr.force-roundtrip=<bool>` or
/// `--dagr.force-roundtrip <bool>` → the parsed boolean. This mirrors the
/// `--dagr.metastore` toggle's grammar exactly, so the two library toggles read the
/// same way.
///
/// # Errors
/// Returns the diagnostic message when a `=<value>` form carries something that is
/// not a boolean token — a bad toggle value fails loudly, never silently off. (A
/// *separate* value that does not parse is treated as a different argument, so
/// `--dagr.force-roundtrip run` is the bare form followed by a verb.)
pub fn parse_force_roundtrip_flag(argv: &[std::ffi::OsString]) -> Result<Option<bool>, String> {
    let eq_prefix = format!("{FORCE_ROUNDTRIP_FLAG}=");
    let mut it = argv.iter().peekable();
    while let Some(arg) = it.next() {
        let Some(s) = arg.to_str() else { continue };
        if s == FORCE_ROUNDTRIP_FLAG {
            // A following value that parses as a bool belongs to the flag; anything
            // else means the bare flag ("on") followed by an unrelated argument.
            if let Some(next) = it.peek().and_then(|a| a.to_str())
                && let Ok(parsed) = next.parse::<EnvBool>()
            {
                return Ok(Some(parsed.into_inner()));
            }
            return Ok(Some(true));
        }
        if let Some(value) = s.strip_prefix(&eq_prefix) {
            return value
                .parse::<EnvBool>()
                .map(|b| Some(b.into_inner()))
                .map_err(|e| format!("`{FORCE_ROUNDTRIP_FLAG}={value}` {e}"));
        }
    }
    Ok(None)
}

// ===========================================================================
// Executor selection — DAGR_EXECUTOR / --dagr.executor
// ===========================================================================

/// The library-owned flag selecting the executor (`--dagr.executor`). Reserved in
/// [`reserved_flag_names`](crate::contract::reserved_flag_names), so a pipeline
/// parameter can never shadow it.
pub const EXECUTOR_FLAG: &str = "--dagr.executor";

/// The library-owned flag capping concurrently-placed node attempts
/// (`--dagr.max-pods`). Reserved in
/// [`reserved_flag_names`](crate::contract::reserved_flag_names).
pub const MAX_PODS_FLAG: &str = "--dagr.max-pods";

/// The default remote-slot ceiling: **unconstrained**.
///
/// The remote pool is a flat *operator* limit on in-flight placed work, and the
/// cluster remains responsible for its own capacity. Picking a finite default here
/// would be dagr second-guessing an accounting it cannot see, and would silently
/// serialize a wide fan-out nobody asked to serialize — so the pool is unpinned
/// until an operator states a number, matching every other pool's
/// [unconstrained](dagr_core::admission::PoolCapacities::new) default.
pub const MAX_PODS_DEFAULT: u32 = u32::MAX;

/// The library-owned flag setting the **infrastructure** retry budget
/// (`--dagr.pod-launch-retries`). Reserved in
/// [`reserved_flag_names`](crate::contract::reserved_flag_names).
pub const POD_LAUNCH_RETRIES_FLAG: &str = "--dagr.pod-launch-retries";

/// The default number of *extra* launches a pod that never started may have.
///
/// Finite, and small. Unbounded would be a hang with extra steps — a permanently
/// unpullable image would retry forever and the run would never report. Zero would
/// fail a node on a single unlucky scheduling decision, which is exactly the
/// transient the separate budget exists to absorb. Two retries covers the
/// short-lived capacity and registry blips T101 observed while still reaching a
/// verdict in seconds.
pub const POD_LAUNCH_RETRIES_DEFAULT: u32 = 2;

/// Resolve which **executor** runs this invocation by `flag > env > default`
/// (default [`Local`](crate::executor::ExecutorKind::Local)): a present
/// `--dagr.executor` flag wins outright (the env is never read); with no flag,
/// [`DAGR_EXECUTOR`] is read and parsed; with neither, every node runs in-process.
///
/// A value outside the closed set fails **loudly**, naming the variable and the
/// rejected value — an executor is never silently downgraded to `local`, because a
/// run that quietly ignored the operator's placement request and reported success
/// is the one outcome worse than refusing.
///
/// # Errors
/// Returns an [`EnvParseError`] (kind [`Parse`](EnvParseErrorKind::Parse) →
/// [`ExitCode::InvalidUsage`]) naming [`DAGR_EXECUTOR`] when its value is not a
/// recognized executor name.
pub fn resolve_executor(
    flag: Option<crate::executor::ExecutorKind>,
) -> Result<crate::executor::ExecutorKind, EnvParseError> {
    resolve(flag, DAGR_EXECUTOR, crate::executor::ExecutorKind::Local)
}

/// Resolve the **remote-slot ceiling** by `flag > env > default` (default
/// [`MAX_PODS_DEFAULT`], unconstrained): a present `--dagr.max-pods` flag wins
/// outright; with no flag, [`DAGR_MAX_PODS`] is read and parsed as a pod count;
/// with neither, the remote pool imposes no dagr-side ceiling.
///
/// `0` is a legitimate value meaning *no remote capacity at all*: a placed node
/// then fails the bootstrap over-demand check with a named reason, rather than
/// waiting for capacity that can never arrive.
///
/// # Errors
/// Returns an [`EnvParseError`] (kind [`Parse`](EnvParseErrorKind::Parse) →
/// [`ExitCode::InvalidUsage`]) naming [`DAGR_MAX_PODS`] when its value is not a
/// non-negative integer.
pub fn resolve_max_pods(flag: Option<u32>) -> Result<u32, EnvParseError> {
    resolve(flag, DAGR_MAX_PODS, MAX_PODS_DEFAULT)
}

/// Parse `--dagr.executor` out of a raw invocation. Absent → [`None`] (fall
/// through to the env/default); `--dagr.executor=k8s` and `--dagr.executor k8s`
/// both yield the parsed executor.
///
/// # Errors
/// Returns the diagnostic message when the value is not a recognized executor name
/// — a bad selection fails loudly, never silently local.
pub fn parse_executor_flag(
    argv: &[std::ffi::OsString],
) -> Result<Option<crate::executor::ExecutorKind>, String> {
    parse_valued_flag(argv, EXECUTOR_FLAG)
}

/// Parse `--dagr.max-pods` out of a raw invocation, in the same two grammars as
/// [`parse_executor_flag`].
///
/// # Errors
/// Returns the diagnostic message when the value is not a non-negative integer.
pub fn parse_max_pods_flag(argv: &[std::ffi::OsString]) -> Result<Option<u32>, String> {
    parse_valued_flag(argv, MAX_PODS_FLAG)
}

/// Resolve the **infrastructure** retry budget by `flag > env > default` (default
/// [`POD_LAUNCH_RETRIES_DEFAULT`]): a present `--dagr.pod-launch-retries` flag wins
/// outright; with no flag, [`DAGR_POD_LAUNCH_RETRIES`] is read and parsed; with
/// neither, a pod that never starts gets [`POD_LAUNCH_RETRIES_DEFAULT`] extra
/// launches before the node fails with a classified infrastructure error.
///
/// `0` is legitimate and means *submit once*: a pod that does not start on the first
/// try fails its node immediately, naming the cause.
///
/// # Errors
/// Returns an [`EnvParseError`] (kind [`Parse`](EnvParseErrorKind::Parse) →
/// [`ExitCode::InvalidUsage`]) naming [`DAGR_POD_LAUNCH_RETRIES`] when its value is
/// not a non-negative integer.
pub fn resolve_pod_launch_retries(flag: Option<u32>) -> Result<u32, EnvParseError> {
    resolve(flag, DAGR_POD_LAUNCH_RETRIES, POD_LAUNCH_RETRIES_DEFAULT)
}

/// Parse `--dagr.pod-launch-retries` out of a raw invocation, in the same two
/// grammars as [`parse_executor_flag`].
///
/// # Errors
/// Returns the diagnostic message when the value is not a non-negative integer, or
/// when the flag is present with no value at all.
pub fn parse_pod_launch_retries_flag(argv: &[std::ffi::OsString]) -> Result<Option<u32>, String> {
    parse_valued_flag(argv, POD_LAUNCH_RETRIES_FLAG)
}

// ===========================================================================
// Object-store location — DAGR_BLOB_* / --dagr.blob.* (M10, T110)
// ===========================================================================
//
// Where the blob store IS, as opposed to what is in it. Four knobs, all
// operator-owned, all following the same `flag > env > default` precedence
// everything else here does.
//
// Deliberately absent from this list: any credential. dagr holds no credential of
// its own, so there is no `--dagr.blob.access-key`, no `DAGR_BLOB_SECRET`, and no
// file it reads one out of. Credentials come from the ambient environment the
// platform already populates — see `dagr_blob::s3::creds`. A knob here would be a
// credential surface, and adding one is the thing this design is avoiding.
//
// Also deliberately absent from a stored reference: the endpoint and the region.
// They are *this process's* view of how to reach a bucket, and the same bucket is
// reached through different endpoints from inside and outside a cluster. A
// reference names `<bucket>/<prefix>` and a content address, so it keeps
// resolving when the network changes and the storage does not.

/// The library-owned flag naming the object store's endpoint
/// (`--dagr.blob.endpoint`). Reserved in
/// [`reserved_flag_names`](crate::contract::reserved_flag_names).
pub const BLOB_ENDPOINT_FLAG: &str = "--dagr.blob.endpoint";

/// The library-owned flag naming the bucket (`--dagr.blob.bucket`). Reserved in
/// [`reserved_flag_names`](crate::contract::reserved_flag_names).
pub const BLOB_BUCKET_FLAG: &str = "--dagr.blob.bucket";

/// The library-owned flag naming the signing region (`--dagr.blob.region`).
/// Reserved in [`reserved_flag_names`](crate::contract::reserved_flag_names).
pub const BLOB_REGION_FLAG: &str = "--dagr.blob.region";

/// The library-owned flag naming the key prefix (`--dagr.blob.prefix`). Reserved
/// in [`reserved_flag_names`](crate::contract::reserved_flag_names).
pub const BLOB_PREFIX_FLAG: &str = "--dagr.blob.prefix";

/// The signing region assumed when an operator configures none.
///
/// `us-east-1` rather than "no region": `SigV4` has no unregioned form, and this is
/// the value every S3-compatible implementation without regions of its own
/// accepts. Against real AWS an operator sets the region their bucket is in;
/// against `MinIO`, Ceph or a gateway the value is signed and ignored.
pub const BLOB_REGION_DEFAULT: &str = "us-east-1";

/// How the object-store knobs read the environment. Injected rather than assumed
/// so a test can assert precedence without mutating the process environment —
/// `std::env` is process-global, and a suite that set and unset real variables
/// would be order-dependent under CI parallelism.
pub type EnvReader<'a> = &'a dyn Fn(&str) -> Option<String>;

/// Read the real process environment. The reader every non-test caller passes.
#[must_use]
pub fn ambient_env(name: &str) -> Option<String> {
    std::env::var(name).ok()
}

/// The shared body of the four resolvers: flag wins outright (the environment is
/// never read), then a non-empty environment value, then the default.
fn resolve_string(flag: Option<String>, env_key: &str, read: EnvReader<'_>) -> Option<String> {
    if let Some(value) = flag {
        let trimmed = value.trim().to_string();
        return if trimmed.is_empty() {
            None
        } else {
            Some(trimmed)
        };
    }
    read(env_key)
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

/// Resolve the object store's **endpoint** by `flag > env > default` (default:
/// none, i.e. AWS's endpoint for the resolved region).
///
/// # Errors
///
/// [`EnvParseError`] (kind [`Parse`](EnvParseErrorKind::Parse) →
/// [`ExitCode::InvalidUsage`]) when the value is not an `http://` or `https://`
/// URL. A bare host is rejected rather than guessed at: guessing the scheme would
/// mean silently choosing between a plaintext and an encrypted channel for signed
/// requests, which is not a default anything should pick.
pub fn resolve_blob_endpoint(
    flag: Option<String>,
    read: EnvReader<'_>,
) -> Result<Option<String>, EnvParseError> {
    let Some(endpoint) = resolve_string(flag, DAGR_BLOB_ENDPOINT, read) else {
        return Ok(None);
    };
    if !endpoint.starts_with("http://") && !endpoint.starts_with("https://") {
        return Err(EnvParseError::parse(
            DAGR_BLOB_ENDPOINT,
            endpoint,
            "an object-store endpoint must be an absolute `http://` or `https://` URL",
        ));
    }
    Ok(Some(endpoint))
}

/// Resolve the **bucket** by `flag > env > default` (default: none — a bucket has
/// no sensible default and an unset one simply means "no object store configured").
///
/// # Errors
///
/// [`EnvParseError`] when the value contains a `/`: that is a bucket **and a
/// prefix**, and the prefix has its own knob. Splitting it silently would make
/// `--dagr.blob.prefix` quietly ineffective.
pub fn resolve_blob_bucket(
    flag: Option<String>,
    read: EnvReader<'_>,
) -> Result<Option<String>, EnvParseError> {
    let Some(bucket) = resolve_string(flag, DAGR_BLOB_BUCKET, read) else {
        return Ok(None);
    };
    if bucket.contains('/') {
        return Err(EnvParseError::parse(
            DAGR_BLOB_BUCKET,
            bucket,
            "a bucket name contains no `/` — set the key prefix with `--dagr.blob.prefix`",
        ));
    }
    Ok(Some(bucket))
}

/// Resolve the **signing region** by `flag > env > default`
/// ([`BLOB_REGION_DEFAULT`]).
///
/// # Errors
///
/// Never fails today; the fallible signature is deliberate, because a region is
/// exactly the kind of value that grows a validation rule, and widening an
/// infallible public signature later is a breaking change.
pub fn resolve_blob_region(
    flag: Option<String>,
    read: EnvReader<'_>,
) -> Result<String, EnvParseError> {
    Ok(resolve_string(flag, DAGR_BLOB_REGION, read)
        .unwrap_or_else(|| BLOB_REGION_DEFAULT.to_string()))
}

/// Resolve the **key prefix** by `flag > env > default` (default: empty).
///
/// # Errors
///
/// Never fails today; fallible for the same reason [`resolve_blob_region`] is.
pub fn resolve_blob_prefix(
    flag: Option<String>,
    read: EnvReader<'_>,
) -> Result<String, EnvParseError> {
    Ok(resolve_string(flag, DAGR_BLOB_PREFIX, read)
        .map(|p| p.trim_matches('/').to_string())
        .unwrap_or_default())
}

/// Parse `--dagr.blob.endpoint` out of a raw invocation.
///
/// # Errors
/// The diagnostic message when the flag is present with no value.
pub fn parse_blob_endpoint_flag(argv: &[std::ffi::OsString]) -> Result<Option<String>, String> {
    parse_valued_flag(argv, BLOB_ENDPOINT_FLAG)
}

/// Parse `--dagr.blob.bucket` out of a raw invocation.
///
/// # Errors
/// The diagnostic message when the flag is present with no value.
pub fn parse_blob_bucket_flag(argv: &[std::ffi::OsString]) -> Result<Option<String>, String> {
    parse_valued_flag(argv, BLOB_BUCKET_FLAG)
}

/// Parse `--dagr.blob.region` out of a raw invocation.
///
/// # Errors
/// The diagnostic message when the flag is present with no value.
pub fn parse_blob_region_flag(argv: &[std::ffi::OsString]) -> Result<Option<String>, String> {
    parse_valued_flag(argv, BLOB_REGION_FLAG)
}

/// Parse `--dagr.blob.prefix` out of a raw invocation.
///
/// # Errors
/// The diagnostic message when the flag is present with no value.
pub fn parse_blob_prefix_flag(argv: &[std::ffi::OsString]) -> Result<Option<String>, String> {
    parse_valued_flag(argv, BLOB_PREFIX_FLAG)
}

// ===========================================================================
// The run-path knob wiring — reserved-flag scanners, duration bounds, and the
// store fallback
// ===========================================================================
//
// The precedence helpers above existed for two milestones with zero non-test
// callers on the shipped run path: `dagr run <flow>` built its `RunConfig` bare
// and every documented knob was silently ignored. This section is what the run
// path (`run_selected_flow`, `RunnableFlow::run_to_store`) consumes to make the
// documented behaviour real: a scanner per reserved runtime flag (so the *flag*
// tier exists, not just the env tier), the duration bounds the tables always
// documented, and the `--store` / `DAGR_STORE` resolution.

/// The library-owned run-store flag (`--store`). Reserved in
/// [`reserved_flag_names`](crate::contract::reserved_flag_names).
pub const STORE_FLAG: &str = "--store";

/// The library-owned grace flag (`--grace`). Reserved in
/// [`reserved_flag_names`](crate::contract::reserved_flag_names).
pub const GRACE_FLAG: &str = "--grace";

/// The library-owned teardown-deadline flag (`--teardown-deadline`). Reserved in
/// [`reserved_flag_names`](crate::contract::reserved_flag_names).
pub const TEARDOWN_DEADLINE_FLAG: &str = "--teardown-deadline";

/// The library-owned failure-mode flag (`--failure-mode`). Reserved in
/// [`reserved_flag_names`](crate::contract::reserved_flag_names).
pub const FAILURE_MODE_FLAG: &str = "--failure-mode";

/// The library-owned compute-thread pool pin (`--dagr.pool.compute-threads`).
/// Reserved in [`reserved_flag_names`](crate::contract::reserved_flag_names).
pub const POOL_COMPUTE_THREADS_FLAG: &str = "--dagr.pool.compute-threads";

/// The library-owned blocking-thread pool pin (`--dagr.pool.blocking-threads`).
/// Reserved in [`reserved_flag_names`](crate::contract::reserved_flag_names).
pub const POOL_BLOCKING_THREADS_FLAG: &str = "--dagr.pool.blocking-threads";

/// The library-owned memory pool pin (`--dagr.pool.memory`). Reserved in
/// [`reserved_flag_names`](crate::contract::reserved_flag_names).
pub const POOL_MEMORY_FLAG: &str = "--dagr.pool.memory";

/// The library-owned headroom flag (`--dagr.headroom-fraction`). Reserved in
/// [`reserved_flag_names`](crate::contract::reserved_flag_names).
pub const HEADROOM_FLAG: &str = "--dagr.headroom-fraction";

/// The smallest accepted grace period: **1 ms**, as both knob tables document.
/// A zero grace makes the printed shutdown budget a lie — cancellation would get
/// no drain window at all — and is an operator typo far more often than intent,
/// so it fails loudly instead of being silently honoured.
pub const GRACE_MIN: Duration = Duration::from_millis(1);

/// The smallest accepted teardown deadline: **1 s**, as both knob tables
/// document. A zero deadline guarantees every teardown attempt is killed before
/// it runs — cleanup that can never happen is a misconfiguration, not a choice.
pub const TEARDOWN_DEADLINE_MIN: Duration = Duration::from_secs(1);

/// Check a resolved duration against its knob's documented minimum; below it,
/// an [`out_of_range`](EnvParseError::out_of_range) error naming `source` (the
/// flag or the environment variable, whichever supplied the value) and the
/// violated bound — mapping to [`ExitCode::BootstrapFailure`], exactly like the
/// headroom range check.
pub(crate) fn validate_duration_min(
    source: &str,
    value: Duration,
    min: Duration,
    bound: &str,
) -> Result<Duration, EnvParseError> {
    if value >= min {
        Ok(value)
    } else {
        Err(EnvParseError::out_of_range(
            source,
            format!("{}ms", value.as_millis()),
            format!("expected a duration of at least {bound}"),
        ))
    }
}

/// Parse `--grace` out of a raw invocation (`--grace 5s` / `--grace=5s`).
/// Absent → [`None`] (fall through to `DAGR_GRACE` / the default).
///
/// # Errors
/// The diagnostic message when the value is not a `10` / `10s` / `10ms`
/// duration, or the flag is present with no value.
pub fn parse_grace_flag(argv: &[std::ffi::OsString]) -> Result<Option<Duration>, String> {
    Ok(parse_valued_flag::<EnvDuration>(argv, GRACE_FLAG)?.map(EnvDuration::into_inner))
}

/// Parse `--teardown-deadline` out of a raw invocation, in the same grammars as
/// [`parse_grace_flag`].
///
/// # Errors
/// The diagnostic message when the value is not a duration, or the flag is
/// present with no value.
pub fn parse_teardown_deadline_flag(
    argv: &[std::ffi::OsString],
) -> Result<Option<Duration>, String> {
    Ok(parse_valued_flag::<EnvDuration>(argv, TEARDOWN_DEADLINE_FLAG)?.map(EnvDuration::into_inner))
}

/// Parse `--failure-mode` out of a raw invocation.
///
/// # Errors
/// The diagnostic message when the value is neither `continue-independent` nor
/// `stop-on-first-failure`, or the flag is present with no value.
pub fn parse_failure_mode_flag(
    argv: &[std::ffi::OsString],
) -> Result<Option<FailureMode>, String> {
    Ok(parse_valued_flag::<EnvFailureMode>(argv, FAILURE_MODE_FLAG)?.map(EnvFailureMode::into_inner))
}

/// Parse the three `--dagr.pool.*` pins out of a raw invocation into the
/// [`PoolPinFlags`] the [`resolve_pool_pins`] resolver consumes. An absent flag
/// leaves its field [`None`], so the env tier (and the un-pinned tri-state
/// beneath it) still applies per pool.
///
/// # Errors
/// The diagnostic message when a pin's value is not a non-negative integer, or a
/// pin flag is present with no value.
pub fn parse_pool_pin_flags(argv: &[std::ffi::OsString]) -> Result<PoolPinFlags, String> {
    Ok(PoolPinFlags {
        compute_threads: parse_valued_flag::<u32>(argv, POOL_COMPUTE_THREADS_FLAG)?,
        blocking_threads: parse_valued_flag::<u32>(argv, POOL_BLOCKING_THREADS_FLAG)?,
        memory: parse_valued_flag::<u64>(argv, POOL_MEMORY_FLAG)?,
    })
}

/// Parse `--dagr.headroom-fraction` out of a raw invocation. Range validation is
/// [`resolve_headroom`]'s (which names the flag on an out-of-range value); this
/// scanner only rejects a value that is not a float at all.
///
/// # Errors
/// The diagnostic message when the value is not a float, or the flag is present
/// with no value.
pub fn parse_headroom_flag(argv: &[std::ffi::OsString]) -> Result<Option<f64>, String> {
    parse_valued_flag::<f64>(argv, HEADROOM_FLAG)
}

/// Parse `--store` out of a raw invocation.
///
/// # Errors
/// The diagnostic message when the flag is present with no value — a bare
/// `--store` silently falling back to the default would drop an operator's
/// instruction on the floor.
pub fn parse_store_flag(argv: &[std::ffi::OsString]) -> Result<Option<String>, String> {
    parse_valued_flag::<String>(argv, STORE_FLAG)
}

/// Resolve the **run-store base** by `flag > env > default`: the `--store` value
/// if present (the env is never read), else [`DAGR_STORE`], else
/// [`DEFAULT_STORE_BASE`](crate::run_store::DEFAULT_STORE_BASE).
///
/// # Errors
///
/// Never fails today — a path string always "parses" — but the resolution runs
/// through the same fallible [`resolve`] helper as every other knob, and the
/// fallible signature is kept so a validation rule (as other knobs grew) is not
/// a breaking change.
pub fn resolve_store_base(flag: Option<String>) -> Result<String, EnvParseError> {
    resolve::<String>(flag, DAGR_STORE, crate::run_store::DEFAULT_STORE_BASE.to_string())
}

/// The resolved pool-sizing knobs: the three pins, the headroom fraction, and
/// whether **any** of them was actually supplied — which is what decides whether
/// the run sizes its admission pools from the machine at all.
///
/// The gate matters because of what the no-knob default is. Admission pools have
/// been **unconstrained** on the shipped run path since it existed; deriving them
/// from the machine whenever *nothing* is set would change every run's capacity
/// behind the operator's back. So the container-limit probe runs **iff** at
/// least one pin or an explicit headroom was supplied (flag or env): a pinned
/// pool is then the pin verbatim, every un-pinned pool derives from detection
/// (the tri-state), and the no-knob run keeps its historical unconstrained set,
/// byte for byte.
#[derive(Debug, Clone, PartialEq)]
pub struct PoolSizing {
    pins: PinnedPools,
    headroom: f64,
    engaged: bool,
}

impl PoolSizing {
    /// The resolved pins (a pool without a flag or env value is un-pinned).
    #[must_use]
    pub fn pins(&self) -> &PinnedPools {
        &self.pins
    }

    /// The resolved headroom fraction (the default when the knob is unset).
    #[must_use]
    pub fn headroom(&self) -> f64 {
        self.headroom
    }

    /// Whether any pool-sizing knob was supplied — i.e. whether
    /// [`capacities`](Self::capacities) will consult the probe at all.
    #[must_use]
    pub fn engaged(&self) -> bool {
        self.engaged
    }

    /// Size the admission pools through `probe` (injected so a test can use a
    /// deterministic root and core count; the run path passes
    /// [`ContainerLimitProbe::from_host`](dagr_core::limits::ContainerLimitProbe::from_host)).
    /// Returns [`None`] when no knob was supplied — the caller keeps the
    /// historical unconstrained capacities — and the probe-derived set otherwise,
    /// with pins verbatim and the headroom applied to every detected pool.
    ///
    /// # Errors
    ///
    /// Propagates [`detect`](dagr_core::limits::ContainerLimitProbe::detect)'s
    /// error type, which is documented never to fail today (sizing always yields
    /// a capacity set); a caller maps it to a bootstrap failure rather than
    /// unwrapping, so a future probe validation cannot turn into a panic here.
    pub fn capacities(
        &self,
        probe: dagr_core::limits::ContainerLimitProbe,
    ) -> Result<Option<dagr_core::admission::PoolCapacities>, dagr_core::limits::CapacityBootstrapFailure>
    {
        if !self.engaged {
            return Ok(None);
        }
        probe
            .with_pins(self.pins.clone())
            .with_headroom(self.headroom)
            .detect()
            .map(Some)
    }
}

/// Resolve the pool pins and the headroom fraction by `flag > env > default`
/// and record whether **any** of them was supplied — see [`PoolSizing`] for why
/// that bit is load-bearing.
///
/// # Errors
///
/// Returns the [`EnvParseError`] of whichever knob failed: an unparseable
/// `DAGR_POOL_*` / `DAGR_HEADROOM` value (kind
/// [`Parse`](EnvParseErrorKind::Parse) → [`ExitCode::InvalidUsage`]) or an
/// out-of-range headroom (kind [`OutOfRange`](EnvParseErrorKind::OutOfRange) →
/// [`ExitCode::BootstrapFailure`]), each naming the offending source.
pub fn resolve_pool_sizing(
    flags: PoolPinFlags,
    headroom_flag: Option<f64>,
) -> Result<PoolSizing, EnvParseError> {
    // Whether the headroom knob was *supplied* is decided before resolution:
    // `resolve_headroom(None)` returns the same 0.20 for "unset" and for an
    // explicit `DAGR_HEADROOM=0.2`, and only the supplied case may engage the
    // probe.
    let headroom_supplied =
        headroom_flag.is_some() || std::env::var(DAGR_HEADROOM).is_ok_and(|v| !v.is_empty());
    let pins = resolve_pool_pins(flags)?;
    let headroom = resolve_headroom(headroom_flag)?;
    let engaged = headroom_supplied
        || pins.memory_pin().is_some()
        || pins.compute_threads_pin().is_some()
        || pins.blocking_threads_pin().is_some();
    Ok(PoolSizing {
        pins,
        headroom,
        engaged,
    })
}

/// Read a **value-taking** library flag out of a raw invocation, accepting both
/// `--flag=value` and `--flag value`.
///
/// Distinct from the bare-boolean toggles ([`parse_force_roundtrip_flag`]): these
/// flags have no meaningful "present means on" form, so a flag with **no** value is
/// itself an error rather than a default — silently ignoring `--dagr.executor` with
/// a missing value would drop an operator's instruction on the floor.
fn parse_valued_flag<T>(argv: &[std::ffi::OsString], flag: &str) -> Result<Option<T>, String>
where
    T: FromStr,
    T::Err: std::fmt::Display,
{
    let eq_prefix = format!("{flag}=");
    let mut it = argv.iter();
    while let Some(arg) = it.next() {
        let Some(s) = arg.to_str() else { continue };
        let raw = if s == flag {
            it.next()
                .and_then(|a| a.to_str())
                .ok_or_else(|| format!("`{flag}` needs a value"))?
        } else if let Some(value) = s.strip_prefix(&eq_prefix) {
            value
        } else {
            continue;
        };
        return T::from_str(raw)
            .map(Some)
            .map_err(|e| format!("`{flag}={raw}` {e}"));
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    use dagr_core::limits::ContainerLimitProbe;

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

    /// T108 · the **infrastructure** retry budget resolves by the one precedence
    /// rule, and an unparseable value fails loudly rather than falling back.
    ///
    /// It resolves against the real `DAGR_POD_LAUNCH_RETRIES` name (the resolver
    /// hardcodes it), so it takes the module's env lock across its whole
    /// set → read → remove window like every other real-name test here.
    #[test]
    fn pod_launch_retries_resolves_flag_then_env_then_default() {
        let g = env_lock();
        unset_env(&g, DAGR_POD_LAUNCH_RETRIES);
        assert_eq!(
            resolve_pod_launch_retries(None).expect("the default path never errors"),
            POD_LAUNCH_RETRIES_DEFAULT,
            "unset falls through to the default"
        );

        set_env(&g, DAGR_POD_LAUNCH_RETRIES, "5");
        let from_env = resolve_pod_launch_retries(None);
        let from_flag = resolve_pod_launch_retries(Some(2));
        set_env(&g, DAGR_POD_LAUNCH_RETRIES, "not-a-number");
        let bad = resolve_pod_launch_retries(None);
        unset_env(&g, DAGR_POD_LAUNCH_RETRIES);

        assert_eq!(
            from_env.expect("a valid env value parses"),
            5,
            "with no flag the env var is read"
        );
        assert_eq!(
            from_flag.expect("the flag path never errors"),
            2,
            "a present flag wins outright and the env var is never read"
        );
        let err = bad.expect_err("an unparseable budget fails loudly");
        assert_eq!(err.kind, EnvParseErrorKind::Parse);
        assert!(
            err.to_string().contains(DAGR_POD_LAUNCH_RETRIES),
            "the diagnostic names the variable: {err}"
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
    fn dagr_env_name_constants_cover_every_knob() {
        // The documented spelling of every shipped knob, so a constant cannot
        // silently drift from its table row. This list is the complete knob set —
        // a knob added without a line here fails review by construction.
        assert_eq!(DAGR_GRACE, "DAGR_GRACE");
        assert_eq!(DAGR_TEARDOWN_DEADLINE, "DAGR_TEARDOWN_DEADLINE");
        assert_eq!(DAGR_FAILURE_MODE, "DAGR_FAILURE_MODE");
        assert_eq!(DAGR_POOL_COMPUTE_THREADS, "DAGR_POOL_COMPUTE_THREADS");
        assert_eq!(DAGR_POOL_BLOCKING_THREADS, "DAGR_POOL_BLOCKING_THREADS");
        assert_eq!(DAGR_POOL_MEMORY, "DAGR_POOL_MEMORY");
        assert_eq!(DAGR_HEADROOM, "DAGR_HEADROOM");
        assert_eq!(DAGR_STORE, "DAGR_STORE");
        assert_eq!(DAGR_METASTORE, "DAGR_METASTORE");
        assert_eq!(DAGR_FORCE_ROUNDTRIP, "DAGR_FORCE_ROUNDTRIP");
        assert_eq!(DAGR_EXECUTOR, "DAGR_EXECUTOR");
        assert_eq!(DAGR_MAX_PODS, "DAGR_MAX_PODS");
        assert_eq!(DAGR_POD_LAUNCH_RETRIES, "DAGR_POD_LAUNCH_RETRIES");
        assert_eq!(DAGR_BLOB_ENDPOINT, "DAGR_BLOB_ENDPOINT");
        assert_eq!(DAGR_BLOB_BUCKET, "DAGR_BLOB_BUCKET");
        assert_eq!(DAGR_BLOB_REGION, "DAGR_BLOB_REGION");
        assert_eq!(DAGR_BLOB_PREFIX, "DAGR_BLOB_PREFIX");
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

    // --- The run-path knob wiring: scanners, bounds, store, sizing --------

    /// One representative token per new scanner, in both grammars, plus the
    /// absent case — proving the flag tier exists for every knob this section
    /// wires.
    #[test]
    fn run_path_flag_scanners_accept_both_grammars() {
        let argv = |tokens: &[&str]| -> Vec<std::ffi::OsString> {
            tokens.iter().map(std::ffi::OsString::from).collect()
        };
        assert_eq!(
            parse_grace_flag(&argv(&["dagr", "run", "--grace", "5s"])).expect("space form"),
            Some(Duration::from_secs(5))
        );
        assert_eq!(
            parse_teardown_deadline_flag(&argv(&["dagr", "run", "--teardown-deadline=40s"]))
                .expect("= form"),
            Some(Duration::from_secs(40))
        );
        assert_eq!(
            parse_failure_mode_flag(&argv(&["dagr", "run", "--failure-mode", "stop-on-first-failure"]))
                .expect("mode token"),
            Some(FailureMode::StopOnFirstFailure)
        );
        let pins = parse_pool_pin_flags(&argv(&[
            "dagr",
            "run",
            "--dagr.pool.compute-threads=2",
            "--dagr.pool.memory",
            "4096",
        ]))
        .expect("pin forms");
        assert_eq!(pins.compute_threads, Some(2));
        assert_eq!(pins.memory, Some(4096));
        assert_eq!(pins.blocking_threads, None, "an absent pin stays None");
        assert_eq!(
            parse_headroom_flag(&argv(&["dagr", "run", "--dagr.headroom-fraction", "0.5"]))
                .expect("headroom"),
            Some(0.5)
        );
        assert_eq!(
            parse_store_flag(&argv(&["dagr", "run", "--store", "./elsewhere"])).expect("store"),
            Some("./elsewhere".to_string())
        );
        // Absent flags fall through to the env tier untouched.
        assert_eq!(parse_grace_flag(&argv(&["dagr", "run"])).expect("absent"), None);
        assert_eq!(parse_store_flag(&argv(&["dagr", "run"])).expect("absent"), None);
    }

    /// A malformed value and a valueless flag are both loud scanner errors —
    /// an operator's instruction is never silently dropped.
    #[test]
    fn run_path_flag_scanners_fail_loudly() {
        let argv = |tokens: &[&str]| -> Vec<std::ffi::OsString> {
            tokens.iter().map(std::ffi::OsString::from).collect()
        };
        let err = parse_grace_flag(&argv(&["dagr", "run", "--grace", "soon"]))
            .expect_err("a non-duration is rejected");
        assert!(err.contains("--grace"), "the diagnostic names the flag: {err}");
        let err = parse_store_flag(&argv(&["dagr", "run", "--store"]))
            .expect_err("a valueless --store is rejected");
        assert!(err.contains("--store"), "the diagnostic names the flag: {err}");
    }

    /// The store base resolves `flag > env > default` through the shared helper.
    #[test]
    fn store_base_flag_env_default_precedence() {
        let g = env_lock();
        set_env(&g, DAGR_STORE, "/env-store");
        let flag_wins = resolve_store_base(Some("/flag-store".into())).expect("flag path");
        let env_used = resolve_store_base(None).expect("env path");
        unset_env(&g, DAGR_STORE);
        let default = resolve_store_base(None).expect("default path");
        assert_eq!(flag_wins, "/flag-store", "a present --store wins outright");
        assert_eq!(env_used, "/env-store", "with no flag, DAGR_STORE is used");
        assert_eq!(
            default,
            crate::run_store::DEFAULT_STORE_BASE,
            "with neither, the default base is unchanged"
        );
    }

    /// The duration bounds the tables document: below the minimum is an
    /// out-of-range bootstrap failure naming the source and the bound.
    #[test]
    fn duration_bound_below_minimum_is_out_of_range() {
        let err = validate_duration_min(DAGR_GRACE, Duration::ZERO, GRACE_MIN, "1 ms")
            .expect_err("zero grace violates the bound");
        assert_eq!(err.kind, EnvParseErrorKind::OutOfRange);
        assert_eq!(err.exit_code(), ExitCode::BootstrapFailure);
        assert!(err.to_string().contains(DAGR_GRACE));
        assert!(err.to_string().contains("1 ms"));
        // At the bound is accepted — the minimum itself is legal.
        assert_eq!(
            validate_duration_min(GRACE_FLAG, GRACE_MIN, GRACE_MIN, "1 ms").expect("at the bound"),
            GRACE_MIN
        );
        assert_eq!(
            validate_duration_min(
                DAGR_TEARDOWN_DEADLINE,
                Duration::from_millis(999),
                TEARDOWN_DEADLINE_MIN,
                "1 s",
            )
            .expect_err("999 ms violates the teardown bound")
            .exit_code(),
            ExitCode::BootstrapFailure
        );
    }

    /// With no pool knob supplied, sizing is **disengaged**: the probe is never
    /// consulted and the caller keeps the historical unconstrained capacities.
    #[test]
    fn pool_sizing_disengaged_when_no_knob_is_supplied() {
        let g = env_lock();
        for k in [
            DAGR_POOL_COMPUTE_THREADS,
            DAGR_POOL_BLOCKING_THREADS,
            DAGR_POOL_MEMORY,
            DAGR_HEADROOM,
        ] {
            unset_env(&g, k);
        }
        let sizing =
            resolve_pool_sizing(PoolPinFlags::default(), None).expect("nothing set resolves");
        assert!(!sizing.engaged(), "no knob supplied → no probe");
        let caps = sizing
            .capacities(ContainerLimitProbe::from_root("/nonexistent-root-for-t114"))
            .expect("sizing never fails");
        assert!(caps.is_none(), "disengaged sizing yields no capacity set");
    }

    /// A pinned pool is the pin verbatim and every un-pinned pool derives from
    /// detection — the tri-state, preserved through the run-path wiring.
    #[test]
    fn pool_sizing_pin_overrides_detection_and_the_rest_detect() {
        let g = env_lock();
        unset_env(&g, DAGR_HEADROOM);
        set_env(&g, DAGR_POOL_MEMORY, "2048");
        let sizing =
            resolve_pool_sizing(PoolPinFlags::default(), None).expect("pin resolves");
        unset_env(&g, DAGR_POOL_MEMORY);
        assert!(sizing.engaged(), "a supplied pin engages the probe");
        let caps = sizing
            .capacities(
                ContainerLimitProbe::from_root("/nonexistent-root-for-t114").with_host_cores(4),
            )
            .expect("sizing never fails")
            .expect("engaged sizing yields capacities");
        assert_eq!(
            caps.total(dagr_core::admission::Pool::Memory),
            2048,
            "the pinned pool is the pin verbatim, not the detected value"
        );
        assert_eq!(
            caps.total(dagr_core::admission::Pool::ComputeThreads),
            3,
            "an un-pinned pool derives from detection (4 cores at the 20% default headroom)"
        );
    }

    /// An explicit headroom engages the probe even with no pin — including a
    /// headroom spelled exactly like the default, because "supplied" is what
    /// engages sizing, not "different".
    #[test]
    fn pool_sizing_explicit_headroom_engages_the_probe() {
        let g = env_lock();
        for k in [
            DAGR_POOL_COMPUTE_THREADS,
            DAGR_POOL_BLOCKING_THREADS,
            DAGR_POOL_MEMORY,
        ] {
            unset_env(&g, k);
        }
        set_env(&g, DAGR_HEADROOM, "0.5");
        let sizing =
            resolve_pool_sizing(PoolPinFlags::default(), None).expect("headroom resolves");
        unset_env(&g, DAGR_HEADROOM);
        assert!(sizing.engaged(), "an explicit DAGR_HEADROOM engages the probe");
        let caps = sizing
            .capacities(
                ContainerLimitProbe::from_root("/nonexistent-root-for-t114").with_host_cores(8),
            )
            .expect("sizing never fails")
            .expect("engaged sizing yields capacities");
        assert_eq!(
            caps.total(dagr_core::admission::Pool::ComputeThreads),
            4,
            "pools are sized to half the detected limit under DAGR_HEADROOM=0.5"
        );

        // The flag spelling of the default value also engages sizing.
        let sizing = resolve_pool_sizing(PoolPinFlags::default(), Some(HEADROOM_DEFAULT))
            .expect("flag headroom resolves");
        assert!(sizing.engaged(), "a flag headroom equal to the default still engages");
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

    // --- The force-round-trip toggle (T103) ------------------------------
    //
    // The local codec check: with it on, every `Payload`-bounded handoff is
    // encoded and decoded locally so a codec bug is catchable without a cluster.
    // Default OFF means the in-memory fast path is untouched.

    #[test]
    fn force_roundtrip_defaults_off() {
        let g = env_lock();
        unset_env(&g, DAGR_FORCE_ROUNDTRIP);
        assert!(
            !resolve_force_roundtrip(None).expect("default off"),
            "the toggle defaults OFF (no flag, no env) — the fast path is untouched"
        );
    }

    #[test]
    fn force_roundtrip_env_used_when_no_flag() {
        let g = env_lock();
        set_env(&g, DAGR_FORCE_ROUNDTRIP, "1");
        let on = resolve_force_roundtrip(None).expect("env used");
        unset_env(&g, DAGR_FORCE_ROUNDTRIP);
        assert!(on, "with no flag, the env value turns the toggle on");
    }

    #[test]
    fn force_roundtrip_flag_beats_env() {
        let g = env_lock();
        // Env says ON, flag says OFF — the flag wins outright (env never read).
        set_env(&g, DAGR_FORCE_ROUNDTRIP, "1");
        let resolved = resolve_force_roundtrip(Some(false)).expect("flag wins");
        unset_env(&g, DAGR_FORCE_ROUNDTRIP);
        assert!(!resolved, "a present flag wins outright over the env var");
    }

    #[test]
    fn force_roundtrip_bad_env_is_invalid_usage_naming_the_variable() {
        let g = env_lock();
        set_env(&g, DAGR_FORCE_ROUNDTRIP, "sometimes");
        let err = resolve_force_roundtrip(None).expect_err("a bad env value fails loudly");
        unset_env(&g, DAGR_FORCE_ROUNDTRIP);
        assert_eq!(err.exit_code(), ExitCode::InvalidUsage);
        assert!(
            err.to_string().contains(DAGR_FORCE_ROUNDTRIP),
            "the Display names the offending variable, got: {err}"
        );
    }

    #[test]
    fn force_roundtrip_flag_parses_every_accepted_form() {
        let argv = |args: &[&str]| -> Vec<std::ffi::OsString> {
            args.iter().map(std::ffi::OsString::from).collect()
        };
        assert_eq!(
            parse_force_roundtrip_flag(&argv(&["dagr", "run", "etl"])).expect("absent"),
            None
        );
        assert_eq!(
            parse_force_roundtrip_flag(&argv(&["dagr", FORCE_ROUNDTRIP_FLAG])).expect("bare"),
            Some(true)
        );
        assert_eq!(
            parse_force_roundtrip_flag(&argv(&["dagr", FORCE_ROUNDTRIP_FLAG, "off"]))
                .expect("separate value"),
            Some(false)
        );
        assert_eq!(
            parse_force_roundtrip_flag(&argv(&["dagr", "--dagr.force-roundtrip=yes"]))
                .expect("=value"),
            Some(true)
        );
        assert!(
            parse_force_roundtrip_flag(&argv(&["dagr", "--dagr.force-roundtrip=maybe"])).is_err(),
            "a non-boolean value fails loudly, never silently off"
        );
    }
}
