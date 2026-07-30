//! **Auto-discovered DAG entrypoint** — `inventory`-backed discovery of every DAG
//! declared in the binary, dispatched through the existing [`run_registry`](crate::registry::run_registry).
//!
//! # Why this module exists
//!
//! A pipeline binary that hosts many flows must otherwise hand-wire each one into a
//! central [`FlowRegistry`](crate::registry::FlowRegistry) in `main` (`FlowRegistry::new().add("etl", …).add(…)`),
//! so adding a DAG means editing that registry. This module supplies the
//! *discovery* half of the declarative-DAG layer: a binary declares each DAG as an
//! [`inventory::submit!`] of a [`DagRegistration`] (the `#[dag]` macro emits those
//! submissions in a later ticket), and [`run()`] finds every one at link time,
//! builds the registry, and delegates to [`run_registry`](crate::registry::run_registry) — so `main` is a one-liner
//! and adding a DAG needs no registry edit.
//!
//! ```no_run
//! # #[cfg(feature = "dag")]
//! fn main() -> std::process::ExitCode {
//!     // discovers every submitted DAG, delegates; `.into()` maps the library
//!     // `ExitCode` to the process one.
//!     dagr_cli::run(std::env::args_os()).into()
//! }
//! ```
//!
//! `no_run`, not executed: this example **is** a `main`, and running it would
//! dispatch the doctest harness's own argv and return a process exit code. It is
//! compile-checked, which is the whole of what a `main` one-liner can promise.
//!
//! # The leaf-binary contract
//!
//! [`inventory`] registers life-before-`main` constructors in a linker section, so a
//! [`DagRegistration`] is **reliably collected only when it is compiled into the
//! final linked binary** — a submission in a *dependency library* the binary does
//! not otherwise reference is dropped by linker dead-code elimination. Therefore DAG
//! declarations (the `inventory::submit!`s, or the `#[dag]`s that emit them) must
//! live in the **binary crate** that calls [`run()`] (across as many of its modules
//! as you like). Cross-crate DAG *libraries* are unsupported. This fully satisfies
//! "many DAGs in one binary."
//!
//! # Two `inventory` realities [`run()`] neutralises
//!
//! - **Unspecified iteration order.** [`inventory::iter`] visits submissions in an
//!   unspecified order, so [`run()`] **sorts** the discovered records by name before
//!   building the registry — giving a deterministic `list` and a deterministic
//!   "available flows" diagnostic regardless of submission order.
//! - **No duplicate-name rejection.** [`FlowRegistry::add`](crate::registry::FlowRegistry::add) does not reject a
//!   duplicate name, so [`run()`] **fails fast** on two records sharing a name with
//!   an [`ExitCode::InvalidUsage`](crate::contract::ExitCode::InvalidUsage) diagnostic naming the duplicate, *before* any flow
//!   is built — a duplicated DAG name is an authoring error, never a silent
//!   last-writer-wins.
//!
//! # A thin layer over the existing dispatch
//!
//! [`run()`] adds no verb logic: it discovers records, sorts + dedups, builds a
//! [`FlowRegistry`](crate::registry::FlowRegistry) registering **each DAG under its
//! own declared name** (so a one-DAG binary keeps its real name — `list` prints it,
//! `graph <name>` selects it, its stream lives under `<base>/<name>/…` — while the
//! name stays omittable, since the registry dispatches the sole flow when exactly one
//! is registered), and delegates to [`run_registry`](crate::registry::run_registry),
//! which already owns `list` / `run` / `graph` / `validate` dispatch, per-verb exit
//! codes, store handling, run-id minting, and diagnostics.

use std::ffi::OsString;
use std::io::{self, Write};

use crate::contract::ExitCode;
use crate::registry::{FlowRegistry, run_registry};
use crate::run_flow::RunnableFlow;

/// A **discovered DAG registration** — the record a binary submits (one per DAG, via
/// [`inventory::submit!`]) and [`run()`] collects at link time.
///
/// It carries the DAG's `name` and a `factory` fn pointer that builds a **fresh**
/// [`RunnableFlow`]. A plain `fn() -> RunnableFlow` (not a stored flow) is
/// load-bearing: [`RunnableFlow::run`](crate::run_flow::RunnableFlow::run) /
/// [`into_pipeline`](crate::run_flow::RunnableFlow::into_pipeline) **consume** the
/// flow and the type is not `Clone`, so one instance serves at most one verb;
/// storing the factory lets each verb build its own fresh flow (the every-verb-its-
/// own-flow guarantee the registry already relies on). The `#[dag]` macro (a later
/// ticket) generates the factory and emits the submission; this ticket exercises
/// discovery against hand-written [`inventory::submit!`]s.
///
/// ```
/// use dagr_cli::run_flow::RunnableFlow;
/// use dagr_cli::DagRegistration;
///
/// fn build_etl() -> RunnableFlow { RunnableFlow::new() }
/// inventory::submit! { DagRegistration { name: "etl", factory: build_etl } }
///
/// // The submission is collected at link time, before `main` — `run()` finds it
/// // without the binary naming it anywhere.
/// assert!(inventory::iter::<DagRegistration>().any(|d| d.name == "etl"));
/// ```
pub struct DagRegistration {
    /// The DAG's name — what `list` prints, what `run <name>` / `graph <name>` /
    /// `validate <name>` select, and the key [`run()`] sorts and dedups on.
    pub name: &'static str,
    /// A re-invokable factory building a **fresh** [`RunnableFlow`] — called once per
    /// verb by the registry (the flow is consumed by a run, so it cannot be reused).
    pub factory: fn() -> RunnableFlow,
}

// The single `inventory::collect!` for `DagRegistration`. A `collect!` must live in
// the crate that DEFINES the collected type (this crate), and there must be exactly
// one; every `inventory::submit!` in any linked binary contributes to this
// collection, which `run` iterates.
inventory::collect!(DagRegistration);

/// **Discover** every [`DagRegistration`] submitted in the linked binary, build a
/// [`FlowRegistry`], and dispatch `argv` over it through [`run_registry`].
///
/// The one-call auto-discovery entrypoint a DAG-hosting binary's `main` delegates to:
///
/// ```no_run
/// # #[cfg(feature = "dag")]
/// fn main() -> std::process::ExitCode {
///     dagr_cli::run(std::env::args_os()).into()
/// }
/// ```
///
/// `no_run`, not executed: this example **is** a `main`, so running it would
/// dispatch the doctest harness's own argv and return a process exit code. It is
/// compile-checked instead.
///
/// It iterates [`inventory::iter::<DagRegistration>()`](inventory::iter), **sorts**
/// the records by name (neutralising [`inventory`]'s unspecified iteration order),
/// **rejects a duplicate name** with an [`ExitCode::InvalidUsage`] diagnostic naming
/// the duplicate *before* building any flow, builds a [`FlowRegistry`] in sorted
/// order ([`add`](FlowRegistry::add) per record, each under its own declared name —
/// a one-DAG binary keeps its real name and, being the sole flow, still serves the
/// verbs with the name omitted), and delegates to
/// [`run_registry`] — which owns the `list` / `run` / `graph` / `validate` dispatch,
/// per-verb exit codes, store handling, run-id minting, and diagnostics.
///
/// The `argv` shape matches [`run_registry`]: `I: IntoIterator<Item = T>`,
/// `T: Into<OsString> + Clone`.
#[must_use]
pub fn run<I, T>(argv: I) -> ExitCode
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    // Duplicate/discovery diagnostics go to stdout, exactly as `run_registry` routes
    // its own selection messages (a run's machine-readable record is the on-disk
    // event stream, never stdout).
    let mut stdout = io::stdout().lock();
    run_to(argv, &mut stdout)
}

/// [`run()`] with an explicit diagnostic writer — the testable seam. `out` receives
/// the duplicate-rejection diagnostic (and, by delegation, everything
/// [`run_registry`] would write); [`run()`] routes it
/// to the process's standard streams.
#[must_use]
pub fn run_to<I, T, W>(argv: I, out: &mut W) -> ExitCode
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
    W: Write,
{
    let registry = match discover(out) {
        Ok(registry) => registry,
        // A duplicate DAG name was rejected before any flow was built; the diagnostic
        // is already written and the exit code is fixed.
        Err(code) => return code,
    };
    run_registry(&registry, argv)
}

/// Collect every submitted [`DagRegistration`], sort by name, reject a duplicate, and
/// build the [`FlowRegistry`] — the discovery half of [`run()`], factored out so its
/// sort + dedup + single-flow-default logic is unit-testable without dispatching a
/// verb. Returns the built registry, or the [`ExitCode`] to exit with (having written
/// the duplicate diagnostic to `out`) if two records share a name.
fn discover<W: Write>(out: &mut W) -> Result<FlowRegistry, ExitCode> {
    // Collect the fn-pointer records (cheap `Copy` values) and sort by name, so the
    // registry — and thus `list` and every "available flows" message — is
    // deterministic regardless of `inventory::iter`'s unspecified order.
    let mut records: Vec<&DagRegistration> = inventory::iter::<DagRegistration>().collect();
    records.sort_by_key(|r| r.name);

    // Fail fast on a duplicate name BEFORE building any flow: `FlowRegistry::add`
    // would silently accept both (last-writer-wins on selection), which is an
    // authoring error, not a valid registry. Sorted order puts duplicates adjacent.
    if let Some(pair) = records.windows(2).find(|w| w[0].name == w[1].name) {
        let name = pair[0].name;
        let _ = writeln!(
            out,
            "dagr: duplicate DAG name `{name}` (two DAGs are declared with the same name; \
             rename one)"
        );
        return Err(ExitCode::InvalidUsage);
    }

    // Register every discovered DAG under its **own declared name**. A one-DAG binary
    // therefore keeps its real name — `list` prints it, `graph <name>` / `run <name>`
    // select it, and its stream lives under `<base>/<name>/…` — while the name stays
    // **omittable** for free: `FlowRegistry::select` dispatches the sole flow when
    // exactly one is registered (`flows.len() == 1`), so `run` / `graph` / `validate`
    // with no name still work. (The synthetic-`flow`-identity `FlowRegistry::single_flow`
    // stays the hand-wired one-flow constructor for a caller who has no name to give.)
    let registry = records
        .iter()
        .fold(FlowRegistry::new(), |reg, r| reg.add(r.name, r.factory));
    Ok(registry)
}
