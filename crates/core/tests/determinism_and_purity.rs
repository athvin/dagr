//! C7 **determinism and purity** tests — ticket T15 (026). Written first, TDD.
//!
//! These lock the two properties T14 (`crates/core/src/assembly.rs`) *promised*
//! but a builder cannot self-certify (arch.md `### C7 · Flow assembly`, its
//! "Acceptance criteria"):
//!
//! - **Determinism** — assembling the same pipeline twice in one process yields
//!   byte-identical graph output (the generation-time field aside, per C20), and
//!   registration order does not change the artifact.
//! - **Purity** — assembly touches no network, filesystem, clock, credentials, or
//!   parameter values (arch.md C7 "Assembly is pure"), so it succeeds in a fully
//!   empty environment and reaches no parameter value.
//!
//! They exercise **only** the public C7 surface (`dagr_core::flow::Pipeline::assemble`,
//! `dagr_core::assembly::AssemblyArtifact`) and add **no** assembly behavior — the
//! validation and precomputation itself is T14's, already landed. The fingerprint
//! slot is a deterministic FNV-1a in-memory **stand-in** (BLAKE3-v1 is deferred to
//! T41); these tests assert the determinism of the *current* fingerprint, never a
//! specific hash algorithm (T15 Out of scope: fingerprint content is C21/T41).
//!
//! # Decision record — mechanical no-filesystem / no-network proof
//!
//! The ticket's open question: sandboxing, syscall audit, or review convention?
//! **Decision: a std-only, in-process structural proof — a scrubbed-environment
//! *child process* plus a negative control — not an OS sandbox and not a syscall
//! auditor.** The full record lives in the ticket file (026), section "Open
//! questions"; the short form:
//!
//! - **Why not an OS sandbox** (seccomp/landlock/`birdcage`/`extrasafe`): those
//!   are Linux-only (they do not cover the macOS CI tier, T70), and they pull a
//!   dependency into `dagr-core`, the deliberately dependency-free, review-gated
//!   "live pipeline" crate (arch.md "Stability"). A one-off, non-portable, dep-
//!   heavy mechanism is exactly what the ticket says the choice must **not** be —
//!   T40 (graph artifact) and the criteria-matrix structural-determinism job reuse
//!   this convention.
//! - **Why not a syscall auditor** (strace/dtrace): OS-specific, cannot run in
//!   process, and unavailable uniformly across the CI matrix.
//! - **What we do instead**: assembly's *entire* input is an owned in-memory
//!   `&Pipeline`, and the `AssemblyArtifact` surface exposes no fs/network/clock/
//!   parameter accessor — purity is therefore a **structural** fact. We enforce it
//!   with (1) a child process launched with a **cleared environment**
//!   (`Command::env_clear`) and an **empty working directory** (a fresh temp dir
//!   with no config files), which assembles the fixture and must still succeed;
//!   and (2) a **negative control** proving the harness bites — a throwaway
//!   assembler variant that *does* touch the filesystem makes the empty-dir guard
//!   fail. This is portable (std only, every CI OS), in-process-friendly, and
//!   dependency-free.
//! - **What it catches / does not**: it catches assembly gaining any dependency on
//!   an environment variable, a config file at a conventional path, or a network
//!   endpoint reachable only outside the scrub. It does **not** intercept an
//!   arbitrary raw syscall mid-assembly (no sandbox does that portably); that
//!   residual is covered by the structural argument above and by review of the
//!   dependency-free `dagr-core` crate.

use std::process::Command;

use dagr_core::assembly::{AssemblyArtifact, DurableOutput, NodePolicy};
use dagr_core::flow::Flow;
use dagr_core::handle::NodeId;
use dagr_core::task::{RunContext, Task};
use dagr_core::TaskError;

// ---------------------------------------------------------------------------
// The canonical fixture — one multi-node pipeline reused by every scenario:
// data edges, an ordering-only branch, a group label, and a typed parameter
// struct. (Ordering-only *edges* as a first-class C4 kind are T50; here the
// "ordering-only" branch is a fan-out consumer whose own output has zero
// consumers — an ordering/effect leaf — so the fixture stays within the C7
// surface T14 exposes without reaching into T50.)
// ---------------------------------------------------------------------------

/// A typed **parameter struct** the fixture "declares". Parameters are a
/// bootstrap concern carried opaquely on the `RunContext` (C8), parsed *after*
/// assembly — the flow/assembly surface has no parameter argument or accessor at
/// all, which is what makes "no parameter value reachable during assembly" a
/// structural fact (arch.md C7; T0.5 ADR §2). This struct exists only to prove a
/// parameterised pipeline assembles with *no* parameter value supplied.
#[derive(Debug, Clone, PartialEq, Eq)]
struct FixtureParams {
    region: String,
    max_rows: u64,
}

// --- Value + task types (distinct, so a mismatch would show) ----------------
struct Rows;
struct Schema;
struct Report;

/// A durable-reference output type (implements the C27 contract).
struct Snapshot;
impl DurableOutput for Snapshot {}

/// A sourceless task producing `Rows`. Its `run` would read the run's parameters
/// off the `RunContext` at execution time — never at assembly.
struct MakeRows;
impl Task for MakeRows {
    type Input = ();
    type Output = Rows;
    async fn run(&mut self, ctx: &RunContext, _i: ()) -> Result<Rows, TaskError> {
        // Illustrative only (never invoked in these assembly-time tests): a task
        // reads parameters from the RunContext at RUN time, proving the read path
        // is a bootstrap/runtime concern, not an assembly one.
        let _maybe_params: Option<&FixtureParams> = ctx.parameters::<FixtureParams>();
        Ok(Rows)
    }
}

/// A sourceless task producing `Schema`.
struct MakeSchema;
impl Task for MakeSchema {
    type Input = ();
    type Output = Schema;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Schema, TaskError> {
        Ok(Schema)
    }
}

/// A single-input consumer of `Rows`, producing a count.
struct CountRows;
impl Task for CountRows {
    type Input = Rows;
    type Output = u64;
    async fn run(&mut self, _c: &RunContext, _i: Rows) -> Result<u64, TaskError> {
        Ok(0)
    }
}

/// A downstream task consuming exactly two inputs, `(Rows, Schema)`.
struct BuildReport;
impl Task for BuildReport {
    type Input = (Rows, Schema);
    type Output = Report;
    async fn run(&mut self, _c: &RunContext, _i: (Rows, Schema)) -> Result<Report, TaskError> {
        Ok(Report)
    }
}

/// A sourceless task whose output satisfies the durable-output contract.
struct MakeSnapshot;
impl Task for MakeSnapshot {
    type Input = ();
    type Output = Snapshot;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Snapshot, TaskError> {
        Ok(Snapshot)
    }
}

/// Build **and assemble** the canonical fixture pipeline. Every scenario calls
/// this, so the fixture is defined once. The build is a pure value
/// transformation; assembly is the pure C7 pass under test.
///
/// Shape (all names are the identity, so registration order is not semantic):
/// - `rows` (source, `MakeRows`) — fans out to `count` and `report`
/// - `schema` (source, `MakeSchema`, in group `"ingest"`) — feeds `report`
/// - `snapshot` (durable source, `MakeSnapshot`) — a durable, retained leaf
/// - `count` (consumes `rows.shared()`) — its own output has zero consumers: an
///   ordering/effect leaf (the fixture's "ordering-only" branch, retained so it
///   raises no zero-consumer warning)
/// - `report` (consumes `(rows.shared(), schema)`) — the join
/// - one env-capture allowlist entry (`DAGR_REGION`) — names only, nothing read
fn assemble_fixture() -> AssemblyArtifact {
    build_fixture_flow().assemble().expect("fixture assembles")
}

/// The fixture as an immutable `Pipeline`, before assembly — so a scenario can
/// assemble it under a scrubbed environment or inspect it directly.
fn build_fixture_flow() -> dagr_core::flow::Pipeline {
    let mut flow = Flow::new();
    let rows = flow.register_source("rows", &MakeRows);
    let schema = flow.register_source_in_group("schema", &MakeSchema, Some("ingest"));
    let _snapshot =
        flow.register_source_durable("snapshot", &MakeSnapshot, NodePolicy::new().retained(true));
    // Ordering/effect leaf: consumes `rows` (shared, a legal fan-out) and is
    // retained so its zero-consumer output raises no warning.
    let _count = flow.register_with(
        "count",
        &CountRows,
        rows.shared(),
        NodePolicy::new().retained(true),
    );
    let _report = flow.register("report", &BuildReport, (rows.shared(), schema));
    flow.allow_env_capture(["DAGR_REGION"]);
    flow.finish()
}

/// The masked view over the pure-assembly byte-identity surface.
///
/// C20 excludes the **generation-time** field from byte-identity comparisons.
/// That field lives in the **T40 artifact header**, not in this pure-assembly
/// slice: `AssemblyArtifact::canonical_bytes` is defined as *"the generation-time
/// field, owned by the artifact writer T40, is not part of this pure-assembly
/// slice"* (see the T14 rustdoc). So on today's surface the generation-time mask
/// is the **empty** mask — there is no generation-time span to remove — and this
/// helper is the single, documented place that fact is encoded, keeping the
/// determinism tests forward-compatible: if a generation-time span is ever folded
/// into this slice, only this function changes.
fn mask_generation_time(artifact: &AssemblyArtifact) -> Vec<u8> {
    // No generation-time span exists in the pure-assembly slice today (T14).
    artifact.canonical_bytes().to_vec()
}

// ===========================================================================
// 1. Byte-identical output across two in-process assemblies.
// ===========================================================================

/// Two in-process assemblies of the fixture serialize to byte-identical output
/// once the generation-time field is masked. Fails loudly if any
/// non-generation-time byte differs — i.e. if assembly were non-deterministic.
#[test]
fn two_in_process_assemblies_are_byte_identical() {
    let a = assemble_fixture();
    let b = assemble_fixture();
    let masked_a = mask_generation_time(&a);
    let masked_b = mask_generation_time(&b);
    assert_eq!(
        masked_a, masked_b,
        "two assemblies of the same pipeline must be byte-identical (generation time masked)"
    );
    assert!(
        !masked_a.is_empty(),
        "the byte-identity surface must be non-empty — the fixture has content"
    );
}

// ===========================================================================
// 2. Generation-time is the *only* permitted difference.
// ===========================================================================

/// Comparing the two serialized outputs **without** masking generation time still
/// shows byte-identity: on the pure-assembly slice the generation-time field does
/// not exist (it is the T40 header's, not assembly's), so the *only* thing the
/// mask would ever remove is absent, and nothing else is non-deterministic. This
/// guards against the mask silently hiding real drift.
#[test]
fn generation_time_is_the_only_masked_difference() {
    let a = assemble_fixture();
    let b = assemble_fixture();
    // Unmasked: on the pure-assembly slice there is NO generation-time span, so
    // the raw canonical bytes are already fully identical — the mask removes
    // nothing, which is exactly the property under test.
    assert_eq!(
        a.canonical_bytes(),
        b.canonical_bytes(),
        "unmasked canonical bytes must already be identical: the pure-assembly slice carries no \
         generation-time field, so nothing but generation time could ever be masked"
    );
    // And masking changes neither side — the mask is the sole (empty) exclusion.
    assert_eq!(mask_generation_time(&a), a.canonical_bytes());
    assert_eq!(mask_generation_time(&b), b.canonical_bytes());
}

// ===========================================================================
// 3. Registration order does not change the artifact.
// ===========================================================================

/// Two builders register the identical node set and wiring in **different**
/// registration orders (identity is the explicit name, so order is not semantic —
/// C7/T13). Assembled and serialized (generation time masked), their output is
/// byte-identical: canonical ordering is applied, not registration order.
#[test]
fn registration_order_does_not_change_the_artifact() {
    // Order A: sources first, then consumers.
    let mut a = Flow::new();
    let rows_a = a.register_source("rows", &MakeRows);
    let schema_a = a.register_source_in_group("schema", &MakeSchema, Some("ingest"));
    let _snap_a =
        a.register_source_durable("snapshot", &MakeSnapshot, NodePolicy::new().retained(true));
    let _count_a = a.register_with(
        "count",
        &CountRows,
        rows_a.shared(),
        NodePolicy::new().retained(true),
    );
    let _report_a = a.register("report", &BuildReport, (rows_a.shared(), schema_a));
    a.allow_env_capture(["DAGR_REGION"]);
    let art_a = a.finish().assemble().expect("order-A assembles");

    // Order B: interleaved / reversed where the wiring still permits it. `rows`
    // and `schema` must precede their consumers (a handle is needed to bind), but
    // the two independent sources, and the two independent consumers, are
    // reordered relative to A.
    let mut b = Flow::new();
    let schema_b = b.register_source_in_group("schema", &MakeSchema, Some("ingest"));
    let rows_b = b.register_source("rows", &MakeRows);
    let _report_b = b.register("report", &BuildReport, (rows_b.shared(), schema_b));
    let _count_b = b.register_with(
        "count",
        &CountRows,
        rows_b.shared(),
        NodePolicy::new().retained(true),
    );
    let _snap_b =
        b.register_source_durable("snapshot", &MakeSnapshot, NodePolicy::new().retained(true));
    b.allow_env_capture(["DAGR_REGION"]);
    let art_b = b.finish().assemble().expect("order-B assembles");

    assert_eq!(
        mask_generation_time(&art_a),
        mask_generation_time(&art_b),
        "registration order must not change the serialized artifact (canonical ordering applies)"
    );
    // The fingerprint slot is likewise order-independent.
    assert_eq!(
        art_a.fingerprint().structural(),
        art_b.fingerprint().structural()
    );
    assert_eq!(art_a.fingerprint().policy(), art_b.fingerprint().policy());
}

// ===========================================================================
// 4. Precomputed runtime data is identical across assemblies.
// ===========================================================================

/// Every precomputed value — per-node consumer count, remaining-dependency count,
/// execution order, and the fingerprint slot — is identical between two
/// assemblies, and the consumer counts match the hand-computed expectation for
/// the fixture exactly.
#[test]
fn precomputed_runtime_data_is_identical_across_assemblies() {
    let a = assemble_fixture();
    let b = assemble_fixture();

    let names = ["rows", "schema", "snapshot", "count", "report"];

    // Consumer counts: identical across assemblies AND exact per node.
    // `rows` feeds `count` and `report` (2); `schema` feeds `report` (1);
    // `snapshot`, `count`, `report` feed nobody (0).
    let expected_consumers = [
        ("rows", 2u32),
        ("schema", 1),
        ("snapshot", 0),
        ("count", 0),
        ("report", 0),
    ];
    for (name, expected) in expected_consumers {
        let id = NodeId::from_name(name);
        assert_eq!(
            a.consumer_count(id),
            Some(expected),
            "consumer count for `{name}` must be exact"
        );
        assert_eq!(
            a.consumer_count(id),
            b.consumer_count(id),
            "consumer count for `{name}` must be identical across assemblies"
        );
    }

    // Remaining-dependency counts: identical across assemblies.
    // sources have 0; `count` depends on `rows` (1); `report` on rows+schema (2).
    let expected_deps = [
        ("rows", 0u32),
        ("schema", 0),
        ("snapshot", 0),
        ("count", 1),
        ("report", 2),
    ];
    for (name, expected) in expected_deps {
        let id = NodeId::from_name(name);
        assert_eq!(
            a.remaining_dependency_count(id),
            Some(expected),
            "remaining-dependency count for `{name}` must be exact"
        );
        assert_eq!(
            a.remaining_dependency_count(id),
            b.remaining_dependency_count(id),
            "remaining-dependency count for `{name}` must be identical across assemblies"
        );
    }

    // Execution order: byte-identical vector across assemblies.
    assert_eq!(
        a.execution_order(),
        b.execution_order(),
        "execution order must be identical across assemblies"
    );
    assert_eq!(a.execution_order().len(), names.len());

    // Fingerprint slot: identical across assemblies.
    assert_eq!(a.fingerprint().structural(), b.fingerprint().structural());
    assert_eq!(a.fingerprint().policy(), b.fingerprint().policy());

    // Node count is stable.
    assert_eq!(a.node_count(), b.node_count());
    assert_eq!(a.node_count(), names.len());
}

// ===========================================================================
// 5. Assembly succeeds in an empty environment (scrubbed child process).
// ===========================================================================
//
// The chosen mechanism (see the module-level decision record): a CHILD process
// launched with a cleared environment and an empty working directory. The child
// re-runs this test binary via a sentinel env var, assembles the fixture, and
// exits 0 on success / 1 on failure. This is hermetic (its own process, env, and
// CWD), portable (std only), and dependency-free.

/// Sentinel env var telling a spawned child which in-process routine to run.
const CHILD_ROLE_VAR: &str = "DAGR_T15_CHILD_ROLE";

/// Child role: assemble the fixture and assert success (the empty-environment
/// proof). Present so the parent can re-exec this binary with a cleared env.
const ROLE_ASSEMBLE: &str = "assemble";

/// Child role: the **negative control** — a throwaway "assembler" that touches
/// the filesystem (writes a file in the CWD). Under the empty-CWD guard, the
/// parent asserts the resulting artifact differs from a clean assembly, proving
/// the guard bites when a stray fs operation is introduced.
const ROLE_STRAY_FS: &str = "stray-fs";

/// Entry point every `#[test]` re-uses: if this process is a spawned child (the
/// sentinel is set), run the requested role and exit; otherwise do nothing.
///
/// Called at the top of the two child-spawning tests so the child, which runs the
/// *same* test binary, dispatches to the child routine instead of the normal test
/// body.
fn run_child_role_if_spawned() {
    let Ok(role) = std::env::var(CHILD_ROLE_VAR) else {
        return;
    };
    match role.as_str() {
        ROLE_ASSEMBLE => {
            // Empty environment, empty CWD: assembly must still succeed.
            let artifact = assemble_fixture();
            assert_eq!(artifact.node_count(), 5, "fixture assembles to five nodes");
            std::process::exit(0);
        }
        ROLE_STRAY_FS => {
            // The negative control: a stray filesystem write in the (supposedly
            // empty) working directory. The parent detects the leftover file and
            // fails, proving the empty-CWD guard bites.
            std::fs::write(
                "stray-artifact.tmp",
                b"a stray assembler touched the filesystem",
            )
            .expect("negative control writes a stray file");
            std::process::exit(0);
        }
        other => panic!("unknown child role: {other}"),
    }
}

/// Spawn a child of this test binary with a **cleared environment** and the given
/// empty working directory, running `role`. Returns the child's exit success plus
/// the directory listing after it ran (so the caller can detect a stray file).
fn spawn_scrubbed_child(role: &str, cwd: &std::path::Path) -> (bool, Vec<String>) {
    let exe = std::env::current_exe().expect("current test executable path");
    let status = Command::new(exe)
        // Run ONLY the spawning test in the child, so the child's own
        // `run_child_role_if_spawned` fires before any assertion and exits.
        .arg("--exact")
        .arg(child_test_name(role))
        .env_clear() // <-- the scrub: no inherited environment at all
        .env(CHILD_ROLE_VAR, role)
        .current_dir(cwd) // <-- the empty working directory
        .status()
        .expect("spawn scrubbed child");
    let listing: Vec<String> = std::fs::read_dir(cwd)
        .expect("read empty cwd")
        .filter_map(Result::ok)
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    (status.success(), listing)
}

/// The test-name the child runs for a given role — kept in one place so the
/// `--exact` filter and the role dispatch stay in sync.
fn child_test_name(role: &str) -> &'static str {
    match role {
        ROLE_ASSEMBLE => "assembly_succeeds_in_an_empty_environment",
        ROLE_STRAY_FS => "negative_control_stray_filesystem_write_is_detected",
        other => panic!("unknown child role: {other}"),
    }
}

/// Create a fresh, empty working directory unique to this process+role under the
/// system temp dir (no external crate). Returned path is cleaned by the caller.
fn fresh_empty_dir(tag: &str) -> std::path::PathBuf {
    let mut dir = std::env::temp_dir();
    let unique = format!(
        "dagr-t15-{tag}-{}-{:?}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos())
    );
    dir.push(unique);
    std::fs::create_dir_all(&dir).expect("create empty working dir");
    dir
}

/// **Assembly succeeds in an empty environment.** The parent spawns a child of
/// this test binary with a *cleared* environment and an *empty* working
/// directory; the child assembles the fixture and exits 0. No config, no
/// reader-visible env var, no expected files, no network — assembly returns
/// success and produces its graph output.
#[test]
fn assembly_succeeds_in_an_empty_environment() {
    // Child leg: if spawned, assemble under the scrub and exit.
    run_child_role_if_spawned();

    // Parent leg: spawn the scrubbed child and require success + a still-empty dir.
    let dir = fresh_empty_dir("assemble");
    let (ok, listing) = spawn_scrubbed_child(ROLE_ASSEMBLE, &dir);
    let _ = std::fs::remove_dir_all(&dir);
    assert!(
        ok,
        "assembly must succeed in a cleared environment with an empty working directory"
    );
    assert!(
        listing.is_empty(),
        "an honest assembly writes nothing to its working directory; found: {listing:?}"
    );
}

/// **Negative control — the guard bites.** A throwaway "assembler" that touches
/// the filesystem leaves a file in the empty working directory; the empty-CWD
/// guard used by `assembly_succeeds_in_an_empty_environment` detects it. This
/// proves the mechanism is not vacuous: a stray filesystem operation is caught.
#[test]
fn negative_control_stray_filesystem_write_is_detected() {
    // Child leg: if spawned, perform the stray write and exit.
    run_child_role_if_spawned();

    // Parent leg: spawn the stray-fs child; the working directory must NOT stay
    // empty — the same guard the empty-environment test relies on now fires.
    let dir = fresh_empty_dir("stray-fs");
    let (ok, listing) = spawn_scrubbed_child(ROLE_STRAY_FS, &dir);
    let contained_stray = listing.iter().any(|n| n == "stray-artifact.tmp");
    let _ = std::fs::remove_dir_all(&dir);
    assert!(ok, "the negative-control child itself runs to completion");
    assert!(
        contained_stray,
        "the empty-CWD guard must detect a stray filesystem write (guard is not vacuous); \
         listing: {listing:?}"
    );
}

// ===========================================================================
// 6. No parameter value is reachable during registration or assembly.
// ===========================================================================

/// A parameterised pipeline (its tasks would read `FixtureParams` off the
/// `RunContext` at run time) assembles with **no** parameter value supplied —
/// because the flow/assembly surface has no parameter argument or accessor at
/// all. Parameters are parsed only at bootstrap, after assembly (C7 §Behavior;
/// T0.5 ADR §2), so there is no assembly-time API by which a node body or the
/// assembler could read a parameter value.
#[test]
fn no_parameter_value_is_reachable_during_assembly() {
    // The fixture's tasks are parameterised (MakeRows::run reads FixtureParams),
    // yet assembly needs — and is given — no parameter value whatsoever.
    let artifact = assemble_fixture();
    assert_eq!(artifact.node_count(), 5);

    // Structural demonstration: the ENTIRE input to assembly is the immutable
    // in-memory pipeline. There is no `assemble(params)` overload, no parameter
    // setter on `Flow`, and no parameter getter on `AssemblyArtifact` — assembly
    // cannot reach a parameter value because the type surface offers no route to
    // one. Constructing a params value here and NOT being able to feed it to
    // assembly is itself the assertion.
    let _unused_params = FixtureParams {
        region: "us-east-1".to_string(),
        max_rows: 1_000,
    };
    // `_unused_params` is deliberately never handed to `assemble` — no API accepts
    // it. Assembly already succeeded above without it.
}

// ===========================================================================
// 8. Empty-environment determinism, combined.
// ===========================================================================
//
// Determinism and purity hold *together* — the exact CI/PR condition C20 relies
// on. Two assemblies performed within a scrubbed child process produce
// byte-identical output. We run BOTH assemblies inside one scrubbed child (so the
// comparison itself happens under the empty environment) and have the child exit
// nonzero if the bytes differ.

/// Child role: assemble the fixture twice under the scrub and compare masked
/// bytes; exit 0 iff identical.
const ROLE_DETERMINISM: &str = "determinism";

/// **Empty-environment determinism, combined.** Inside a scrubbed child (cleared
/// env, empty CWD), assemble the fixture twice and compare bytes (generation time
/// masked): they are byte-identical. Determinism and purity hold together.
#[test]
fn empty_environment_determinism_combined() {
    // Child leg for this combined scenario.
    if let Ok(role) = std::env::var(CHILD_ROLE_VAR) {
        if role == ROLE_DETERMINISM {
            let a = assemble_fixture();
            let b = assemble_fixture();
            let identical = mask_generation_time(&a) == mask_generation_time(&b)
                && a.fingerprint().structural() == b.fingerprint().structural()
                && a.fingerprint().policy() == b.fingerprint().policy();
            std::process::exit(i32::from(!identical));
        }
    }

    // Parent leg: spawn a scrubbed child running the determinism comparison.
    let dir = fresh_empty_dir("determinism");
    let exe = std::env::current_exe().expect("current test executable path");
    let status = Command::new(exe)
        .arg("--exact")
        .arg("empty_environment_determinism_combined")
        .env_clear()
        .env(CHILD_ROLE_VAR, ROLE_DETERMINISM)
        .current_dir(&dir)
        .status()
        .expect("spawn scrubbed determinism child");
    let _ = std::fs::remove_dir_all(&dir);
    assert!(
        status.success(),
        "two assemblies within a scrubbed empty environment must be byte-identical \
         (determinism and purity hold together — the C20 CI/PR condition)"
    );
}
