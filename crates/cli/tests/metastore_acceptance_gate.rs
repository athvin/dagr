//! **T88 — the M7 metastore acceptance gate.** The end-to-end proof of M7's
//! done-when ("one binary hosting many DAGs writes and serves a single, queryable,
//! guaranteed run index — without touching the zero-dep core or crossing the
//! coordination boundary") plus the milestone's invariants, asserted in one place —
//! exactly as `system_acceptance_gate.rs` (T65) does for the whole product and the
//! milestone demos do for M1–M4.
//!
//! Like every gate ticket this file **adds zero capability**: every assertion below
//! composes already-merged behavior (T83–T87). If an assertion ever surfaces missing
//! behavior, that is a STOP pointing at the owning ticket, never new code here.
//!
//! # What is asserted
//!
//! - **End-to-end** (a): from a **many-DAGs binary** — `metastore init`, several DAGs
//!   run **live** as separate OS processes against one store, `sync <base>` — the
//!   store is complete and consistent and a **native `sqlite3`** query returns the
//!   expected cross-run aggregate.
//! - **Guarantee** (b): a durable metastore-write failure surfaces as a `SinkFault`
//!   in the run's exit code (composed from T86) — the write is guaranteed, not
//!   best-effort.
//! - **Parity** (c): live-produced rows equal reconcile-produced rows for the same
//!   runs.
//!
//! The **boundary** invariants (d) are *structural* and CI-enforced, not runtime, so
//! they live in `scripts/check-metastore-acceptance-boundary.sh` (a `cargo tree` /
//! crate-graph / diff check a unit test cannot reach), wired into the CI gate job.
//!
//! # Coverage-matrix posture
//!
//! M7 is entirely behind the default-off `metastore` feature and maps to **no new
//! machine criterion** in the system-level acceptance partition (`SL1`–`SL8`) — the
//! same posture as every prior M7 ticket (T83–T87 add no coverage-matrix row). This
//! gate therefore adds no matrix entry; it proves the *milestone's* done-when, which
//! sits under the permanent-non-goals carve-out (arch.md "What it is not": a *local,
//! embedded, opt-in, non-coordinating* run index), not under a new SL criterion.
//!
//! # Determinism / portability
//!
//! The whole file is `#![cfg(feature = "metastore")]`; CI runs it with `--features
//! metastore` on both `ubuntu-latest` and `macos-latest`. Every step uses a PRIVATE
//! temp base (pid + nanos), the registry's deterministic `TickClock`, and **no fixed
//! sleeps** — the multi-process step joins all worker processes before asserting (the
//! T85 rendezvous discipline: disjoint rows keyed per DAG, so genuine overlap is
//! safe and the assertions are count-based and order-independent). `sqlite3`-backed
//! steps skip when `sqlite3` is absent unless `DAGR_REQUIRE_SQLITE3=1` (set in CI).

#![cfg(feature = "metastore")]

use std::path::{Path, PathBuf};
use std::process::{Child, Command};

use dagr_cli::driver::{RunConfig, ShutdownExit};
use dagr_cli::metastore_tee::{stream_path, RunSink};
use dagr_cli::run_flow::RunnableFlow;
use dagr_cli::run_store::TickClock;
use dagr_core::context::RunContext;
use dagr_core::task::Task;
use dagr_core::TaskError;

use dagr_metastore::store::OpenMode;
use dagr_metastore::MetaStore;

// ===========================================================================
// Harness
// ===========================================================================

/// Locate the repository root from this crate's manifest directory (`crates/cli`).
fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("crates/cli has a two-level ancestor (the repo root)")
        .to_path_buf()
}

/// A private, disjoint temp base under the OS temp dir (pid + nanos), so concurrent
/// test binaries never collide and each run's store is inspectable in isolation.
fn temp_base(tag: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "dagr-t88-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

/// Whether `sqlite3` is on PATH. CI installs it on every runner image; a dev machine
/// without it **skips** (matching the T87 convention). `DAGR_REQUIRE_SQLITE3=1` turns
/// an absent `sqlite3` into a hard failure (set in CI).
fn sqlite3_available() -> bool {
    let present = Command::new("sqlite3")
        .arg("--version")
        .output()
        .is_ok_and(|o| o.status.success());
    if present {
        return true;
    }
    assert!(
        std::env::var_os("DAGR_REQUIRE_SQLITE3").is_none(),
        "sqlite3 is required (DAGR_REQUIRE_SQLITE3=1) but is not on PATH"
    );
    eprintln!("skipping: `sqlite3` not found on PATH (set DAGR_REQUIRE_SQLITE3=1 to require it)");
    false
}

/// Run one `sqlite3 <db> "<sql>"` and return trimmed stdout. Uses **plain
/// `sqlite3`** — the native-access story: the libSQL file is byte-compatible with
/// stock `SQLite`, so no dagr tool is needed to read the index.
fn sqlite3(db: &Path, sql: &str) -> String {
    let out = Command::new("sqlite3")
        .arg(db)
        .arg(sql)
        .output()
        .expect("spawn sqlite3");
    assert!(
        out.status.success(),
        "sqlite3 query failed: {sql}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

/// Spawn the **real `dagr` binary** (built with `--features metastore`, so it carries
/// the `metastore init`/`sync` verbs) with `args`, and return the finished output.
/// The gate composes the many-DAGs *binary* (the example) with `dagr`'s store-level
/// `metastore` verbs against one store file — the verbs are store-level and
/// binary-agnostic, so this is legitimate composition, not new capability.
fn run_dagr(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO"))
        .current_dir(repo_root())
        .env("DAGR_NO_BANNER", "1")
        .args([
            "run",
            "--quiet",
            "-p",
            "dagr-cli",
            "--features",
            "metastore",
            "--bin",
            "dagr",
            "--",
        ])
        .args(args)
        .output()
        .expect("failed to spawn `cargo run --bin dagr --features metastore`")
}

/// Spawn the `many_dags` example (built with `--features metastore`, so its `run`s
/// carry the live tee) **without waiting** — the caller launches all DAG processes,
/// then joins them, forcing genuine multi-process overlap on one store.
fn spawn_example(args: &[&str]) -> Child {
    Command::new(env!("CARGO"))
        .current_dir(repo_root())
        .env("DAGR_NO_BANNER", "1")
        .args([
            "run",
            "--quiet",
            "-p",
            "dagr-cli",
            "--features",
            "metastore",
            "--example",
            "many_dags",
            "--",
        ])
        .args(args)
        .spawn()
        .expect("failed to spawn `cargo run --example many_dags --features metastore`")
}

/// The three DAGs the `many_dags` example declares.
const DAGS: [&str; 3] = ["alpha", "beta", "gamma"];

/// Pre-build the example + binary once (a cold `cargo run` can take minutes on a CI
/// runner), so the concurrent `run <dag>` processes start their write windows close
/// together rather than serializing behind three independent cold compiles.
fn prebuild() {
    let status = Command::new(env!("CARGO"))
        .current_dir(repo_root())
        .args([
            "build",
            "--quiet",
            "-p",
            "dagr-cli",
            "--features",
            "metastore",
            "--bin",
            "dagr",
            "--example",
            "many_dags",
        ])
        .status()
        .expect("spawn cargo build");
    assert!(
        status.success(),
        "pre-building the example + dagr binary failed"
    );
}

// ===========================================================================
// (a) END-TO-END: init -> live multi-process runs -> sync -> native query
// ===========================================================================

/// The headline M7 done-when, end to end, from a **many-DAGs binary**:
///
/// 1. `dagr metastore init --store <base>/metastore.db` creates the index.
/// 2. `many_dags run <alpha|beta|gamma> --dagr.metastore` runs live **as three
///    separate OS processes launched together** against that one store (the
///    multi-process live-write path — T85/T86 proved it is lossless; the disjoint
///    per-DAG run ids keep the writes safe under overlap).
/// 3. `dagr metastore sync <base>` reconciles the run store against the same index —
///    an idempotent UPSERT over the already-live-written runs, proving the store is
///    complete and consistent (no duplicate row, no missing row).
/// 4. A **native `sqlite3`** query returns the expected cross-run aggregate — one
///    `dag` row per DAG and complete `dag_run`/`node_attempt`/`node_terminal` rows.
#[test]
fn end_to_end_init_live_multiprocess_sync_and_native_query() {
    if !sqlite3_available() {
        return;
    }
    prebuild();

    let base = temp_base("e2e");
    let runs = base.join("runs");
    std::fs::create_dir_all(&runs).expect("mk run-store base");
    let store = runs.join("metastore.db");
    let store_arg = store.to_str().unwrap();
    let runs_arg = runs.to_str().unwrap();

    // 1. init — the store is created before any run touches it.
    let init = run_dagr(&["metastore", "init", "--store", store_arg]);
    assert_eq!(
        init.status.code(),
        Some(0),
        "`metastore init` exits Success\nstderr:\n{}",
        String::from_utf8_lossy(&init.stderr)
    );
    assert!(store.exists(), "`metastore init` created the store file");

    // 2. run all three DAGs LIVE, as separate processes launched together — genuine
    // multi-process overlap on one store (no sleep; disjoint per-DAG run ids).
    let children: Vec<(&str, Child)> = DAGS
        .iter()
        .map(|dag| {
            (
                *dag,
                spawn_example(&["run", dag, "--store", runs_arg, "--dagr.metastore"]),
            )
        })
        .collect();
    for (dag, child) in children {
        let out = child.wait_with_output().expect("join example process");
        assert_eq!(
            out.status.code(),
            Some(0),
            "live multi-process `run {dag}` exits Success\nstderr:\n{}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    // The live tee already populated the store across the three processes.
    let live_dags = sqlite3(&store, "SELECT name FROM dag ORDER BY name");
    assert_eq!(
        live_dags, "alpha\nbeta\ngamma",
        "the live multi-process runs populated one dag row per DAG"
    );

    // 3. sync <base> — reconcile the run store into the same index. Because the live
    // tee already wrote every row, this is an idempotent UPSERT: it must add NO new
    // rows (a run's row set is stable) and leave the store complete + consistent.
    let before = row_counts(&store);
    let sync = run_dagr(&["metastore", "sync", "--store", store_arg, runs_arg]);
    assert_eq!(
        sync.status.code(),
        Some(0),
        "`metastore sync <base>` exits Success\nstderr:\n{}",
        String::from_utf8_lossy(&sync.stderr)
    );
    let after = row_counts(&store);
    assert_eq!(
        before, after,
        "sync over already-live-written runs is idempotent (complete + consistent: \
         it neither duplicated nor dropped a row)"
    );

    // 4. NATIVE cross-run aggregate via plain sqlite3 — the queryable-index proof.
    // One succeeded dag_run per DAG.
    let runs_by_dag = sqlite3(
        &store,
        "SELECT dag_id, count(*) FROM dag_run WHERE state='succeeded' GROUP BY dag_id ORDER BY dag_id",
    );
    assert_eq!(
        runs_by_dag, "alpha|1\nbeta|1\ngamma|1",
        "exactly one succeeded run per DAG in the index"
    );
    // The cross-run terminal-state view joins node_terminal back to its DAG: alpha's
    // two nodes (extract, load) + beta/gamma's single aggregate node.
    let terminals = sqlite3(
        &store,
        "SELECT dr.dag_id, nt.node_id, nt.state FROM node_terminal nt \
         JOIN dag_run dr ON dr.run_id = nt.run_id ORDER BY dr.dag_id, nt.node_id",
    );
    assert_eq!(
        terminals,
        "alpha|extract|succeeded\nalpha|load|succeeded\nbeta|aggregate|succeeded\ngamma|aggregate|succeeded",
        "the cross-run node_terminal view is complete and joins back to each DAG"
    );
    // Four succeeded attempts total (2 + 1 + 1) across the three runs.
    let attempts = sqlite3(
        &store,
        "SELECT count(*) FROM node_attempt WHERE state='succeeded'",
    );
    assert_eq!(
        attempts, "4",
        "every node attempt landed (alpha 2 + beta 1 + gamma 1)"
    );

    let _ = std::fs::remove_dir_all(&base);
}

/// The four M7 index tables' row counts — the completeness/consistency fingerprint
/// the end-to-end test compares across the live→sync boundary (an idempotent sync
/// must not move any of them). Read through plain `sqlite3` (native access).
fn row_counts(store: &Path) -> [i64; 4] {
    ["dag", "dag_version", "dag_run", "node_attempt"].map(|t| {
        sqlite3(store, &format!("SELECT count(*) FROM {t}"))
            .parse::<i64>()
            .expect("count is an integer")
    })
}

// ===========================================================================
// (b) GUARANTEE: a durable metastore-write failure ⇒ a SinkFault in the exit code
// ===========================================================================

/// A source producing a fixed number (the T86 fixture shape, so the gate composes the
/// same guaranteed-write path).
struct Count {
    up_to: u64,
}
impl Task for Count {
    type Input = ();
    type Output = u64;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<u64, TaskError> {
        Ok(self.up_to)
    }
}

/// A sink doubling its input.
struct Double;
impl Task for Double {
    type Input = u64;
    type Output = u64;
    async fn run(&mut self, _c: &RunContext, input: u64) -> Result<u64, TaskError> {
        Ok(input * 2)
    }
}

/// Build a fresh two-node flow (count → double).
fn two_node_flow() -> RunnableFlow {
    let mut flow = RunnableFlow::new();
    let counted = flow.register_source("count", Count { up_to: 21 });
    let _doubled = flow.register::<Double, _>("double", Double, counted);
    flow
}

/// **The guaranteed-write invariant, re-asserted at the gate (composed from T86).** A
/// durable metastore-write failure is NOT swallowed: it surfaces as a `SinkFault`
/// that the driver folds into its shutdown exit-code precedence as the distinct
/// sink-failure selection. We inject the failure by holding the store's single WAL
/// write lock from a competing connection while the run projects, with the live
/// sink's store pinned to a one-attempt retry cap so the contention is a hard fault.
#[test]
fn a_metastore_write_failure_surfaces_as_the_sink_failure_exit_code() {
    use dagr_metastore::store::RetryPolicy;
    use dagr_metastore::MetastoreSink;

    let base = temp_base("fault");
    let base_str = base.to_string_lossy().to_string();
    std::fs::create_dir_all(&base).expect("mk base");
    let store_path = base.join("metastore.db");
    let run_id = "run-fault";
    let stream = stream_path(&base_str, "pipe", run_id);

    // A one-attempt cap ⇒ a held write lock is a hard fault (no retry can outlast it).
    let one_shot = RetryPolicy {
        max_attempts: 1,
        base_backoff: std::time::Duration::from_millis(1),
        max_backoff: std::time::Duration::from_millis(1),
    };
    let events_path = stream.to_string_lossy().into_owned();
    let metastore = MetastoreSink::open_with_retry(store_path.clone(), Some(events_path), one_shot)
        .expect("open the live sink");
    let file = dagr_cli::run_store::FileSink::create(&stream).expect("open events.jsonl");
    let sink = RunSink::Tee { file, metastore };

    // A competing writer holds the WAL write lock for the whole run, so every
    // projection the live sink attempts hits SQLITE_BUSY past its one-attempt cap.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("contender runtime");
    let contender = rt.block_on(async {
        let s = MetaStore::open(OpenMode::LocalFile(store_path.clone()))
            .await
            .expect("open contender");
        s.connection()
            .execute("BEGIN IMMEDIATE", ())
            .await
            .expect("contender takes the write lock");
        s
    });

    let config = RunConfig::new(&base_str).run_id(run_id);
    let report = two_node_flow()
        .run("pipe", &config, sink, TickClock::default())
        .expect("the flow assembles (the failure is a SINK fault, not assembly)");

    assert_eq!(
        report.driver_report().shutdown_exit,
        ShutdownExit::SinkFailure,
        "a durable metastore-write failure surfaces as the distinct sink-failure exit \
         (guaranteed write, not best-effort)"
    );

    rt.block_on(async {
        let _ = contender.connection().execute("ROLLBACK", ()).await;
    });
    let _ = std::fs::remove_dir_all(&base);
}

// ===========================================================================
// (c) PARITY: live-produced rows == reconcile-produced rows for the same runs
// ===========================================================================

/// **The parity invariant.** The same runs projected **live** (the tee, toggle on)
/// and **reconciled** (toggle off, then `sync`) produce **identical** index rows.
/// Proven end to end from the example binary: run the three DAGs into a live store,
/// run the same three into a second base with the toggle OFF (no live write), then
/// `sync` that second base — the two stores' `dag` / `dag_run` / `node_attempt` /
/// `node_terminal` row sets must match exactly (state + counts + per-node terminals).
#[test]
fn live_produced_rows_equal_reconcile_produced_rows() {
    if !sqlite3_available() {
        return;
    }
    prebuild();

    // --- LIVE store: three DAGs with the tee on.
    let live_base = temp_base("parity-live");
    let live_runs = live_base.join("runs");
    std::fs::create_dir_all(&live_runs).expect("mk live base");
    let live_store = live_runs.join("metastore.db");
    for dag in DAGS {
        let child = spawn_example(&[
            "run",
            dag,
            "--store",
            live_runs.to_str().unwrap(),
            "--dagr.metastore",
        ]);
        let out = child.wait_with_output().expect("join live run");
        assert_eq!(out.status.code(), Some(0), "live `run {dag}` succeeds");
    }
    assert!(live_store.exists(), "the live tee created the live store");

    // --- RECONCILE store: same three DAGs, toggle OFF (plain FileSink, no libsql
    // activity), then a `sync` of the run store into a fresh index.
    let rec_base = temp_base("parity-reconcile");
    let rec_runs = rec_base.join("runs");
    std::fs::create_dir_all(&rec_runs).expect("mk reconcile base");
    let rec_store = rec_runs.join("metastore.db");
    for dag in DAGS {
        let child = spawn_example(&["run", dag, "--store", rec_runs.to_str().unwrap()]);
        let out = child.wait_with_output().expect("join reconcile run");
        assert_eq!(
            out.status.code(),
            Some(0),
            "toggle-off `run {dag}` succeeds"
        );
    }
    // Toggle-off runs created NO index — the feature is additive.
    assert!(
        !rec_store.exists(),
        "toggle-off runs create no metastore.db (no libsql activity)"
    );
    let sync = run_dagr(&[
        "metastore",
        "sync",
        "--store",
        rec_store.to_str().unwrap(),
        rec_runs.to_str().unwrap(),
    ]);
    assert_eq!(
        sync.status.code(),
        Some(0),
        "`sync` of the toggle-off run store succeeds\nstderr:\n{}",
        String::from_utf8_lossy(&sync.stderr)
    );

    // --- Compare the two indexes on everything a run projects. The run ids differ
    // per invocation (each `run` mints its own), so parity is asserted on the
    // per-DAG shape the projection carries — not on the volatile run id.
    for (label, q) in parity_projections() {
        let live = sqlite3(&live_store, q);
        let rec = sqlite3(&rec_store, q);
        assert_eq!(
            live, rec,
            "live == reconcile for `{label}`\nlive:\n{live}\nreconcile:\n{rec}"
        );
    }

    let _ = std::fs::remove_dir_all(&live_base);
    let _ = std::fs::remove_dir_all(&rec_base);
}

/// The projection queries the parity test compares between the live and reconciled
/// stores. Each is keyed on the DAG (stable across invocations), never the volatile
/// per-run `run_id`, so the two indexes are compared on the *shape* the projection
/// produces — the whole point of the "live == reconcile" invariant.
fn parity_projections() -> Vec<(&'static str, &'static str)> {
    vec![
        ("dag names", "SELECT name FROM dag ORDER BY name"),
        (
            "runs per dag by state",
            "SELECT dag_id, state, count(*) FROM dag_run GROUP BY dag_id, state ORDER BY dag_id, state",
        ),
        (
            "attempts per dag+node by state",
            "SELECT dr.dag_id, na.node_id, na.state, count(*) \
             FROM node_attempt na JOIN dag_run dr ON dr.run_id = na.run_id \
             GROUP BY dr.dag_id, na.node_id, na.state ORDER BY dr.dag_id, na.node_id, na.state",
        ),
        (
            "terminal state per dag+node",
            "SELECT dr.dag_id, nt.node_id, nt.state \
             FROM node_terminal nt JOIN dag_run dr ON dr.run_id = nt.run_id \
             ORDER BY dr.dag_id, nt.node_id",
        ),
    ]
}
