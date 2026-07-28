//! **T87 acceptance: the many-dags metastore example is real and the cookbook's
//! query block executes.** Written first (TDD), behind the default-off `metastore`
//! feature.
//!
//! T81 shipped the copyable many-dags `#[dag]` example; T83–T86 shipped the run
//! index (schema, reconcile, multi-process writes, the guaranteed live tee). This
//! ticket proves the headline story end to end — *many DAGs in one binary, one
//! queryable place for their state* — and that the cookbook teaches it without
//! overclaiming.
//!
//! These scenarios need the live tee, so the whole file is `#![cfg(feature =
//! "metastore")]`; CI runs it with `--features metastore` on both `ubuntu-latest`
//! and `macos-latest` (the ticket DoD). The docs-**truthfulness** guards that must
//! red a plain `cargo test --workspace` (no `libsql`) live in the always-compiled
//! sibling [`metastore_docs_claims.rs`](metastore_docs_claims.rs).
//!
//! The query block is not paraphrased: it is **extracted from
//! `docs/cookbook.md`** (the fenced `sqlite3` block in the "Querying run state
//! across DAGs" section) and executed verbatim against a store this test just
//! populated by running three of the example's DAGs — so the doc's commands
//! provably run and a rotted query reds the build.

#![cfg(feature = "metastore")]

use std::path::{Path, PathBuf};
use std::process::Command;

/// Locate the repository root from this crate's manifest directory (`crates/cli`).
fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("crates/cli has a two-level ancestor (the repo root)")
        .to_path_buf()
}

/// A unique run-store base under the OS temp dir, so concurrent test binaries never
/// collide and each test's store is inspectable in isolation.
fn temp_base(tag: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "dagr-t87-{tag}-{}-{:?}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    ))
}

/// Run the `many_dags` example (compiled **with** `--features metastore`, so the
/// live tee is present) with `args`, returning the exit code and stderr. The banner
/// is suppressed so stdout stays clean for any callers that read it.
fn run_example(args: &[&str]) -> (i32, String) {
    let out = Command::new(env!("CARGO"))
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
        .output()
        .expect("failed to spawn `cargo run --example many_dags --features metastore`");
    (
        out.status.code().unwrap_or(-1),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

/// Whether `sqlite3` is on PATH. CI installs it (it ships with every runner image),
/// but a dev machine without it should **skip**, not fail — matching the
/// render-reference-tools skip-if-absent convention. `DAGR_REQUIRE_SQLITE3=1` turns
/// an absent `sqlite3` into a hard failure (set in CI).
fn sqlite3_available() -> bool {
    if Command::new("sqlite3")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
    {
        return true;
    }
    if std::env::var_os("DAGR_REQUIRE_SQLITE3").is_some() {
        panic!("sqlite3 is required (DAGR_REQUIRE_SQLITE3=1) but is not on PATH");
    }
    eprintln!("skipping: `sqlite3` not found on PATH (set DAGR_REQUIRE_SQLITE3=1 to require it)");
    false
}

/// Run one `sqlite3 <db> "<sql>"` and return trimmed stdout. Uses **plain
/// `sqlite3`** — the whole point of the native-access story: the libSQL file is
/// byte-compatible with stock SQLite, so no dagr tool is needed to read it.
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

/// Populate one `metastore.db` by running the example's three DAGs (`alpha`,
/// `beta`, `gamma`) against one store base with the live tee toggled on
/// (`--dagr.metastore`), each as its own process — the multi-process live-write
/// path from T85/T86. Returns the store-base dir and the `metastore.db` path.
fn populate_store(tag: &str) -> (PathBuf, PathBuf) {
    let base = temp_base(tag);
    let runs = base.join("runs");
    let store = runs.join("metastore.db");
    for dag in ["alpha", "beta", "gamma"] {
        let (code, stderr) = run_example(&[
            "run",
            dag,
            "--store",
            runs.to_str().unwrap(),
            "--dagr.metastore",
        ]);
        assert_eq!(
            code, 0,
            "running `{dag}` with the live tee on exits Success, stderr:\n{stderr}"
        );
    }
    assert!(
        store.exists(),
        "running several DAGs with the toggle on created one metastore.db at {}",
        store.display()
    );
    (base, store)
}

// ===========================================================================
// The example builds+runs WITH the feature and populates one queryable store
// ===========================================================================

/// Running three of the example's DAGs against one store with `--dagr.metastore`
/// populates a single `metastore.db` with **one `dag` row per distinct DAG** plus
/// the expected `dag_run` / `node_attempt` / `node_terminal` rows — all queryable
/// with plain `sqlite3` (the native-access story). This is the multi-process
/// live-write path (each `run` is its own process) and the ticket's headline proof.
#[test]
fn running_several_dags_populates_one_queryable_store() {
    if !sqlite3_available() {
        return;
    }
    let (_base, store) = populate_store("populate");

    // One `dag` row per distinct DAG (alpha, beta, gamma), sorted.
    let dags = sqlite3(&store, "SELECT name FROM dag ORDER BY name");
    assert_eq!(
        dags, "alpha\nbeta\ngamma",
        "one dag row per DAG, queryable by plain sqlite3"
    );

    // Exactly one succeeded run per DAG (each `run <dag>` is its own run).
    let runs_by_dag = sqlite3(
        &store,
        "SELECT dag_id, count(*) FROM dag_run WHERE state='succeeded' GROUP BY dag_id ORDER BY dag_id",
    );
    assert_eq!(
        runs_by_dag, "alpha|1\nbeta|1\ngamma|1",
        "one succeeded dag_run per DAG"
    );

    // node_attempt rows exist: alpha has two nodes (extract, load); beta and gamma
    // one each (aggregate) — 4 succeeded attempts total across the three runs.
    let attempts = sqlite3(
        &store,
        "SELECT count(*) FROM node_attempt WHERE state='succeeded'",
    );
    assert_eq!(
        attempts, "4",
        "the succeeded node_attempt rows across the three DAGs (2 + 1 + 1)"
    );

    // node_terminal carries the single terminal state per node per run, joinable
    // back to the DAG — alpha's extract/load, beta/gamma's aggregate.
    let terminals = sqlite3(
        &store,
        "SELECT dr.dag_id, nt.node_id, nt.state FROM node_terminal nt \
         JOIN dag_run dr ON dr.run_id = nt.run_id ORDER BY dr.dag_id, nt.node_id",
    );
    assert_eq!(
        terminals,
        "alpha|extract|succeeded\nalpha|load|succeeded\nbeta|aggregate|succeeded\ngamma|aggregate|succeeded",
        "node_terminal joins back to the DAG for a cross-run terminal-state view"
    );
}

/// The example builds and runs **without** the feature too (the default build):
/// `run alpha` succeeds and writes an on-disk event stream, and **no** `metastore.db`
/// is created even when `--dagr.metastore` is passed — the feature is additive and a
/// default binary has zero `libsql` activity. Proven by spawning a *default* build of
/// the example (no `--features metastore`).
#[test]
fn the_default_build_runs_unchanged_and_creates_no_index() {
    let base = temp_base("default");
    let runs = base.join("runs");
    // A DEFAULT build of the example — note: no `--features metastore`.
    let out = Command::new(env!("CARGO"))
        .current_dir(repo_root())
        .env("DAGR_NO_BANNER", "1")
        .args([
            "run",
            "--quiet",
            "-p",
            "dagr-cli",
            "--example",
            "many_dags",
            "--",
            "run",
            "alpha",
            "--store",
            runs.to_str().unwrap(),
            // The toggle is passed but the feature is OFF, so it is inert: the flag
            // lives in the pipeline's trailing args and the feature-off build never
            // consults it.
            "--dagr.metastore",
        ])
        .output()
        .expect("spawn default many_dags");
    assert_eq!(
        out.status.code().unwrap_or(-1),
        0,
        "the default build runs alpha unchanged, stderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    // The run still wrote its event stream (the resume source of truth is unchanged).
    let ran = std::fs::read_dir(runs.join("alpha"))
        .map(|d| {
            d.flatten()
                .any(|e| e.path().join("events.jsonl").exists())
        })
        .unwrap_or(false);
    assert!(ran, "the default build still wrote alpha's event stream");
    // No index: a default build has no libsql edge, so no metastore.db is created.
    assert!(
        !runs.join("metastore.db").exists(),
        "the default (feature-off) build creates NO metastore.db — the feature is additive"
    );
    let _ = std::fs::remove_dir_all(&base);
}

// ===========================================================================
// The cookbook's copy-paste query block actually executes against the store
// ===========================================================================

/// Every fenced `sqlite3` query in the cookbook's **"Querying run state across
/// DAGs"** section is extracted verbatim and run against a freshly-populated store:
/// each executes without error (`sqlite3` exits 0). This is the docs-are-executable
/// guarantee — a rotted or renamed column reds the build. Each query's *shape* is
/// separately pinned by [`running_several_dags_populates_one_queryable_store`].
#[test]
fn the_cookbook_query_block_executes_against_a_populated_store() {
    if !sqlite3_available() {
        return;
    }
    let (_base, store) = populate_store("cookbook");

    let md = std::fs::read_to_string(repo_root().join("docs/cookbook.md"))
        .expect("read docs/cookbook.md");
    let queries = extract_cookbook_sqlite_queries(&md);
    assert!(
        queries.len() >= 3,
        "the cookbook's query section carries at least three example queries (found {})",
        queries.len()
    );
    for q in &queries {
        // Run it verbatim through plain sqlite3: a nonzero exit (bad SQL / renamed
        // column) panics with the failing query, so the doc cannot drift.
        let _ = sqlite3(&store, q);
    }
}

/// Extract the SQL statements from the cookbook's "Querying run state across DAGs"
/// section: the section's fenced ```` ```sql ```` block(s). Each fenced block holds
/// one or more `SELECT …;` statements; we split on `;` and keep the non-empty ones.
/// A comment-only or blank fragment is dropped.
fn extract_cookbook_sqlite_queries(md: &str) -> Vec<String> {
    // Bound the scan to the target section so an unrelated future ```sql``` block
    // elsewhere in the cookbook is never picked up.
    let start = md
        .find("## Querying run state across DAGs")
        .expect("the cookbook has the 'Querying run state across DAGs' section");
    let rest = &md[start..];
    // The section ends at the next H2 (or EOF).
    let end = rest[3..].find("\n## ").map(|i| i + 3).unwrap_or(rest.len());
    let section = &rest[..end];

    let mut queries = Vec::new();
    let mut in_sql = false;
    let mut buf = String::new();
    for line in section.lines() {
        let t = line.trim_start();
        if t.starts_with("```sql") {
            in_sql = true;
            buf.clear();
            continue;
        }
        if in_sql && t.starts_with("```") {
            in_sql = false;
            // Split the block into individual statements on `;`.
            for stmt in buf.split(';') {
                let cleaned = strip_sql_comments(stmt);
                if !cleaned.trim().is_empty() {
                    queries.push(format!("{};", cleaned.trim()));
                }
            }
            continue;
        }
        if in_sql {
            buf.push_str(line);
            buf.push('\n');
        }
    }
    queries
}

/// Drop `--`-to-end-of-line SQL comments from a statement fragment so a
/// comment-only fragment does not become an empty query, and inline commentary
/// does not confuse the `;` split. (The cookbook's queries are plain `SELECT`s;
/// this only needs to handle line comments.)
fn strip_sql_comments(stmt: &str) -> String {
    stmt.lines()
        .map(|l| match l.find("--") {
            Some(i) => &l[..i],
            None => l,
        })
        .collect::<Vec<_>>()
        .join("\n")
}
