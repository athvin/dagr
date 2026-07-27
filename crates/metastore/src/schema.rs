//! The run-index schema as **ordered, idempotent** migrations.
//!
//! Every migration is `CREATE TABLE IF NOT EXISTS …` / `CREATE INDEX IF NOT
//! EXISTS …`, so applying [`migrations`] against a fresh **or** an
//! already-initialized store is a no-op the second time (arch.md/ADR 097: the
//! index is a *guaranteed, reproducible* projection). The M7 tables are `dag`,
//! `dag_version`, `dag_run`, `node_attempt`, `node_terminal`.
//!
//! # State enums are `CHECK`-constrained to the canonical vocabulary
//!
//! `node_attempt.state` is `TEXT` with a `CHECK` naming **exactly** dagr's
//! **nine** terminal states in their canonical kebab-case wire spelling (arch.md
//! "Vocabulary — terminal states"); [`NODE_TERMINAL_STATES`] is that list, and
//! the `CHECK` clause is generated from it so the two can never drift. A row with
//! a `state` outside the set is rejected by the database. `dag_run.state` is the
//! **six** run-level states ([`RUN_STATES`]): the five [`RunOutcome`] terminal
//! outcomes plus `running` (a run exists in the index from the moment it starts,
//! before it finishes).
//!
//! Timestamps are `INTEGER` unix-milliseconds; JSON blobs are `TEXT`.
//!
//! [`RunOutcome`]: dagr_artifact::event_stream::RunOutcome

/// dagr's **nine** canonical node terminal states, in their normative kebab-case
/// wire spelling (arch.md "Vocabulary"). This is the exact set the
/// `node_attempt.state` `CHECK` constraint accepts — the ordering here has no
/// meaning, only membership does.
pub const NODE_TERMINAL_STATES: [&str; 9] = [
    "succeeded",
    "failed",
    "timed-out",
    "skipped",
    "upstream-skipped",
    "upstream-failed",
    "cancelled",
    "abandoned",
    "satisfied-from-prior",
];

/// The **six** run-level states the `dag_run.state` `CHECK` accepts: the five
/// [`RunOutcome`](dagr_artifact::event_stream::RunOutcome) terminal outcomes
/// (`succeeded`, `failed`, `cancelled`, `assembly-failed`, `bootstrap-failed`)
/// plus `running` — a run is recorded in the index the moment it starts, before
/// any outcome exists.
pub const RUN_STATES: [&str; 6] = [
    "running",
    "succeeded",
    "failed",
    "cancelled",
    "assembly-failed",
    "bootstrap-failed",
];

/// Render an SQL `CHECK (col IN ('a','b',…))` clause body from a state list, so
/// the `CHECK` constraint is generated from the canonical Rust constant and the
/// two provably never drift.
fn state_check(column: &str, states: &[&str]) -> String {
    let quoted: Vec<String> = states.iter().map(|s| format!("'{s}'")).collect();
    format!("CHECK ({column} IN ({}))", quoted.join(", "))
}

/// The ordered migration set. Each entry is one idempotent DDL statement, applied
/// in order on every [`MetaStore::open`](crate::MetaStore::open). Later tickets
/// (T84/T86) append rows; they never re-decide this shape.
///
/// The vector is built at call time because the `node_attempt` / `dag_run`
/// `CHECK` clauses are generated from [`NODE_TERMINAL_STATES`] / [`RUN_STATES`].
#[must_use]
pub fn migrations() -> Vec<String> {
    let node_state_check = state_check("state", &NODE_TERMINAL_STATES);
    let run_state_check = state_check("state", &RUN_STATES);

    vec![
        // --- dag: one row per logical DAG (identity by stable name) -----------
        "CREATE TABLE IF NOT EXISTS dag (
            dag_id       TEXT PRIMARY KEY,
            name         TEXT NOT NULL,
            created_ms   INTEGER NOT NULL
        )"
        .to_string(),
        // --- dag_version: a DAG's structural fingerprint over time ------------
        // A DAG's shape changes across builds (C21); each distinct structural
        // fingerprint is a version. `meta_json` carries the policy hash and any
        // future additive fields (readers ignore unknown keys).
        "CREATE TABLE IF NOT EXISTS dag_version (
            dag_version_id         TEXT PRIMARY KEY,
            dag_id                 TEXT NOT NULL,
            structural_fingerprint TEXT NOT NULL,
            created_ms             INTEGER NOT NULL,
            meta_json              TEXT
        )"
        .to_string(),
        // --- dag_run: one row per run (run_id PK) -----------------------------
        format!(
            "CREATE TABLE IF NOT EXISTS dag_run (
            run_id          TEXT PRIMARY KEY,
            dag_id          TEXT NOT NULL,
            dag_version_id  TEXT,
            state           TEXT NOT NULL {run_state_check},
            started_ms      INTEGER NOT NULL,
            finished_ms     INTEGER,
            params_json     TEXT,
            meta_json       TEXT
        )"
        ),
        // --- node_attempt: one row per attempt, UNIQUE(run_id,node_id,try) ----
        // The run artifact carries one record per *attempt* (C22): retries are the
        // interesting capacity-planning signal. `state` is CHECK-constrained to the
        // nine canonical terminal states.
        format!(
            "CREATE TABLE IF NOT EXISTS node_attempt (
            run_id       TEXT NOT NULL,
            node_id      TEXT NOT NULL,
            try_number   INTEGER NOT NULL,
            state        TEXT NOT NULL {node_state_check},
            started_ms   INTEGER,
            finished_ms  INTEGER,
            message      TEXT,
            metrics_json TEXT,
            UNIQUE (run_id, node_id, try_number)
        )"
        ),
        // --- node_terminal: the single terminal state per node per run --------
        // Every node ends in exactly one terminal state, exactly once (arch.md
        // "Vocabulary"); this is that one row, distinct from the per-attempt log.
        format!(
            "CREATE TABLE IF NOT EXISTS node_terminal (
            run_id       TEXT NOT NULL,
            node_id      TEXT NOT NULL,
            state        TEXT NOT NULL {node_state_check},
            finished_ms  INTEGER,
            PRIMARY KEY (run_id, node_id)
        )"
        ),
        // --- Indexes on dag_id, run_id, and state (per the plan) --------------
        "CREATE INDEX IF NOT EXISTS idx_dag_version_dag_id ON dag_version (dag_id)".to_string(),
        "CREATE INDEX IF NOT EXISTS idx_dag_run_dag_id ON dag_run (dag_id)".to_string(),
        "CREATE INDEX IF NOT EXISTS idx_dag_run_state ON dag_run (state)".to_string(),
        "CREATE INDEX IF NOT EXISTS idx_node_attempt_run_id ON node_attempt (run_id)".to_string(),
        "CREATE INDEX IF NOT EXISTS idx_node_attempt_state ON node_attempt (state)".to_string(),
        "CREATE INDEX IF NOT EXISTS idx_node_terminal_run_id ON node_terminal (run_id)".to_string(),
        "CREATE INDEX IF NOT EXISTS idx_node_terminal_state ON node_terminal (state)".to_string(),
    ]
}

/// The five M7 table names, in creation order. Public so a test (and the `init`
/// verb) can assert every one exists via `sqlite_master`.
pub const TABLES: [&str; 5] = [
    "dag",
    "dag_version",
    "dag_run",
    "node_attempt",
    "node_terminal",
];
