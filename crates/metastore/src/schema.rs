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

    let mut m = vec![
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
        // A run is recorded from the moment it starts (`state='running'`), through
        // its terminal outcome. `interrupted` distinguishes a crash-truncation from
        // a deliberate cancellation (both read `state='cancelled'`; C22/fold);
        // `resumed_from` carries resume lineage; `events_path` points back at the
        // source stream this row was folded from; params/env are JSON blobs. The
        // T84-added columns are ALSO applied idempotently to a pre-existing T83
        // store via [`ADDITIVE_COLUMNS`], so both shapes converge (additive-only).
        format!(
            "CREATE TABLE IF NOT EXISTS dag_run (
            run_id          TEXT PRIMARY KEY,
            dag_id          TEXT NOT NULL,
            dag_version_id  TEXT,
            state           TEXT NOT NULL {run_state_check},
            started_ms      INTEGER NOT NULL,
            finished_ms     INTEGER,
            interrupted     INTEGER NOT NULL DEFAULT 0,
            resumed_from    TEXT,
            events_path     TEXT,
            params_json     TEXT,
            env_json        TEXT,
            meta_json       TEXT
        )"
        ),
        // --- node_attempt: one row per attempt, UNIQUE(run_id,node_id,try) ----
        // The run artifact carries one record per *attempt* (C22): retries are the
        // interesting capacity-planning signal. `state` is CHECK-constrained to the
        // nine canonical terminal states. The rich fold fields (worker, phase
        // durations, error/cost JSON, durable reference, resume/propagation
        // lineage) are carried too — see [`ADDITIVE_COLUMNS`] for the pre-existing-
        // store convergence path.
        // The T91-added durable_reference_meta columns (content_hash / size_bytes /
        // scheme / produced_at_offset_ns) are ALSO applied idempotently to a
        // pre-existing T83/M7 store via [`ADDITIVE_COLUMNS`], so both shapes
        // converge (additive-only). They project the OPTIONAL per-attempt
        // durable-reference metadata (T89) as first-class columns for querying.
        format!(
            "CREATE TABLE IF NOT EXISTS node_attempt (
            run_id             TEXT NOT NULL,
            node_id            TEXT NOT NULL,
            try_number         INTEGER NOT NULL,
            state              TEXT NOT NULL {node_state_check},
            started_ms         INTEGER,
            finished_ms        INTEGER,
            message            TEXT,
            metrics_json       TEXT,
            worker             TEXT,
            phase_durations_json TEXT,
            error_json         TEXT,
            cost_declared_json TEXT,
            cost_measured_json TEXT,
            durable_reference  TEXT,
            satisfied_from_run TEXT,
            originating_node   TEXT,
            content_hash       TEXT,
            size_bytes         INTEGER,
            scheme             TEXT,
            produced_at_offset_ns INTEGER,
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
    ];
    // The T91 lineage tables + their cross-run indexes are appended as forward,
    // additive migrations (kept in their own function so `migrations` stays within
    // the pedantic line budget and the M8 addition is auditable in one place).
    m.extend(lineage_migrations());
    // The T111 submission/audit tables, likewise forward and additive.
    m.extend(submission_migrations());
    m
}

/// The M8 lineage migrations (T91): the append-only `output_produced` /
/// `input_consumed` tables, the optional by-value `asset` identity endpoint, and
/// the cross-run `uri` / `content_hash` / `run_id` indexes. All idempotent
/// (`CREATE … IF NOT EXISTS`), so a pre-T91 (M7) store gains them in place and a
/// re-open is a no-op. NONE of the lineage tables carries a foreign key to `asset`:
/// the `uri` join is **by value**, so a lineage row outlives garbage-collection (or
/// deletion) of the referent — the append-only, survives-GC discipline ADR 097's
/// lineage note fixes.
fn lineage_migrations() -> Vec<String> {
    vec![
        // --- output_produced: append-only produced-output lineage -------------
        // One row per `output-produced` fold entry (arch.md C22 / T90 outputs[]).
        // Every field is carried BY VALUE; there is deliberately NO foreign key to
        // `asset`. `originating_run` is this run on a fresh produce, or the prior
        // run when a resume copied the output forward (satisfied-from-prior).
        "CREATE TABLE IF NOT EXISTS output_produced (
            id                    INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id                TEXT NOT NULL,
            node_id               TEXT NOT NULL,
            attempt               INTEGER NOT NULL,
            uri                   TEXT NOT NULL,
            content_hash          TEXT,
            size_bytes            INTEGER,
            kind                  TEXT,
            produced_at_offset_ns INTEGER,
            originating_run       TEXT,
            UNIQUE (run_id, node_id, attempt, uri, content_hash)
        )"
        .to_string(),
        // --- input_consumed: consumed durable inputs -------------------------
        // One row per consumed durable input an attempt read (T90 inputs[]). The
        // `uri` references a dataset BY VALUE, with NO foreign key to `asset`.
        "CREATE TABLE IF NOT EXISTS input_consumed (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id       TEXT NOT NULL,
            node_id      TEXT NOT NULL,
            attempt      INTEGER NOT NULL,
            uri          TEXT NOT NULL,
            content_hash TEXT,
            UNIQUE (run_id, node_id, attempt, uri, content_hash)
        )"
        .to_string(),
        // --- asset: the OPTIONAL identity endpoint ---------------------------
        // A single joinable row per distinct `uri`, populated on first sight of a
        // uri in `output_produced`/`input_consumed`. Referenced BY VALUE (the `uri`
        // string) — NEVER a hard FK — so deleting an `asset` row never orphans
        // lineage. Identity-only: `extra` is a free JSON blob for future additive
        // fields; the produced/consumed rows already answer "what did this run
        // produce/consume", so this table is a convenience join target.
        "CREATE TABLE IF NOT EXISTS asset (
            uri   TEXT PRIMARY KEY,
            extra TEXT
        )"
        .to_string(),
        // --- Lineage cross-run indexes: uri / content_hash / run_id ----------
        // Support the "which runs produced/consumed dataset X" cross-run queries
        // (arch.md C22): join by `uri` (or `content_hash` for an exact content
        // match), filter by `run_id`.
        "CREATE INDEX IF NOT EXISTS idx_output_produced_uri ON output_produced (uri)".to_string(),
        "CREATE INDEX IF NOT EXISTS idx_output_produced_content_hash ON output_produced (content_hash)"
            .to_string(),
        "CREATE INDEX IF NOT EXISTS idx_output_produced_run_id ON output_produced (run_id)"
            .to_string(),
        "CREATE INDEX IF NOT EXISTS idx_input_consumed_uri ON input_consumed (uri)".to_string(),
        "CREATE INDEX IF NOT EXISTS idx_input_consumed_content_hash ON input_consumed (content_hash)"
            .to_string(),
        "CREATE INDEX IF NOT EXISTS idx_input_consumed_run_id ON input_consumed (run_id)"
            .to_string(),
    ]
}

/// The M10 submission/audit migrations (T111): the `attempt_submitted` row per
/// remote attempt the orchestrator launched, and the `attempt_submitted_input`
/// child carrying its **ordered, positional** references. Both idempotent
/// (`CREATE … IF NOT EXISTS`), so a pre-T111 store gains them in place and a
/// re-open is a no-op.
///
/// Two shape decisions are load-bearing.
///
/// **No foreign keys — not to `attempt_submitted`, not to `node_attempt`, not to
/// `asset`.** An audit row's whole purpose is to answer "what was this launched
/// with?" *after* the thing it names is gone: the remote work object is garbage
/// collected by the platform, the blob is collected by `prune`, and the attempt
/// row may never have existed at all (a submission with no outcome). Every
/// reference is by value, exactly as the lineage tables do it.
///
/// **Submitted-but-never-completed is a first-class state, not an absence.**
/// `completed` is a 0/1 flag and `outcome_state` is `NULL` until an
/// `attempt-outcome` exists, so `WHERE completed = 0` is the direct query for what
/// a crashed orchestrator left behind. `outcome_state` is `CHECK`-constrained to
/// the same **nine** canonical terminal states as everywhere else — the taxonomy is
/// closed and gains no tenth member for "submitted"; that fact lives in `completed`.
/// `input_count` is likewise `NULL` for unknown and `0` for a consume-nothing
/// source, because conflating them would make an arity mismatch undetectable.
fn submission_migrations() -> Vec<String> {
    let quoted: Vec<String> = NODE_TERMINAL_STATES
        .iter()
        .map(|s| format!("'{s}'"))
        .collect();
    let outcome_check = format!(
        "CHECK (outcome_state IS NULL OR outcome_state IN ({}))",
        quoted.join(", ")
    );
    vec![
        // --- attempt_submitted: one row per submitted remote attempt ----------
        // Keyed on the executor's own idempotency triple (run_id, node_id, attempt),
        // so a re-projection of the same submission updates rather than duplicates.
        // Intent (`target_name`) and reality (`observed_*`) are separate columns
        // because they diverge and a post-mortem needs both.
        format!(
            "CREATE TABLE IF NOT EXISTS attempt_submitted (
            run_id                 TEXT NOT NULL,
            node_id                TEXT NOT NULL,
            attempt                INTEGER NOT NULL,
            executor               TEXT,
            target_name            TEXT,
            observed_name          TEXT,
            observed_uid           TEXT,
            observed_host          TEXT,
            structural_fingerprint TEXT,
            policy_hash            TEXT,
            tool_version           TEXT,
            image_digest           TEXT,
            input_count            INTEGER,
            submitted_at_offset_ns INTEGER,
            completed              INTEGER NOT NULL DEFAULT 0,
            outcome_state          TEXT {outcome_check},
            PRIMARY KEY (run_id, node_id, attempt)
        )"
        ),
        // --- attempt_submitted_input: the ordered references, one row each -----
        // `position` is the declared positional index dagr binds by, and it is part
        // of the key: order is load-bearing, so a row set that loses it loses the
        // audit's meaning. Normalized rather than encoded in one column because the
        // audit queries filter on individual references (the divergence join).
        "CREATE TABLE IF NOT EXISTS attempt_submitted_input (
            run_id       TEXT NOT NULL,
            node_id      TEXT NOT NULL,
            attempt      INTEGER NOT NULL,
            position     INTEGER NOT NULL,
            uri          TEXT NOT NULL,
            content_hash TEXT,
            PRIMARY KEY (run_id, node_id, attempt, position)
        )"
        .to_string(),
        // --- Audit indexes ----------------------------------------------------
        // Filter by run, find the stranded submissions, and join a reference back
        // to the lineage tables by value (uri / content_hash).
        "CREATE INDEX IF NOT EXISTS idx_attempt_submitted_run_id ON attempt_submitted (run_id)"
            .to_string(),
        "CREATE INDEX IF NOT EXISTS idx_attempt_submitted_completed ON attempt_submitted (completed)"
            .to_string(),
        "CREATE INDEX IF NOT EXISTS idx_attempt_submitted_node_id ON attempt_submitted (node_id)"
            .to_string(),
        "CREATE INDEX IF NOT EXISTS idx_attempt_submitted_input_run_id ON attempt_submitted_input (run_id)"
            .to_string(),
        "CREATE INDEX IF NOT EXISTS idx_attempt_submitted_input_uri ON attempt_submitted_input (uri)"
            .to_string(),
        "CREATE INDEX IF NOT EXISTS idx_attempt_submitted_input_content_hash ON attempt_submitted_input (content_hash)"
            .to_string(),
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

/// The three M8 lineage table names T91 adds (append-only produced-output lineage,
/// consumed durable inputs, and the optional by-value asset identity endpoint).
/// Public so a test can assert every one exists via `sqlite_master`. These are
/// forward, additive migrations (`CREATE TABLE IF NOT EXISTS`): a pre-T91 (M7)
/// store gains them in place on the next open, and a store with no lineage data has
/// them empty.
pub const LINEAGE_TABLES: [&str; 3] = ["output_produced", "input_consumed", "asset"];

/// The two M10 submission/audit table names T111 adds: the per-attempt submission
/// row and its ordered-input child. Public so a test can assert every one exists
/// via `sqlite_master`. Forward, additive migrations (`CREATE TABLE IF NOT
/// EXISTS`): a pre-T111 store gains them in place on the next open, and a store
/// that has indexed only local runs has them empty.
pub const SUBMISSION_TABLES: [&str; 2] = ["attempt_submitted", "attempt_submitted_input"];

/// Additive `(table, column, type-affinity)` columns the T84 mapping projects
/// into that the T83 `CREATE TABLE` shapes did not name. Applied idempotently by
/// [`MetaStore::open`](crate::MetaStore::open) as `ALTER TABLE … ADD COLUMN` **only
/// when the column is absent** (`SQLite` has no `ADD COLUMN IF NOT EXISTS`), so a
/// fresh store (which gets them from the widened `CREATE TABLE` below) and a
/// pre-existing T83 store converge on the same shape without re-deciding it.
///
/// Evolution stays additive-only (arch.md C22 "schema evolution is bounded"): a
/// reader of an older row sees the new columns as `NULL`, and nothing existing is
/// dropped or retyped. The type affinity is `TEXT` for JSON blobs and identity
/// strings, `INTEGER` for the boolean-as-0/1 `interrupted` flag.
pub const ADDITIVE_COLUMNS: &[(&str, &str, &str)] = &[
    // dag_run — the run-level fields T84 carries beyond the T83 shape.
    ("dag_run", "interrupted", "INTEGER NOT NULL DEFAULT 0"),
    ("dag_run", "resumed_from", "TEXT"),
    ("dag_run", "events_path", "TEXT"),
    ("dag_run", "env_json", "TEXT"),
    // node_attempt — the rich per-attempt fold fields (arch.md C22 body).
    ("node_attempt", "worker", "TEXT"),
    ("node_attempt", "phase_durations_json", "TEXT"),
    ("node_attempt", "error_json", "TEXT"),
    ("node_attempt", "cost_declared_json", "TEXT"),
    ("node_attempt", "cost_measured_json", "TEXT"),
    ("node_attempt", "durable_reference", "TEXT"),
    ("node_attempt", "satisfied_from_run", "TEXT"),
    ("node_attempt", "originating_node", "TEXT"),
    // node_attempt — the T91 durable_reference_meta projection columns. The
    // lineage TABLES are additive via `CREATE TABLE IF NOT EXISTS`; only these new
    // COLUMNS on an existing `node_attempt` need an `ALTER … ADD COLUMN` on a
    // pre-T91 store (which reads them as NULL on old rows — additive-only).
    ("node_attempt", "content_hash", "TEXT"),
    ("node_attempt", "size_bytes", "INTEGER"),
    ("node_attempt", "scheme", "TEXT"),
    ("node_attempt", "produced_at_offset_ns", "INTEGER"),
];
