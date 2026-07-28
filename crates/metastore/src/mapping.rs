//! The **event→row mapping** and the `sync` reconcile walk (T84).
//!
//! # What this module is
//!
//! A pure projection from a folded [`RunArtifact`] to run-index rows, plus a
//! run-store walk that folds every `events.jsonl` under a base and upserts it.
//! The event stream stays the source of truth (ADR 097, arch.md "The shape of a
//! run"); this is the derived, guaranteed projection of it.
//!
//! It reuses [`fold_stream`] as the folding authority (arch.md C22 / T42) — it
//! never re-implements folding — and depends **only** on `dagr-artifact` types
//! and the T83 store (`MetaStore`, `with_write_txn`). It has **no** dependency on
//! the driver or a live run: T86's live tee sink will reuse this same mapping, so
//! it stays pure — it reads finished/observed streams and writes rows, nothing
//! more.
//!
//! # Idempotence
//!
//! Every write is an **UPSERT** (`INSERT … ON CONFLICT … DO UPDATE`) keyed by
//! `run_id` (`dag_run`), `(run_id, node_id, try_number)` (`node_attempt`), and
//! `(run_id, node_id)` (`node_terminal`), and the fold is deterministic — so
//! re-syncing a run creates no duplicate rows and leaves counts unchanged. That
//! is what makes `sync` safe to run repeatedly and safe as a backfill/repair path
//! for runs that predate the metastore or came from another machine.
//!
//! # Timing (offsets, not wall clocks)
//!
//! The fold computes durations from **monotonic offsets only**, never from the
//! informational wall stamp (C19/C22), so an absolute unix-ms run-start instant is
//! not recoverable from the artifact. The projection therefore anchors
//! `dag_run.started_ms` at `0` (run start is offset 0) and records elapsed time in
//! milliseconds derived from those offsets; the authoritative per-attempt timing —
//! the named phase durations that sum bit-exactly to the attempt total — is carried
//! verbatim in `node_attempt.phase_durations_json`.

use std::path::{Path, PathBuf};

use dagr_artifact::event_stream::EVENTS_FILE_NAME;
use dagr_artifact::fold::{fold_stream, AttemptRecord, ProducedOutput, RunArtifact};
use serde_json::Value;

use crate::store::WriteError;
use crate::MetaStore;

/// The outcome of a [`sync_run_store`] pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SyncSummary {
    /// How many run directories were folded and upserted successfully.
    pub synced: u64,
    /// How many run directories were reported and skipped (no readable/foldable
    /// stream). A skipped run never aborts the batch.
    pub skipped: u64,
    /// How many of the synced runs were **newly** inserted (their `run_id` was not
    /// already in the index before this pass). The follow consolidator uses this to
    /// tell "a new run appeared" from "an already-synced run was re-folded".
    pub newly_synced: u64,
}

impl SyncSummary {
    /// Fold this pass's counts into a running total (used by `--follow` across
    /// passes).
    pub fn add(&mut self, other: SyncSummary) {
        self.synced += other.synced;
        self.skipped += other.skipped;
        self.newly_synced += other.newly_synced;
    }
}

/// Convert an offset in nanoseconds to whole milliseconds (the schema's unix-ms
/// integer affinity). Truncating is fine: the authoritative sub-ms timing lives in
/// `phase_durations_json`.
fn ns_to_ms(ns: u64) -> i64 {
    i64::try_from(ns / 1_000_000).unwrap_or(i64::MAX)
}

/// A JSON `Value` serialized to its string form for a `TEXT` blob column, or
/// `None` when the value is absent — so an absent field stores `NULL`, never the
/// string `"null"`.
fn json_text(v: Option<&Value>) -> Option<String> {
    v.map(std::string::ToString::to_string)
}

/// Escape a string for embedding in a single-quoted SQL literal (double the
/// quotes). The mapping builds `INSERT … ON CONFLICT` statements as text because
/// libSQL's positional-parameter binding across a heterogeneous batch is more
/// error-prone than one well-escaped statement per row, and every value here is
/// framework-derived (node names, states, JSON blobs), not free user SQL.
fn sql_quote(s: &str) -> String {
    s.replace('\'', "''")
}

/// Render an optional string as a SQL value: `'escaped'` or `NULL`.
fn sql_opt(s: Option<&str>) -> String {
    match s {
        Some(v) => format!("'{}'", sql_quote(v)),
        None => "NULL".to_string(),
    }
}

/// Project a folded [`RunArtifact`] into the run-index rows for one run and upsert
/// them under a single write transaction (`BEGIN IMMEDIATE`), idempotently.
///
/// Returns `true` when the run was **newly** inserted (its `run_id` was absent
/// before this call), `false` when it already existed (a re-sync). `events_path`
/// is recorded on `dag_run.events_path` so a row points back at the stream it was
/// folded from.
///
/// # Errors
/// Returns a [`WriteError`] if the write transaction fails (a non-busy libsql
/// error, or the bounded busy-retry budget is exhausted).
pub async fn sync_run(
    store: &MetaStore,
    artifact: &RunArtifact,
    events_path: Option<&str>,
) -> Result<bool, WriteError> {
    upsert_run(store, artifact, events_path, RunState::Terminal).await
}

/// The **live tee** projection (T86): project a folded artifact **while the run is
/// still executing**, overriding the `dag_run.state` to `running` until the run
/// finishes. This is the single seam the guaranteed live [`crate::live_sink::MetastoreSink`]
/// upserts through on each event, reusing the very same `build_statements`
/// projection and T83 [`MetaStore::with_write_txn`] write discipline that
/// [`sync_run`] uses — so a live run and a post-hoc `sync` of the same finished
/// stream converge on **byte-identical rows** (live == reconcile).
///
/// `still_running` is `true` while the stream has not yet carried its
/// `run-finished` record (the run is in flight, `dag_run.state='running'`,
/// `interrupted` still reflects the folded artifact); it becomes `false` at the
/// `run-finished` event, at which point this call produces exactly what
/// [`sync_run`] would (the folded terminal outcome). Every write is the same
/// idempotent UPSERT, so re-projecting the growing stream on each event never
/// duplicates a row and always converges on the reconcile projection.
///
/// # Errors
/// Returns a [`WriteError`] if the write transaction fails (a non-busy libsql
/// error, or the bounded busy-retry budget is exhausted).
pub async fn sync_run_live(
    store: &MetaStore,
    artifact: &RunArtifact,
    events_path: Option<&str>,
    still_running: bool,
) -> Result<bool, WriteError> {
    let state = if still_running {
        RunState::Running
    } else {
        RunState::Terminal
    };
    upsert_run(store, artifact, events_path, state).await
}

/// The `dag_run.state` a projection stamps: the folded terminal
/// [`overall_outcome`](RunArtifact::overall_outcome) (`sync` and a finished live
/// run), or the in-flight `running` sentinel (a live run before `run-finished`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RunState {
    /// Stamp `dag_run.state='running'` (the run is still executing).
    Running,
    /// Stamp the folded [`overall_outcome`](RunArtifact::overall_outcome).
    Terminal,
}

/// Shared upsert path for [`sync_run`] and [`sync_run_live`]: build the projection
/// (with the resolved run state) and run it in one `BEGIN IMMEDIATE` write txn.
async fn upsert_run(
    store: &MetaStore,
    artifact: &RunArtifact,
    events_path: Option<&str>,
    run_state: RunState,
) -> Result<bool, WriteError> {
    let run_id = artifact.header_run_id().to_string();
    let already = run_exists(store, &run_id).await?;

    // Build every statement up front (pure projection), then run them in one txn.
    let statements = build_statements(artifact, events_path, run_state);
    store
        .with_write_txn(move |conn| {
            let statements = statements.clone();
            Box::pin(async move {
                for stmt in &statements {
                    conn.execute(stmt, ()).await?;
                }
                Ok(())
            })
        })
        .await?;
    Ok(!already)
}

/// Whether a `dag_run` row for `run_id` already exists.
async fn run_exists(store: &MetaStore, run_id: &str) -> Result<bool, WriteError> {
    let mut rows = store
        .connection()
        .query(
            &format!(
                "SELECT 1 FROM dag_run WHERE run_id='{}' LIMIT 1",
                sql_quote(run_id)
            ),
            (),
        )
        .await
        .map_err(WriteError::Libsql)?;
    let present = rows.next().await.map_err(WriteError::Libsql)?.is_some();
    Ok(present)
}

/// Build the full ordered statement list for one run: the `dag`/`dag_version`
/// parents, the `dag_run` row, one `node_attempt` per [`AttemptRecord`], and the
/// latest-per-node `node_terminal`. Every statement is an idempotent UPSERT.
fn build_statements(
    artifact: &RunArtifact,
    events_path: Option<&str>,
    run_state: RunState,
) -> Vec<String> {
    let run_id = artifact.header_run_id().to_string();
    let pipeline = artifact.header_pipeline().to_string();
    // The DAG identity is its pipeline (stable) name; a distinct structural
    // fingerprint is a distinct version (arch.md C21). Absent fingerprint (a
    // pre-execution failure) ⇒ no dag_version row.
    let dag_id = pipeline.clone();
    let fingerprint = artifact.header_fingerprint_structural().map(String::from);
    let dag_version_id = fingerprint.as_ref().map(|fp| format!("{dag_id}@{fp}"));

    let mut out = Vec::new();

    // --- dag (identity by stable name) ------------------------------------
    out.push(format!(
        "INSERT INTO dag (dag_id, name, created_ms) VALUES ('{}', '{}', 0) \
         ON CONFLICT(dag_id) DO UPDATE SET name=excluded.name",
        sql_quote(&dag_id),
        sql_quote(&pipeline),
    ));

    // --- dag_version (one per distinct structural fingerprint) ------------
    if let (Some(dvid), Some(fp)) = (&dag_version_id, &fingerprint) {
        let policy = artifact.header_fingerprint_policy();
        let meta = serde_json::json!({ "fingerprint_policy": policy }).to_string();
        out.push(format!(
            "INSERT INTO dag_version (dag_version_id, dag_id, structural_fingerprint, created_ms, meta_json) \
             VALUES ('{}', '{}', '{}', 0, '{}') \
             ON CONFLICT(dag_version_id) DO UPDATE SET meta_json=excluded.meta_json",
            sql_quote(dvid),
            sql_quote(&dag_id),
            sql_quote(fp),
            sql_quote(&meta),
        ));
    }

    // --- dag_run ----------------------------------------------------------
    // The run state is the folded terminal outcome for `sync` and a finished live
    // run; a live run still in flight overrides it to the `running` sentinel (the
    // sixth `dag_run.state` value the schema's CHECK admits). The rest of the row
    // (interrupted, timing, attempts) is the identical projection either way, so a
    // finished live run and a post-hoc `sync` converge on byte-identical rows.
    let state = match run_state {
        RunState::Running => "running",
        RunState::Terminal => artifact.overall_outcome(),
    };
    let interrupted = i32::from(artifact.is_interrupted());
    let resumed_from = artifact.header_resumed_from();
    let params_json = Value::Object(artifact.header_parameters().clone()).to_string();
    let env_json = artifact.header_captured_environment().to_string();
    let finished_ms = artifact.summary_total_elapsed_ns();
    // A run has a summary only when it executed; a pre-execution failure carries
    // none, so finished_ms is 0 there.
    let finished_sql = if artifact.summary().is_some() {
        ns_to_ms(finished_ms).to_string()
    } else {
        "NULL".to_string()
    };
    out.push(format!(
        "INSERT INTO dag_run \
         (run_id, dag_id, dag_version_id, state, started_ms, finished_ms, interrupted, \
          resumed_from, events_path, params_json, env_json) \
         VALUES ('{run_id}', '{dag}', {dvid}, '{state}', 0, {finished}, {interrupted}, \
          {resumed}, {events}, '{params}', '{env}') \
         ON CONFLICT(run_id) DO UPDATE SET \
          dag_id=excluded.dag_id, dag_version_id=excluded.dag_version_id, state=excluded.state, \
          started_ms=excluded.started_ms, finished_ms=excluded.finished_ms, \
          interrupted=excluded.interrupted, resumed_from=excluded.resumed_from, \
          events_path=excluded.events_path, params_json=excluded.params_json, \
          env_json=excluded.env_json",
        run_id = sql_quote(&run_id),
        dag = sql_quote(&dag_id),
        dvid = dag_version_id
            .as_deref()
            .map_or("NULL".to_string(), |v| format!("'{}'", sql_quote(v))),
        state = sql_quote(state),
        finished = finished_sql,
        interrupted = interrupted,
        resumed = sql_opt(resumed_from),
        events = sql_opt(events_path),
        params = sql_quote(&params_json),
        env = sql_quote(&env_json),
    ));

    // --- node_attempt (one row per attempt) + node_terminal (latest) ------
    // The fold emits attempts in stream (seq) order, so the last attempt-record a
    // node has is its latest — used for node_terminal.
    for a in artifact.attempts() {
        out.push(attempt_upsert(&run_id, a));
    }
    for stmt in terminal_upserts(&run_id, artifact.attempts()) {
        out.push(stmt);
    }

    // --- Lineage projection (T91): produced outputs, consumed inputs, asset ---
    // Append-only, FK-free produced-output lineage from the folded `outputs[]`, and
    // the consumed durable `inputs[]` an attempt read. The optional `asset` identity
    // row is populated (by VALUE on the `uri`) on first sight of every produced /
    // consumed uri — never a hard FK, so an absent/deleted asset row never orphans
    // lineage. Every statement is an idempotent UPSERT (keyed by the row's natural
    // lineage identity), so re-projecting a growing live stream, or re-syncing a
    // finished one, never duplicates a lineage row (live == reconcile).
    for o in artifact.outputs() {
        out.push(output_produced_upsert(&run_id, o));
        out.push(asset_upsert(o.uri()));
    }
    for a in artifact.attempts() {
        for input in a.inputs() {
            out.push(input_consumed_upsert(
                &run_id,
                a.node(),
                a.attempt_number(),
                input.uri(),
                input.content_hash(),
            ));
            out.push(asset_upsert(input.uri()));
        }
    }

    out
}

/// The UPSERT for one produced-output lineage row. Append-only and FK-free: the
/// `uri`/hash are carried by value with no foreign key to the `asset` table. The
/// conflict target is the row's natural lineage identity
/// `(run_id, node_id, attempt, uri, content_hash)`, so re-projecting the same run
/// (live or a re-sync) never duplicates it.
fn output_produced_upsert(run_id: &str, o: &ProducedOutput) -> String {
    format!(
        "INSERT INTO output_produced \
         (run_id, node_id, attempt, uri, content_hash, size_bytes, kind, produced_at_offset_ns, \
          originating_run) \
         VALUES ('{run_id}', '{node}', {attempt}, '{uri}', {content_hash}, {size_bytes}, {kind}, \
          {produced_at}, {originating}) \
         ON CONFLICT(run_id, node_id, attempt, uri, content_hash) DO UPDATE SET \
          size_bytes=excluded.size_bytes, kind=excluded.kind, \
          produced_at_offset_ns=excluded.produced_at_offset_ns, \
          originating_run=excluded.originating_run",
        run_id = sql_quote(run_id),
        node = sql_quote(o.node()),
        attempt = o.attempt_number(),
        uri = sql_quote(o.uri()),
        content_hash = sql_opt(o.content_hash()),
        size_bytes = sql_u64(o.size_bytes()),
        kind = sql_opt(o.kind()),
        produced_at = o.produced_at_offset_ns(),
        originating = sql_opt(Some(o.originating_run())),
    )
}

/// The UPSERT for one consumed durable-input lineage row. FK-free: the `uri`/hash
/// are carried by value. The conflict target is the natural lineage identity
/// `(run_id, node_id, attempt, uri, content_hash)`.
fn input_consumed_upsert(
    run_id: &str,
    node: &str,
    attempt: u32,
    uri: &str,
    content_hash: Option<&str>,
) -> String {
    format!(
        "INSERT INTO input_consumed (run_id, node_id, attempt, uri, content_hash) \
         VALUES ('{run_id}', '{node}', {attempt}, '{uri}', {content_hash}) \
         ON CONFLICT(run_id, node_id, attempt, uri, content_hash) DO NOTHING",
        run_id = sql_quote(run_id),
        node = sql_quote(node),
        attempt = attempt,
        uri = sql_quote(uri),
        content_hash = sql_opt(content_hash),
    )
}

/// The UPSERT that populates the OPTIONAL `asset` identity row on first sight of a
/// `uri`. Referenced BY VALUE only (the `uri` string) — never a hard FK — so an
/// absent/deleted `asset` row never orphans lineage. Idempotent: a repeat sighting
/// of the same uri is a no-op (`DO NOTHING`), preserving any `extra` written later.
fn asset_upsert(uri: &str) -> String {
    format!(
        "INSERT INTO asset (uri, extra) VALUES ('{uri}', NULL) \
         ON CONFLICT(uri) DO NOTHING",
        uri = sql_quote(uri),
    )
}

/// The UPSERT for one attempt row.
fn attempt_upsert(run_id: &str, a: &AttemptRecord) -> String {
    let phases = serde_json::to_value(a.phase_durations_ns())
        .unwrap_or(Value::Null)
        .to_string();
    let metrics = a.metrics().to_string();
    let total_ms = ns_to_ms(a.total_elapsed_ns());
    // The OPTIONAL durable-reference metadata (T89/T91), projected as first-class
    // node_attempt columns for querying. Absent fields (a non-producing attempt, or
    // a producing output that supplied no metadata) store NULL.
    let meta = a.durable_reference_meta();
    let content_hash = meta_str(meta, "content_hash");
    let scheme = meta_str(meta, "scheme");
    let size_bytes = meta_u64(meta, "size_bytes");
    let produced_at = meta_u64(meta, "produced_at_offset_ns");
    format!(
        "INSERT INTO node_attempt \
         (run_id, node_id, try_number, state, started_ms, finished_ms, message, metrics_json, \
          worker, phase_durations_json, error_json, cost_declared_json, cost_measured_json, \
          durable_reference, satisfied_from_run, originating_node, \
          content_hash, size_bytes, scheme, produced_at_offset_ns) \
         VALUES ('{run_id}', '{node}', {try_number}, '{state}', NULL, {finished_ms}, {message}, '{metrics}', \
          '{worker}', '{phases}', {error}, {cost_declared}, {cost_measured}, \
          {durable}, {satisfied}, {originating}, \
          {content_hash}, {size_bytes}, {scheme}, {produced_at}) \
         ON CONFLICT(run_id, node_id, try_number) DO UPDATE SET \
          state=excluded.state, finished_ms=excluded.finished_ms, message=excluded.message, \
          metrics_json=excluded.metrics_json, worker=excluded.worker, \
          phase_durations_json=excluded.phase_durations_json, error_json=excluded.error_json, \
          cost_declared_json=excluded.cost_declared_json, cost_measured_json=excluded.cost_measured_json, \
          durable_reference=excluded.durable_reference, satisfied_from_run=excluded.satisfied_from_run, \
          originating_node=excluded.originating_node, \
          content_hash=excluded.content_hash, size_bytes=excluded.size_bytes, \
          scheme=excluded.scheme, produced_at_offset_ns=excluded.produced_at_offset_ns",
        run_id = sql_quote(run_id),
        node = sql_quote(a.node()),
        try_number = a.attempt_number(),
        state = sql_quote(a.status()),
        finished_ms = total_ms,
        message = sql_opt(a.message()),
        metrics = sql_quote(&metrics),
        worker = sql_quote(a.worker()),
        phases = sql_quote(&phases),
        error = sql_opt(json_text(a.error()).as_deref()),
        cost_declared = sql_opt(json_text(a.cost_declared()).as_deref()),
        cost_measured = sql_opt(json_text(a.cost_measured()).as_deref()),
        durable = sql_opt(json_text(a.durable_reference()).as_deref()),
        satisfied = sql_opt(a.satisfied_from_run()),
        originating = sql_opt(a.originating_node()),
        content_hash = sql_opt(content_hash.as_deref()),
        size_bytes = sql_u64(size_bytes),
        scheme = sql_opt(scheme.as_deref()),
        produced_at = sql_u64(produced_at),
    )
}

/// Read a string field out of the optional `durable_reference_meta` JSON object.
fn meta_str(meta: Option<&Value>, key: &str) -> Option<String> {
    meta.and_then(|m| m.get(key))
        .and_then(Value::as_str)
        .map(String::from)
}

/// Read a `u64` field out of the optional `durable_reference_meta` JSON object.
fn meta_u64(meta: Option<&Value>, key: &str) -> Option<u64> {
    meta.and_then(|m| m.get(key)).and_then(Value::as_u64)
}

/// Render an optional `u64` as a SQL integer literal or `NULL`.
fn sql_u64(v: Option<u64>) -> String {
    v.map_or_else(|| "NULL".to_string(), |n| n.to_string())
}

/// The `node_terminal` upserts — one per node, carrying that node's **latest**
/// terminal state. The attempts arrive in stream order, so the last record for a
/// node is its latest; a later record's state overwrites an earlier one via the
/// UPSERT keyed on `(run_id, node_id)`.
fn terminal_upserts(run_id: &str, attempts: &[AttemptRecord]) -> Vec<String> {
    // Preserve the first-seen node order for deterministic statement output; the
    // UPSERT overwrites with the latest state as later attempts of the same node
    // are applied.
    let mut order: Vec<&str> = Vec::new();
    for a in attempts {
        if !order.iter().any(|n| *n == a.node()) {
            order.push(a.node());
        }
    }
    // For each node, the latest (last-in-stream) attempt's status and elapsed.
    order
        .into_iter()
        .map(|node| {
            let latest = attempts
                .iter()
                .rev()
                .find(|a| a.node() == node)
                .expect("node came from the attempts list");
            format!(
                "INSERT INTO node_terminal (run_id, node_id, state, finished_ms) \
                 VALUES ('{run_id}', '{node}', '{state}', {finished}) \
                 ON CONFLICT(run_id, node_id) DO UPDATE SET \
                  state=excluded.state, finished_ms=excluded.finished_ms",
                run_id = sql_quote(run_id),
                node = sql_quote(node),
                state = sql_quote(latest.status()),
                finished = ns_to_ms(latest.total_elapsed_ns()),
            )
        })
        .collect()
}

/// Walk a run store rooted at `base` (`<base>/<pipeline>/<run-id>/events.jsonl`,
/// the T0.6 layout), fold each stream via [`fold_stream`], and upsert it. A run
/// directory with no readable/foldable stream is **reported and skipped** — never
/// fatal — so one bad run never aborts the batch.
///
/// Returns a [`SyncSummary`] of the pass (synced / skipped / newly-synced counts).
///
/// # Errors
/// Returns a [`WriteError`] only for a genuine store-write failure. A per-run fold
/// or read failure is a *skip*, not an error (batch robustness).
pub async fn sync_run_store(store: &MetaStore, base: &Path) -> Result<SyncSummary, WriteError> {
    let mut summary = SyncSummary::default();
    for events_path in discover_run_streams(base) {
        match std::fs::read(&events_path) {
            Ok(bytes) => match fold_stream(&bytes, &[]) {
                Ok(artifact) => {
                    let path_str = events_path.to_string_lossy().into_owned();
                    let newly = sync_run(store, &artifact, Some(&path_str)).await?;
                    summary.synced += 1;
                    if newly {
                        summary.newly_synced += 1;
                    }
                }
                // An unfoldable stream (no run-started, or a non-tolerable
                // corruption) is skipped and reported, not fatal.
                Err(_) => summary.skipped += 1,
            },
            // An unreadable/absent stream is skipped and reported.
            Err(_) => summary.skipped += 1,
        }
    }
    Ok(summary)
}

/// Discover every `<base>/<pipeline>/<run-id>/events.jsonl` under `base`, in a
/// deterministic (sorted) order. A directory missing its `events.jsonl` is a
/// run-directory candidate the caller will skip (it is still returned so the skip
/// is reported); a non-existent base yields an empty list.
fn discover_run_streams(base: &Path) -> Vec<PathBuf> {
    let mut streams = Vec::new();
    let Ok(pipelines) = sorted_dir(base) else {
        return streams;
    };
    for pipeline in pipelines {
        if !pipeline.is_dir() {
            continue;
        }
        let Ok(runs) = sorted_dir(&pipeline) else {
            continue;
        };
        for run in runs {
            if !run.is_dir() {
                continue;
            }
            streams.push(run.join(EVENTS_FILE_NAME));
        }
    }
    streams
}

/// The entries of `dir`, sorted by path for deterministic traversal.
fn sorted_dir(dir: &Path) -> std::io::Result<Vec<PathBuf>> {
    let mut entries: Vec<PathBuf> = std::fs::read_dir(dir)?
        .filter_map(Result::ok)
        .map(|e| e.path())
        .collect();
    entries.sort();
    Ok(entries)
}
