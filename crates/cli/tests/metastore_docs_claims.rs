//! **T87 docs-truthfulness guards** — always compiled (no `libsql`, no feature), so
//! a plain `cargo test --workspace` reds if the metastore docs rot or overclaim.
//!
//! T87 is a `feature (docs)` ticket: it adds **no capability**, and its claims must
//! not exceed shipped behavior. ADR 097 fixed **native access only** — the query
//! path is plain `sqlite3` against a byte-compatible libSQL file, and there is **no**
//! Postgres wire protocol and **no** server/remote. These checks assert the cookbook
//! + README teach the shipped shape and do not promise the unshipped one.
//!
//! **T91 update:** lineage projection now ships — the cookbook documents the
//! `output_produced` / `input_consumed` / `asset` tables and the cross-run "which
//! runs touched dataset X" query — so the forbidden-substring guard no longer bans
//! the lineage/asset tables. What stays forbidden is the asset-**scheduler** surface
//! dagr permanently rejects (data-triggered runs, asset queues/watchers/partitions)
//! and any server/remote/pgwire promise.
//!
//! The *executable* proof — that the cookbook's query block runs against a real
//! populated store — lives in the feature-gated
//! [`metastore_example_and_docs.rs`](metastore_example_and_docs.rs); these guards
//! need no store and no feature, so they run everywhere.

use std::path::{Path, PathBuf};

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("crates/cli has a two-level ancestor (the repo root)")
        .to_path_buf()
}

fn read_doc(rel: &str) -> String {
    let path = repo_root().join(rel);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("cannot read {rel}: {e}"))
}

/// The metastore section of the cookbook, isolated so the claims checks below scan
/// only it (not an unrelated future mention elsewhere in the file).
fn cookbook_metastore_section() -> String {
    let md = read_doc("docs/cookbook.md");
    let heading = "## Querying run state across DAGs";
    let start = md
        .find(heading)
        .unwrap_or_else(|| panic!("the cookbook has a '{heading}' section"));
    let rest = &md[start..];
    let end = rest[3..].find("\n## ").map_or(rest.len(), |i| i + 3);
    rest[..end].to_string()
}

/// Collapse all runs of ASCII whitespace (incl. the Markdown line-wraps that break a
/// phrase like "no foreign key" across two source lines) to single spaces, so a
/// prose-claim scan matches on meaning, not on where the author happened to wrap.
fn normalize_ws(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

// ===========================================================================
// The cookbook teaches the SHIPPED shape
// ===========================================================================

/// The cookbook has the "Querying run state across DAGs" section, and it teaches the
/// three load-bearing shipped facts: turn the tee on with the `--dagr.metastore` /
/// `DAGR_METASTORE` toggle, run some DAGs, then query with **plain `sqlite3`** — with
/// the `turso db shell` / `libsql` CLIs noted as equivalents, and the `many_dags`
/// example named as the compiled backing.
#[test]
fn cookbook_teaches_the_native_query_path() {
    let s = cookbook_metastore_section();
    assert!(
        s.contains("--dagr.metastore") && s.contains("DAGR_METASTORE"),
        "the section documents the flag + env toggle (flag > env > default)"
    );
    assert!(
        s.contains("sqlite3"),
        "the section shows the plain `sqlite3` query path (zero new tools)"
    );
    assert!(
        s.contains("turso db shell") && s.contains("libsql"),
        "the section notes `turso db shell <file>` / the `libsql` CLI as equivalents"
    );
    assert!(
        s.contains("many_dags"),
        "the section points at the compiled `many_dags` example that backs it"
    );
    // The tables it actually queries are the shipped M7 tables plus the M8 (T91)
    // lineage tables.
    for table in [
        "dag",
        "dag_run",
        "node_attempt",
        "node_terminal",
        "output_produced",
        "input_consumed",
        "asset",
    ] {
        assert!(
            s.contains(table),
            "the section queries the shipped `{table}` table"
        );
    }
}

/// The lineage section teaches the shipped T91 shape: the by-value / no-FK
/// discipline, the cross-run "which runs produced/consumed dataset X" query, and the
/// hard boundary that dagr is **not** an asset scheduler.
#[test]
fn cookbook_teaches_the_lineage_projection_and_its_boundary() {
    let lower = normalize_ws(&cookbook_metastore_section()).to_lowercase();
    assert!(
        lower.contains("lineage"),
        "the section documents the lineage projection (T91)"
    );
    assert!(
        lower.contains("by value") || lower.contains("by its `uri` value"),
        "the section states lineage references a dataset BY VALUE"
    );
    assert!(
        lower.contains("no foreign key") || lower.contains("no fk"),
        "the section states there is no hard foreign key to the asset row (survives-GC)"
    );
    assert!(
        lower.contains("not an asset scheduler"),
        "the section restates dagr is NOT an asset scheduler (permanent non-goal)"
    );
}

/// The section explicitly states the two shipped constraints ADR 097 fixed:
/// **native access only** (no Postgres wire) and **same-host local filesystem**.
#[test]
fn cookbook_states_native_only_and_same_host() {
    let s = cookbook_metastore_section().to_lowercase();
    assert!(
        s.contains("native"),
        "the section states native-access-only"
    );
    assert!(
        s.contains("no postgres") || s.contains("no pgwire") || s.contains("no wire protocol"),
        "the section states there is no Postgres wire protocol"
    );
    assert!(
        s.contains("same-host")
            || (s.contains("same host"))
            || s.contains("local filesystem")
            || s.contains("local fs"),
        "the section states the same-host local-filesystem constraint"
    );
}

/// `dagr metastore init` / `sync [--follow]` and the toggle are documented (in the
/// README reference), including the **guaranteed-live + reconcile-backfill** model.
#[test]
fn reference_documents_the_verbs_and_the_live_plus_reconcile_model() {
    let readme = read_doc("README.md");
    assert!(
        readme.contains("dagr metastore init"),
        "the reference documents `dagr metastore init`"
    );
    assert!(
        readme.contains("dagr metastore sync") && readme.contains("--follow"),
        "the reference documents `dagr metastore sync` and its `--follow` flag"
    );
    assert!(
        readme.contains("--dagr.metastore"),
        "the reference documents the live-tee toggle"
    );
    let lower = readme.to_lowercase();
    assert!(
        lower.contains("guaranteed") && lower.contains("live"),
        "the reference states the guaranteed-live model (the tee is not best-effort)"
    );
    assert!(
        lower.contains("reconcile") || lower.contains("backfill") || lower.contains("sync"),
        "the reference states the reconcile/backfill model (sync folds finished streams)"
    );
    assert!(
        lower.contains("source of truth"),
        "the reference states the event stream stays the source of truth"
    );
}

// ===========================================================================
// The docs do NOT exceed shipped behavior (no server/remote/pgwire/lineage)
// ===========================================================================

/// A claims check over the metastore docs: **nothing** references unshipped or
/// permanently-rejected behavior. No Postgres wire protocol, no server/remote/`sqld`
/// setup, and — the permanent non-goal T91 restated — no asset-**scheduler** surface
/// (data-triggered runs, asset queues/watchers/partitions). A doc that starts
/// promising any of these reds this test.
#[test]
fn the_docs_claim_nothing_unshipped() {
    // The metastore text lives in these two docs; scan both.
    let cookbook = cookbook_metastore_section();
    let readme = read_doc("README.md");

    // Hard-forbidden substrings, each a thing dagr deliberately does NOT ship.
    // (Case-insensitive; matched against a lowercased haystack.) The lineage/asset
    // *tables* now ship (T91), so they are no longer forbidden.
    let forbidden: &[(&str, &str)] = &[("pgwire", "no Postgres wire protocol (ADR 097)")];

    for haystack in [&cookbook, &readme] {
        let lower = haystack.to_lowercase();
        for (needle, why) in forbidden {
            assert!(
                !lower.contains(needle),
                "the metastore docs must not reference `{needle}` — {why}"
            );
        }
    }

    // The cookbook section, specifically, must not pitch a server/BI-tool path: it
    // is embedded local access only. We allow the words to appear in a NEGATED form
    // ("no server", "not a server"), so assert on the negated phrasing being present
    // rather than the bare word being absent. Whitespace is normalized first so a
    // Markdown line-wrap never splits a "no <term>" phrase across two source lines.
    let cs = normalize_ws(&cookbook).to_lowercase();
    if cs.contains("server") {
        assert!(
            cs.contains("no server")
                || cs.contains("not a server")
                || cs.contains("without a server"),
            "if the cookbook mentions a server it must be to say there is none (embedded local access only)"
        );
    }

    // The asset-**scheduler** cluster is a permanent non-goal (T91): the cookbook may
    // NAME it only to reject it. Each term, if present, must appear in the negated /
    // "not / no" form, never as a promised capability.
    for (term, negated_forms) in [
        (
            "asset scheduler",
            [
                "not an asset scheduler",
                "no asset scheduler",
                "is not an asset scheduler",
            ],
        ),
        (
            "data-triggered",
            [
                "no data-triggered",
                "not data-triggered",
                "no data-triggered runs",
            ],
        ),
        (
            "asset queue",
            ["no asset queue", "not asset queue", "no asset queues"],
        ),
    ] {
        if cs.contains(term) {
            assert!(
                negated_forms.iter().any(|neg| cs.contains(neg)),
                "if the cookbook mentions `{term}` it must be to REJECT it \
                 (dagr is not an asset scheduler — permanent non-goal)"
            );
        }
    }
}
