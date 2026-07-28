//! **T87 docs-truthfulness guards** — always compiled (no `libsql`, no feature), so
//! a plain `cargo test --workspace` reds if the metastore docs rot or overclaim.
//!
//! T87 is a `feature (docs)` ticket: it adds **no capability**, and its claims must
//! not exceed shipped behavior. ADR 097 fixed **native access only** — the query
//! path is plain `sqlite3` against a byte-compatible libSQL file, and there is **no**
//! Postgres wire protocol, **no** server/remote, and **no** lineage/asset tables
//! (that is M8). These checks assert the cookbook + README teach the shipped shape
//! and do not promise the unshipped one.
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
    let start = md.find(heading).unwrap_or_else(|| {
        panic!("the cookbook has a '{heading}' section")
    });
    let rest = &md[start..];
    let end = rest[3..].find("\n## ").map(|i| i + 3).unwrap_or(rest.len());
    rest[..end].to_string()
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
    // The tables it actually queries are the shipped M7 tables.
    for table in ["dag", "dag_run", "node_attempt", "node_terminal"] {
        assert!(
            s.contains(table),
            "the section queries the shipped `{table}` table"
        );
    }
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
        s.contains("same-host") || (s.contains("same host") ) || s.contains("local filesystem")
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

/// A claims check over the metastore docs: **nothing** references unshipped behavior.
/// No Postgres wire protocol, no server/remote/`sqld` setup, and no lineage/asset
/// tables (all M8 or permanently rejected). A doc that starts promising any of these
/// reds this test.
#[test]
fn the_docs_claim_nothing_unshipped() {
    // The metastore text lives in these two docs; scan both.
    let cookbook = cookbook_metastore_section();
    let readme = read_doc("README.md");

    // Forbidden substrings, each a thing dagr deliberately does NOT ship in M7.
    // (Case-insensitive; matched against a lowercased haystack.)
    let forbidden: &[(&str, &str)] = &[
        ("pgwire", "no Postgres wire protocol (ADR 097)"),
        // Lineage / asset tables are M8 (T89–T91), not this ticket.
        ("lineage table", "lineage tables are M8, not shipped"),
        ("asset table", "asset tables are M8, not shipped"),
        ("produced_asset", "asset tables are M8, not shipped"),
    ];

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
    // rather than the bare word being absent.
    let cs = cookbook.to_lowercase();
    if cs.contains("server") {
        assert!(
            cs.contains("no server") || cs.contains("not a server") || cs.contains("without a server"),
            "if the cookbook mentions a server it must be to say there is none (embedded local access only)"
        );
    }
}
