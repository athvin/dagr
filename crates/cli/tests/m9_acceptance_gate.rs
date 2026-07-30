//! **M9 acceptance gate.** Written first, TDD.
//!
//! M9 touched the build profiles, the language edition, the lint policy, the
//! error surface, the docs, and CI. Each ticket proved its own change. What none
//! of them proved is the property the milestone exists to establish: that after
//! all of it **dagr still behaves identically**, and that the rule dispositions
//! are complete and honest.
//!
//! This suite asserts *structural* facts about the finished state and adds no
//! capability — the shape the M7 gate
//! (`crates/cli/tests/metastore_acceptance_gate.rs`) and the system gate
//! (`crates/cli/tests/system_acceptance_gate.rs`) already use. It covers five
//! groups:
//!
//! 1. **Register integrity.** The shipped verifier passes and its self-tests
//!    pass; independently of the verifier, every rule id under the skill's
//!    `rules/` directory is dispositioned exactly once and the row count equals
//!    the rule-file count.
//! 2. **Adopt traceability.** Every `adopt` row is traced to an item its named
//!    ticket actually shipped, tabulated in `docs/rust-skills-register.md` and
//!    machine-checked here: the table's rule set must equal the register's
//!    `adopt` set, the tickets must agree, and every row's evidence must be a
//!    real token in a real file.
//! 3. **Satisfied spot-checks.** Twelve sampled `satisfied` claims are
//!    re-derived from the tree by this suite — not read back from the register —
//!    and the register's spot-check table must document exactly the checks that
//!    run, so it cannot claim a check nobody performs.
//! 4. **Behavioural identity.** The reference pipeline's graph artifact, both
//!    fingerprints, a scripted run's terminal states, its folded run artifact,
//!    and every record of its event stream are byte-identical to a baseline
//!    captured from the **pre-M9 tree** by the same probe program.
//! 5. **Structural guarantees and the performance envelope.** No profile sets
//!    `panic = "abort"`; `render` and `metastore` have no manifest edge onto
//!    `core`; the resolved feature matrix still shows `dagr-core` alone under
//!    `--no-default-features`; the scale benchmark is within budget with the
//!    pre-M9 and post-M9 figures recorded.
//!
//! # Non-vacuity
//!
//! A gate that cannot fail is the defect a hardening milestone is most prone to,
//! so every scan here is paired with a negative control that plants the very
//! thing it looks for and asserts the scan reports it:
//! [`the_traceability_evidence_check_is_not_vacuous`],
//! [`the_snapshot_comparison_catches_a_one_byte_drift`],
//! [`the_panic_abort_scan_catches_a_planted_profile`], and
//! [`the_spot_check_runner_reports_a_broken_claim`]. The remaining assertions run
//! shipped verifiers whose own non-vacuity is proven by their self-test scripts.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::Command;

use dagr_cli::scale_bench::{CI_BUDGET_NS_PER_NODE, SPEC_CEILING_NS_PER_NODE, SCALE_NODE_COUNT};

// ===========================================================================
// Repository helpers
// ===========================================================================

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("crates/cli has a two-level ancestor (the repository root)")
        .to_path_buf()
}

fn read_repo_file(rel: &str) -> String {
    let path = repo_root().join(rel);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

/// The six workspace members, in manifest order.
const MEMBERS: [&str; 6] = ["core", "macros", "artifact", "render", "metastore", "cli"];

/// Every production `.rs` file — `crates/*/src/**/*.rs`. Tests, examples, and
/// fixtures are deliberately outside: the register's claims are about production
/// code.
fn production_sources() -> Vec<(String, String)> {
    fn walk(dir: &Path, root: &Path, out: &mut Vec<(String, String)>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        let mut paths: Vec<PathBuf> = entries.flatten().map(|e| e.path()).collect();
        paths.sort();
        for path in paths {
            if path.is_dir() {
                walk(&path, root, out);
            } else if path.extension().is_some_and(|e| e == "rs") {
                let text = std::fs::read_to_string(&path).expect("source is readable");
                let rel = path
                    .strip_prefix(root)
                    .unwrap_or(&path)
                    .to_string_lossy()
                    .into_owned();
                out.push((rel, text));
            }
        }
    }
    let root = repo_root();
    let mut out = Vec::new();
    for member in MEMBERS {
        walk(&root.join("crates").join(member).join("src"), &root, &mut out);
    }
    out
}

/// Count the lines across every production source matching any of `needles`.
fn production_line_hits(sources: &[(String, String)], needles: &[&str]) -> Vec<String> {
    let mut hits = Vec::new();
    for (rel, text) in sources {
        for (i, line) in text.lines().enumerate() {
            if needles.iter().any(|n| line.contains(n)) {
                hits.push(format!("{rel}:{}: {}", i + 1, line.trim()));
            }
        }
    }
    hits
}

/// Total occurrences of `needle` across every production source.
fn production_occurrences(sources: &[(String, String)], needle: &str) -> usize {
    sources
        .iter()
        .map(|(_, text)| text.matches(needle).count())
        .sum()
}

fn run_script(script: &str, args: &[&str]) -> (bool, String) {
    let root = repo_root();
    let out = Command::new("bash")
        .arg(root.join(script))
        .args(args)
        .current_dir(&root)
        .output()
        .unwrap_or_else(|e| panic!("running {script}: {e}"));
    let text = format!(
        "--- stdout ---\n{}\n--- stderr ---\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    (out.status.success(), text)
}

// ===========================================================================
// The register, parsed independently of the shipped verifier
// ===========================================================================

const REGISTER: &str = "docs/rust-skills-register.md";
const RULES_DIR: &str = ".claude/skills/rust-skills/rules";

/// One dispositioned rule row: `| rule | category | disposition | ticket | reason |`.
#[derive(Debug, Clone)]
struct RuleRow {
    rule: String,
    disposition: String,
    ticket: String,
    reason: String,
}

/// Split a markdown table line into its data cells, or `None` if it is not a
/// table row. A row `| a | b |` yields `["a", "b"]`.
fn table_cells(line: &str) -> Option<Vec<String>> {
    let trimmed = line.trim();
    if !trimmed.starts_with('|') || !trimmed.ends_with('|') || trimmed.len() < 2 {
        return None;
    }
    let inner = &trimmed[1..trimmed.len() - 1];
    Some(inner.split('|').map(|c| c.trim().to_owned()).collect())
}

/// Every rule row in the register — the five-column disposition tables only.
/// Rows whose first cell is a heading, a separator, or prose are skipped, the
/// same discrimination `scripts/check-rust-skills-adoption.sh` makes.
fn rule_rows(register: &str) -> Vec<RuleRow> {
    let mut rows = Vec::new();
    for line in register.lines() {
        let Some(cells) = table_cells(line) else {
            continue;
        };
        if cells.len() != 5 {
            continue;
        }
        let rule = cells[0].clone();
        if rule.is_empty()
            || rule == "Rule"
            || rule.chars().all(|c| c == '-')
            || rule.contains(char::is_whitespace)
        {
            continue;
        }
        rows.push(RuleRow {
            rule,
            disposition: cells[2].clone(),
            ticket: cells[3].clone(),
            reason: cells[4].clone(),
        });
    }
    rows
}

/// Every four-column row under the `### <heading>` section, up to the next
/// heading of the same or a higher level. Header and separator rows are dropped.
fn gate_table(register: &str, heading: &str, columns: usize) -> Vec<Vec<String>> {
    let mut rows = Vec::new();
    let mut inside = false;
    for line in register.lines() {
        let trimmed = line.trim_end();
        if trimmed.starts_with("### ") || trimmed.starts_with("## ") {
            inside = trimmed == heading;
            continue;
        }
        if !inside {
            continue;
        }
        let Some(cells) = table_cells(trimmed) else {
            continue;
        };
        if cells.len() != columns {
            continue;
        }
        if cells[0].chars().all(|c| c == '-' || c == ':') || cells[0].is_empty() {
            continue;
        }
        if cells[0] == "Rule" || cells[0] == "Measurement" {
            continue;
        }
        rows.push(cells);
    }
    rows
}

/// The rule ids the skill actually ships, derived from the rules directory —
/// never a number written down.
fn rule_ids_on_disk() -> BTreeSet<String> {
    let dir = repo_root().join(RULES_DIR);
    let entries = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("read {}: {e} — the rules directory is the authority", dir.display()));
    entries
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|x| x == "md"))
        .filter_map(|p| p.file_stem().map(|s| s.to_string_lossy().into_owned()))
        .collect()
}

// ===========================================================================
// (1) REGISTER INTEGRITY
// ===========================================================================

/// **The shipped verifier passes, and its own self-tests pass.** The register is
/// a checked-in data file; the verifier is what stops it rotting. Running the
/// self-test script as well is what makes the verifier's verdict worth anything
/// — it plants each defect the verifier claims to catch and requires the
/// verifier to catch it.
#[test]
fn the_register_verifier_and_its_self_tests_pass() {
    let (ok, out) = run_script("scripts/check-rust-skills-adoption.sh", &[]);
    assert!(ok, "the rust-skills adoption register verifier passes\n{out}");
    assert!(
        out.contains("REGISTER=PASS"),
        "the verifier printed its PASS verdict\n{out}"
    );

    let (ok, out) = run_script("scripts/check-rust-skills-verifier-selftest.sh", &[]);
    assert!(
        ok,
        "the register verifier's own self-tests pass — without them its PASS \
         proves nothing\n{out}"
    );
}

/// **Every rule id is dispositioned exactly once, and the count matches.**
/// Derived here from the rules directory and the register text directly, so this
/// is an *independent* confirmation rather than a second reading of the
/// verifier's verdict.
#[test]
fn every_rule_id_is_dispositioned_exactly_once_against_the_rules_directory() {
    let register = read_repo_file(REGISTER);
    let rows = rule_rows(&register);
    let on_disk = rule_ids_on_disk();

    assert!(
        on_disk.len() > 200,
        "the rules directory yielded only {} ids — the scan is not looking where \
         it should be",
        on_disk.len()
    );

    let mut seen: BTreeMap<&str, usize> = BTreeMap::new();
    for row in &rows {
        *seen.entry(row.rule.as_str()).or_default() += 1;
    }
    let duplicated: Vec<&&str> = seen.iter().filter(|(_, n)| **n > 1).map(|(r, _)| r).collect();
    assert!(
        duplicated.is_empty(),
        "these rule ids are dispositioned more than once: {duplicated:?}"
    );

    let dispositioned: BTreeSet<String> = seen.keys().map(|s| (*s).to_owned()).collect();
    let absent: Vec<&String> = on_disk.difference(&dispositioned).collect();
    assert!(absent.is_empty(), "these rule ids are not dispositioned: {absent:?}");
    let dangling: Vec<&String> = dispositioned.difference(&on_disk).collect();
    assert!(
        dangling.is_empty(),
        "these register rows name a rule with no file in {RULES_DIR}: {dangling:?}"
    );

    assert_eq!(
        rows.len(),
        on_disk.len(),
        "the register carries exactly one row per rule file"
    );

    for row in &rows {
        assert!(
            matches!(
                row.disposition.as_str(),
                "satisfied" | "adopt" | "n-a" | "declined"
            ),
            "`{}` has an unrecognised disposition `{}`",
            row.rule,
            row.disposition
        );
        assert!(
            !matches!(row.reason.as_str(), "" | "—" | "-" | "n/a" | "TBD"),
            "`{}` ({}) carries no reason",
            row.rule,
            row.disposition
        );
    }
}

// ===========================================================================
// (2) ADOPT TRACEABILITY
// ===========================================================================

const TRACEABILITY_HEADING: &str = "### Every `adopt` row, traced to the item its ticket shipped";

/// An evidence cell reads `` `path` :: `token` ``. Returns the path and the
/// literal token that must appear in it.
fn parse_evidence(cell: &str) -> Option<(String, String)> {
    let (path, token) = cell.split_once(" :: ")?;
    let strip = |s: &str| s.trim().trim_matches('`').to_owned();
    Some((strip(path), strip(token)))
}

/// Does `token` appear literally in the repository file at `path`?
fn evidence_present(path: &str, token: &str) -> Result<bool, String> {
    let full = repo_root().join(path);
    let text = std::fs::read_to_string(&full).map_err(|e| format!("{path}: {e}"))?;
    Ok(text.contains(token))
}

/// **Every `adopt` row is traced to a shipped ticket item.** The register's
/// traceability table must cover exactly the register's `adopt` rows, agree with
/// them on the ticket, and every row's evidence must be a token that really is in
/// the file named — so reverting the item its ticket shipped fails this gate.
#[test]
fn every_adopt_row_is_traced_to_a_shipped_ticket_item() {
    let register = read_repo_file(REGISTER);
    let adopt: BTreeMap<String, String> = rule_rows(&register)
        .into_iter()
        .filter(|r| r.disposition == "adopt")
        .map(|r| (r.rule, r.ticket))
        .collect();
    assert!(
        adopt.len() > 30,
        "only {} adopt rows found — the parse is wrong",
        adopt.len()
    );

    let table = gate_table(&register, TRACEABILITY_HEADING, 4);
    assert!(
        !table.is_empty(),
        "the register carries no traceability table under `{TRACEABILITY_HEADING}`"
    );

    let traced: BTreeMap<String, Vec<String>> = table
        .iter()
        .map(|cells| (cells[0].trim_matches('`').to_owned(), cells.clone()))
        .collect();

    let missing: Vec<&String> = adopt
        .keys()
        .filter(|r| !traced.contains_key(*r))
        .collect();
    assert!(
        missing.is_empty(),
        "these `adopt` rows are not traced to a shipped item: {missing:?}"
    );
    let extra: Vec<&String> = traced.keys().filter(|r| !adopt.contains_key(*r)).collect();
    assert!(
        extra.is_empty(),
        "the traceability table traces rules that are not dispositioned `adopt`: {extra:?}"
    );

    let mut broken: Vec<String> = Vec::new();
    for (rule, cells) in &traced {
        let ticket = cells[1].trim_matches('`');
        let expected = &adopt[rule];
        assert_eq!(
            ticket, expected,
            "`{rule}` is traced to {ticket} but dispositioned to {expected}"
        );
        let ticket_files: Vec<PathBuf> = std::fs::read_dir(repo_root().join("docs/implementation"))
            .expect("the implementation ticket directory is readable")
            .flatten()
            .map(|e| e.path())
            .filter(|p| {
                p.file_name()
                    .is_some_and(|n| n.to_string_lossy().contains(&format!("-{ticket}-")))
            })
            .collect();
        assert!(
            !ticket_files.is_empty(),
            "`{rule}` names ticket {ticket}, which has no file under docs/implementation/"
        );

        let Some((path, token)) = parse_evidence(&cells[3]) else {
            broken.push(format!("{rule}: evidence cell is not `path` :: `token`: {}", cells[3]));
            continue;
        };
        match evidence_present(&path, &token) {
            Ok(true) => {}
            Ok(false) => broken.push(format!("{rule}: `{token}` is absent from {path}")),
            Err(e) => broken.push(format!("{rule}: {e}")),
        }
    }
    assert!(
        broken.is_empty(),
        "these `adopt` rows claim an item their ticket did not ship (or no longer \
         ships):\n{}",
        broken.join("\n")
    );
}

/// **Non-vacuity: the evidence check really reads the file.** A token that is
/// deliberately absent must be reported absent, and a token that is present must
/// be reported present — otherwise the traceability table above would pass with
/// any contents at all.
#[test]
fn the_traceability_evidence_check_is_not_vacuous() {
    assert_eq!(
        evidence_present("Cargo.toml", "[workspace]"),
        Ok(true),
        "a token that is present is reported present"
    );
    assert_eq!(
        evidence_present("Cargo.toml", "[workspace.this-table-does-not-exist]"),
        Ok(false),
        "a token that is absent is reported absent — the check is a real read"
    );
    assert!(
        evidence_present("crates/does-not-exist.rs", "anything").is_err(),
        "a missing evidence file is an error, never a silent pass"
    );
}

// ===========================================================================
// (3) SATISFIED SPOT-CHECKS
// ===========================================================================

const SPOT_CHECK_HEADING: &str = "### The sampled `satisfied` rows, re-derived by the gate";

/// The sampled `satisfied` rules this suite re-derives from the tree. The
/// register's spot-check table must name exactly these.
const SPOT_CHECKS: [&str; 12] = [
    "api-must-use",
    "api-serde-optional",
    "coll-map-choice",
    "conc-thread-local",
    "doc-module-inner",
    "err-custom-type",
    "lint-workspace-lints",
    "name-crate-no-rs",
    "own-slice-over-vec",
    "proj-flat-small",
    "test-integration-dir",
    "unsafe-safety-comment",
];

/// Re-derive one sampled `satisfied` claim from the tree. `Ok(evidence)` when the
/// claim holds; `Err(reason)` when it does not.
fn spot_check(rule: &str, sources: &[(String, String)]) -> Result<String, String> {
    let root = repo_root();
    match rule {
        "own-slice-over-vec" => {
            let hits = production_line_hits(sources, &[": &Vec<", ": &String", ": &Box<"]);
            if hits.is_empty() {
                Ok("zero `&Vec<T>` / `&String` / `&Box<T>` parameters in production".into())
            } else {
                Err(format!("{} borrowed-container parameter(s): {hits:?}", hits.len()))
            }
        }
        "conc-thread-local" => {
            let hits = production_line_hits(sources, &["static mut "]);
            if !hits.is_empty() {
                return Err(format!("{} `static mut` site(s): {hits:?}", hits.len()));
            }
            let locals = production_occurrences(sources, "thread_local!");
            if locals == 0 {
                return Err("no `thread_local!` remains — the attribution path changed".into());
            }
            Ok(format!(
                "{locals} `thread_local!` declarations and zero `static mut` in production"
            ))
        }
        "unsafe-safety-comment" => {
            let mut uncommented = Vec::new();
            let mut blocks = 0usize;
            for (rel, text) in sources {
                let lines: Vec<&str> = text.lines().collect();
                for (i, line) in lines.iter().enumerate() {
                    let code = line.trim_start();
                    // Prose that *mentions* an unsafe block is not one.
                    if code.starts_with("//") || code.starts_with('*') {
                        continue;
                    }
                    let is_block = code.contains("unsafe {") || code.starts_with("unsafe impl");
                    if !is_block {
                        continue;
                    }
                    blocks += 1;
                    // The justification sits in the run of comments and attributes
                    // introducing the block.
                    let commented = lines[i.saturating_sub(8)..i]
                        .iter()
                        .any(|l| l.contains("// SAFETY:"));
                    if !commented {
                        uncommented.push(format!("{rel}:{}", i + 1));
                    }
                }
            }
            if blocks == 0 {
                return Err("no `unsafe` block found at all — the scan is misdirected".into());
            }
            if uncommented.is_empty() {
                Ok(format!(
                    "all {blocks} production `unsafe` blocks/impls carry a `// SAFETY:` \
                     comment"
                ))
            } else {
                Err(format!("`unsafe` without `// SAFETY:`: {uncommented:?}"))
            }
        }
        "coll-map-choice" => {
            let hash_set = production_line_hits(sources, &["HashSet"]);
            let btree = production_occurrences(sources, "BTreeMap")
                + production_occurrences(sources, "BTreeSet");
            let hash_map = production_occurrences(sources, "HashMap");
            if !hash_set.is_empty() {
                return Err(format!("`HashSet` appears in production: {hash_set:?}"));
            }
            if btree < 200 || hash_map > 12 {
                return Err(format!(
                    "the ordered-map preference has shifted: {btree} BTree mentions \
                     against {hash_map} HashMap mentions"
                ));
            }
            Ok(format!(
                "{btree} `BTreeMap`/`BTreeSet` mentions against {hash_map} `HashMap` \
                 and zero `HashSet`"
            ))
        }
        "api-must-use" => {
            let n = production_occurrences(sources, "#[must_use]");
            if n >= 600 {
                Ok(format!("{n} `#[must_use]` attributes in production"))
            } else {
                Err(format!("only {n} `#[must_use]` attributes"))
            }
        }
        "api-serde-optional" => {
            let manifest = read_repo_file("crates/core/Cargo.toml");
            let offending: Vec<&str> = manifest
                .lines()
                .filter(|l| {
                    let l = l.trim();
                    !l.starts_with('#') && l.contains("serde")
                })
                .collect();
            if offending.is_empty() {
                Ok("`dagr-core`'s manifest names no `serde` dependency at all".into())
            } else {
                Err(format!("`dagr-core` names serde: {offending:?}"))
            }
        }
        "doc-module-inner" => {
            let mut missing = Vec::new();
            for (rel, text) in sources {
                let opens = text
                    .lines()
                    .take(3)
                    .any(|l| l.starts_with("//!") || l.starts_with("#![doc"));
                if !opens {
                    missing.push(rel.clone());
                }
            }
            if missing.is_empty() {
                Ok(format!(
                    "all {} production files open with a module doc",
                    sources.len()
                ))
            } else {
                Err(format!("{} file(s) without a `//!` header: {missing:?}", missing.len()))
            }
        }
        "proj-flat-small" => {
            let mut nested = Vec::new();
            for member in MEMBERS {
                let src = root.join("crates").join(member).join("src");
                let Ok(entries) = std::fs::read_dir(&src) else {
                    continue;
                };
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.is_dir() && path.file_name().is_some_and(|n| n != "bin") {
                        nested.push(path.to_string_lossy().into_owned());
                    }
                }
            }
            if nested.is_empty() {
                Ok("every crate is flat `foo.rs` files in `src/`, with only `src/bin/`".into())
            } else {
                Err(format!("nested module directories: {nested:?}"))
            }
        }
        "lint-workspace-lints" => {
            let workspace = read_repo_file("Cargo.toml");
            if !workspace.contains("[workspace.lints.rust]") {
                return Err("the root manifest carries no `[workspace.lints.rust]`".into());
            }
            let mut missing = Vec::new();
            for member in MEMBERS {
                let manifest = read_repo_file(&format!("crates/{member}/Cargo.toml"));
                if !manifest.contains("[lints]") || !manifest.contains("workspace = true") {
                    missing.push(member);
                }
            }
            if missing.is_empty() {
                Ok("`[workspace.lints]` plus `[lints] workspace = true` in all six members".into())
            } else {
                Err(format!("members not opting in: {missing:?}"))
            }
        }
        "name-crate-no-rs" => {
            let mut names = Vec::new();
            for member in MEMBERS {
                let manifest = read_repo_file(&format!("crates/{member}/Cargo.toml"));
                let name = manifest
                    .lines()
                    .find_map(|l| l.trim().strip_prefix("name = "))
                    .map(|n| n.trim().trim_matches('"').to_owned())
                    .ok_or_else(|| format!("crates/{member}/Cargo.toml has no package name"))?;
                if !name.starts_with("dagr-") || name.ends_with("-rs") || name.ends_with("-rust") {
                    return Err(format!("`{name}` breaks the naming convention"));
                }
                names.push(name);
            }
            Ok(format!("the six crates are {}", names.join(", ")))
        }
        "err-custom-type" => {
            let n = production_line_hits(sources, &["impl std::error::Error for", "impl Error for"])
                .len();
            if n >= 25 {
                Ok(format!("{n} production `impl … Error for` blocks"))
            } else {
                Err(format!("only {n} domain error types implement `Error`"))
            }
        }
        "test-integration-dir" => {
            let mut total = 0usize;
            let mut missing = Vec::new();
            for member in MEMBERS {
                let dir = root.join("crates").join(member).join("tests");
                let count = std::fs::read_dir(&dir)
                    .map(|e| {
                        e.flatten()
                            .filter(|x| x.path().extension().is_some_and(|s| s == "rs"))
                            .count()
                    })
                    .unwrap_or(0);
                if count == 0 {
                    missing.push(member);
                }
                total += count;
            }
            if !missing.is_empty() {
                return Err(format!("crates with no integration tests: {missing:?}"));
            }
            if total < 100 {
                return Err(format!("only {total} integration test files"));
            }
            Ok(format!("{total} integration test files across all six crates"))
        }
        other => Err(format!("no spot check is implemented for `{other}`")),
    }
}

/// **The sampled `satisfied` claims reproduce independently.** Twelve rows across
/// twelve categories, each re-derived from the tree here rather than read back
/// from the register. A `satisfied` claim that nobody can reproduce is worse than
/// an empty register, because it stops the next person from looking.
#[test]
fn the_sampled_satisfied_claims_reproduce_independently() {
    let register = read_repo_file(REGISTER);
    let satisfied: BTreeSet<String> = rule_rows(&register)
        .into_iter()
        .filter(|r| r.disposition == "satisfied")
        .map(|r| r.rule)
        .collect();

    let sources = production_sources();
    assert!(
        sources.len() > 40,
        "only {} production sources found — the scan is not looking where it should be",
        sources.len()
    );

    let mut failures = Vec::new();
    for rule in SPOT_CHECKS {
        assert!(
            satisfied.contains(rule),
            "`{rule}` is sampled as a `satisfied` claim but the register does not \
             disposition it `satisfied`"
        );
        match spot_check(rule, &sources) {
            Ok(evidence) => println!("spot check {rule}: {evidence}"),
            Err(reason) => failures.push(format!("{rule}: {reason}")),
        }
    }
    assert!(
        failures.is_empty(),
        "these sampled `satisfied` claims do not reproduce:\n{}",
        failures.join("\n")
    );
}

/// **The register documents exactly the spot checks the gate runs.** Without
/// this, the register's spot-check table could claim a verification nobody
/// performs — the same failure mode the register exists to prevent, one level up.
#[test]
fn the_spot_check_table_documents_exactly_the_checks_the_gate_runs() {
    let register = read_repo_file(REGISTER);
    let table = gate_table(&register, SPOT_CHECK_HEADING, 4);
    assert!(
        !table.is_empty(),
        "the register carries no spot-check table under `{SPOT_CHECK_HEADING}`"
    );
    let documented: BTreeSet<String> = table
        .iter()
        .map(|cells| cells[0].trim_matches('`').to_owned())
        .collect();
    let implemented: BTreeSet<String> = SPOT_CHECKS.iter().map(|s| (*s).to_owned()).collect();
    assert_eq!(
        documented, implemented,
        "the register's spot-check table and the checks this gate actually runs \
         must be the same set"
    );
}

/// **Non-vacuity: the spot-check runner reports a broken claim.** A rule with no
/// implementation is an error rather than a silent pass, and a claim inverted by
/// a planted violation is reported.
#[test]
fn the_spot_check_runner_reports_a_broken_claim() {
    let sources = production_sources();
    assert!(
        spot_check("no-such-rule", &sources).is_err(),
        "an unimplemented spot check is an error, never a silent pass"
    );

    // Plant the very thing `own-slice-over-vec` scans for, in a synthetic source
    // set, and require the scan to report it.
    let planted = vec![(
        "crates/core/src/planted.rs".to_owned(),
        "fn takes(v: &Vec<u8>) {}\n".to_owned(),
    )];
    let verdict = spot_check("own-slice-over-vec", &planted);
    assert!(
        verdict.is_err(),
        "a planted `&Vec<T>` parameter must fail the scan, not pass it: {verdict:?}"
    );

    let planted = vec![(
        "crates/core/src/planted.rs".to_owned(),
        "static mut COUNTER: u64 = 0;\n".to_owned(),
    )];
    assert!(
        spot_check("conc-thread-local", &planted).is_err(),
        "a planted `static mut` must fail the scan"
    );

    let planted = vec![(
        "crates/core/src/planted.rs".to_owned(),
        "fn f() {\n    unsafe { g() };\n}\n".to_owned(),
    )];
    assert!(
        spot_check("unsafe-safety-comment", &planted).is_err(),
        "an `unsafe` block with no `// SAFETY:` must fail the scan"
    );
    let commented = vec![(
        "crates/core/src/planted.rs".to_owned(),
        "fn f() {\n    // SAFETY: the planted control's justification.\n    unsafe { g() };\n}\n"
            .to_owned(),
    )];
    assert!(
        spot_check("unsafe-safety-comment", &commented).is_ok(),
        "a justified `unsafe` block passes — the scan reads the comment, it does \
         not simply reject all unsafe"
    );
}

// ===========================================================================
// (4) BEHAVIOURAL IDENTITY
// ===========================================================================

const BASELINE_SNAPSHOT: &str = "crates/cli/tests/fixtures/m9-baseline/reference.snapshot.txt";
const BASELINE_PROVENANCE: &str = "crates/cli/tests/fixtures/m9-baseline/PROVENANCE.md";
const PROBE: &str = "crates/cli/examples/m9_baseline_capture.rs";

/// FNV-1a 64, the same digest family the fingerprints use — enough to prove the
/// probe program is byte-for-byte the one the baseline was captured with, with no
/// new dependency.
fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for b in bytes {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

/// The first differing line between two snapshots, as
/// `(line number, baseline, current)`, or `None` when they are identical.
fn first_line_diff(baseline: &str, current: &str) -> Option<(usize, String, String)> {
    let a: Vec<&str> = baseline.lines().collect();
    let b: Vec<&str> = current.lines().collect();
    for i in 0..a.len().max(b.len()) {
        let left = a.get(i).copied().unwrap_or("<absent>");
        let right = b.get(i).copied().unwrap_or("<absent>");
        if left != right {
            return Some((i + 1, left.to_owned(), right.to_owned()));
        }
    }
    None
}

/// Run the checked-in probe on the current head and return its stdout.
fn current_snapshot() -> String {
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_owned());
    let mut args = vec![
        "run",
        "--quiet",
        "-p",
        "dagr-cli",
        "--example",
        "m9_baseline_capture",
    ];
    // Keep the nested resolution equal to the outer one: `metastore` is
    // default-off, so it is on here only under `--all-features`.
    if cfg!(feature = "metastore") {
        args.push("--all-features");
    }
    let out = Command::new(cargo)
        .args(&args)
        .current_dir(repo_root())
        .output()
        .expect("the behavioural probe runs");
    assert!(
        out.status.success(),
        "the behavioural probe failed:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).expect("the probe prints UTF-8")
}

/// **The reference behaviour is byte-identical to the pre-M9 baseline.** The
/// graph artifact, both fingerprints, the scripted run's terminal states, the
/// folded run artifact, and every event-stream record are compared against a
/// baseline produced by running **this same probe** against the pre-M9 tree.
/// M9 was hardening; anything that changed here is a defect, and this is the last
/// chance to catch it.
#[test]
fn the_reference_behaviour_snapshot_is_byte_identical_to_the_pre_m9_baseline() {
    let baseline = read_repo_file(BASELINE_SNAPSHOT);
    assert!(
        baseline.lines().count() > 20,
        "the baseline fixture is too small to be the real snapshot"
    );
    let current = current_snapshot();

    if let Some((line, want, got)) = first_line_diff(&baseline, &current) {
        panic!(
            "the M9 head's observable behaviour diverges from the pre-M9 baseline at \
             line {line}.\n  pre-M9 : {want}\n  M9 head: {got}\n\nM9 changed no \
             behaviour by design; investigate the divergence rather than re-basing \
             the fixture."
        );
    }

    // The snapshot really carries all four surfaces (a truncated fixture must not
    // pass by being a prefix of nothing).
    for marker in [
        "graph-artifact=",
        "fingerprint-structural=",
        "fingerprint-policy=",
        "overall-outcome=",
        "run-artifact=",
        "event=",
    ] {
        assert!(
            baseline.contains(marker),
            "the baseline snapshot carries no `{marker}` line"
        );
    }
}

/// **The baseline records the pre-M9 commit and the probe it was captured with.**
/// A baseline whose provenance is unrecorded is a fixture someone can quietly
/// re-bless. The digest is the load-bearing half: it proves the checked-in probe
/// is byte-for-byte the program that produced the baseline, so the comparison
/// above is one engine against another rather than one program against another.
#[test]
fn the_baseline_fixture_records_the_pre_m9_commit_and_the_probe_it_was_captured_with() {
    let provenance = read_repo_file(BASELINE_PROVENANCE);

    let commit = provenance
        .lines()
        .find_map(|l| l.trim().strip_prefix("pre-m9-commit = "))
        .map(|s| s.trim().trim_matches('`').to_owned())
        .expect("PROVENANCE.md records `pre-m9-commit = <sha>`");
    assert_eq!(commit.len(), 40, "the recorded commit is a full 40-hex sha");
    assert!(
        commit.chars().all(|c| c.is_ascii_hexdigit()),
        "the recorded commit is hexadecimal"
    );

    let recorded = provenance
        .lines()
        .find_map(|l| l.trim().strip_prefix("probe-fnv1a64 = "))
        .map(|s| s.trim().trim_matches('`').to_owned())
        .expect("PROVENANCE.md records `probe-fnv1a64 = <hex>`");
    let actual = format!("{:016x}", fnv1a64(read_repo_file(PROBE).as_bytes()));
    assert_eq!(
        recorded, actual,
        "the checked-in probe ({PROBE}) is not the program the baseline was \
         captured with — re-capture the baseline from {commit} with the current \
         probe, or restore the probe"
    );
}

/// **Non-vacuity: the snapshot comparison catches a one-byte drift.** The
/// comparison must be a real line-by-line comparison that names where the
/// divergence is, not a no-op that would accept anything.
#[test]
fn the_snapshot_comparison_catches_a_one_byte_drift() {
    let baseline = read_repo_file(BASELINE_SNAPSHOT);
    assert!(
        first_line_diff(&baseline, &baseline).is_none(),
        "a snapshot compared against itself is identical"
    );

    // Flip exactly one character of the structural fingerprint.
    let drifted: String = baseline
        .lines()
        .map(|l| {
            if let Some(rest) = l.strip_prefix("fingerprint-structural=") {
                format!("fingerprint-structural={}0", &rest[..rest.len() - 1])
            } else {
                l.to_owned()
            }
        })
        .collect::<Vec<_>>()
        .join("\n");
    let diff = first_line_diff(&baseline, &drifted);
    assert!(
        diff.is_some(),
        "a one-character change to the structural fingerprint must be reported — \
         the behavioural-identity check is a real comparison"
    );
    println!("snapshot drift control: caught at line {}", diff.expect("caught").0);

    // A truncated snapshot is a divergence too, not a prefix match.
    let truncated: String = baseline.lines().take(3).collect::<Vec<_>>().join("\n");
    assert!(
        first_line_diff(&baseline, &truncated).is_some(),
        "a truncated snapshot must be reported as a divergence"
    );
}

// ===========================================================================
// (5) STRUCTURAL GUARANTEES
// ===========================================================================

/// Every `panic = "abort"` setting in a manifest's profile tables, with the
/// profile it sits under. Comments are stripped first, so the *documentation* of
/// why abort is refused never reads as the setting itself.
fn profiles_setting_panic_abort(manifest: &str) -> Vec<String> {
    let mut current = String::new();
    let mut hits = Vec::new();
    for line in manifest.lines() {
        let code = line.split('#').next().unwrap_or("").trim();
        if code.starts_with('[') && code.ends_with(']') {
            current = code.trim_matches(['[', ']']).to_owned();
            continue;
        }
        let Some((key, value)) = code.split_once('=') else {
            continue;
        };
        if key.trim() == "panic" && value.trim().trim_matches('"') == "abort" {
            hits.push(format!("[{current}] {code}"));
        }
    }
    hits
}

/// **No profile anywhere sets `panic = "abort"`.** Panic containment converts a
/// task panic into a `failed` terminal state, which is only possible while panics
/// unwind — `execution::check_panic_strategy` refuses to start a run otherwise.
/// Scanned over every manifest in the workspace, not just the root.
#[test]
fn no_profile_anywhere_sets_panic_abort() {
    let mut manifests = vec!["Cargo.toml".to_owned()];
    manifests.extend(MEMBERS.map(|m| format!("crates/{m}/Cargo.toml")));

    let mut offenders = Vec::new();
    for rel in &manifests {
        for hit in profiles_setting_panic_abort(&read_repo_file(rel)) {
            offenders.push(format!("{rel}: {hit}"));
        }
    }
    assert!(
        offenders.is_empty(),
        "these profiles set `panic = \"abort\"`, which would turn every contained \
         task panic into a process abort:\n{}",
        offenders.join("\n")
    );

    // The one profile that names the key at all names it explicitly as `unwind`,
    // so a consumer inheriting these settings cannot flip it by accident.
    assert!(
        read_repo_file("Cargo.toml").contains("panic = \"unwind\""),
        "`[profile.release]` states `panic = \"unwind\"` explicitly"
    );

    // The shipped profile checker agrees (it also covers the rustflags route).
    let (ok, out) = run_script("scripts/check-cargo-profiles.sh", &[]);
    assert!(ok, "the Cargo-profile invariant checker passes\n{out}");
}

/// **Non-vacuity: the abort scan catches a planted profile.** A commented-out
/// mention must not trip it, and a real setting must.
#[test]
fn the_panic_abort_scan_catches_a_planted_profile() {
    let clean = "[profile.release]\nlto = \"fat\"\n# panic = \"abort\" is refused\npanic = \"unwind\"\n";
    assert!(
        profiles_setting_panic_abort(clean).is_empty(),
        "a commented explanation of why abort is refused is not the setting"
    );
    let planted = "[profile.release]\nlto = \"fat\"\npanic = \"abort\"\n";
    let hits = profiles_setting_panic_abort(planted);
    assert_eq!(hits.len(), 1, "a planted `panic = \"abort\"` is reported: {hits:?}");
    assert!(
        hits[0].contains("profile.release"),
        "the report names the profile it found: {hits:?}"
    );
}

/// **`render` and `metastore` have no dependency edge onto `core`.** C24's
/// "rendering needs no access to the pipeline binary" and ADR 097 §5's equivalent
/// boundary for the run index are structural facts about the crate graph, not
/// conventions — a missing manifest edge.
#[test]
fn render_and_metastore_have_no_dependency_edge_onto_core() {
    for member in ["render", "metastore"] {
        let manifest = read_repo_file(&format!("crates/{member}/Cargo.toml"));
        let offenders: Vec<&str> = manifest
            .lines()
            .filter(|l| {
                let code = l.split('#').next().unwrap_or("").trim();
                code.starts_with("dagr-core") || code.contains("path = \"../core\"")
            })
            .collect();
        assert!(
            offenders.is_empty(),
            "`dagr-{member}` has a dependency edge onto `dagr-core`, which is the \
             boundary that makes the guarantee structural: {offenders:?}"
        );
    }

    // The shipped skeleton checker asserts the same direction manifest-wide.
    let (ok, out) = run_script("scripts/check-workspace-skeleton.sh", &[]);
    assert!(ok, "the workspace-skeleton dependency-direction checker passes\n{out}");
}

/// The resolved runtime dependency package names of `pkg`, via `cargo tree -e
/// normal` — the resolution cargo would actually build, so a build that merely
/// succeeded proves nothing next to it.
fn resolved_runtime_deps(pkg: &str, extra: &[&str]) -> Vec<String> {
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_owned());
    let out = Command::new(cargo)
        .args(["tree", "-p", pkg, "-e", "normal", "--prefix", "none"])
        .args(extra)
        .current_dir(repo_root())
        .output()
        .expect("cargo tree runs");
    assert!(
        out.status.success(),
        "cargo tree -p {pkg} failed:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter_map(|l| l.split_whitespace().next())
        .filter(|l| !l.is_empty())
        .map(str::to_owned)
        .collect()
}

/// **The resolved feature matrix still holds the zero-dependency guarantee.**
/// `dagr-core` resolves alone under `--no-default-features`, and a default
/// resolution of the binary crate pulls neither `libsql` nor `jsonschema`. Read
/// from the RESOLVED graph here, and cross-checked against the shipped checker,
/// which proves its own query non-vacuous.
#[test]
fn the_resolved_feature_matrix_holds_the_zero_dependency_guarantee() {
    let core = resolved_runtime_deps("dagr-core", &["--no-default-features"]);
    assert!(
        !core.is_empty(),
        "cargo tree reported nothing for dagr-core — the assertion would be vacuous"
    );
    let foreign: Vec<&String> = core.iter().filter(|p| *p != "dagr-core").collect();
    assert!(
        foreign.is_empty(),
        "`dagr-core` must resolve with an EMPTY runtime dependency set under \
         --no-default-features; it reached: {foreign:?}"
    );

    let cli = resolved_runtime_deps("dagr-cli", &[]);
    assert!(cli.len() > 5, "the default dagr-cli resolution is non-trivial");
    for banned in ["libsql", "jsonschema", "dagr-metastore"] {
        assert!(
            !cli.iter().any(|p| p == banned),
            "a DEFAULT build must not pull `{banned}`; the resolved graph has it"
        );
    }
    // Non-vacuity of the query itself: the default resolution does show the edges
    // it is supposed to show, so "absent" means absent and not "nothing queried".
    assert!(
        cli.iter().any(|p| p == "dagr-core"),
        "the query sees real edges (dagr-cli -> dagr-core)"
    );

    let (ok, out) = run_script("scripts/check-feature-matrix.sh", &[]);
    assert!(ok, "the resolved feature-matrix checker agrees\n{out}");
}

// ===========================================================================
// (6) THE PERFORMANCE ENVELOPE
// ===========================================================================

const PERF_HEADING: &str = "### The performance envelope, before and after";

/// **The scale benchmark is within budget, with the pre-M9 and post-M9 figures
/// recorded.** The live run is measured here; the recorded table is what makes
/// the M9 profile change (T93) *accounted for* rather than silently absorbed, so
/// the gate requires both trees' figures and requires every one of them to be
/// under the spec ceiling.
#[test]
fn the_scale_benchmark_is_within_budget_with_both_figures_recorded() {
    let run = dagr_cli::scale_bench::run_scale_benchmark();
    let measured = run.per_node_overhead_ns();
    println!(
        "M9 gate scale benchmark: {measured} ns/node over {SCALE_NODE_COUNT} nodes \
         (spec ceiling {SPEC_CEILING_NS_PER_NODE}, CI budget {CI_BUDGET_NS_PER_NODE})"
    );
    assert!(
        measured <= CI_BUDGET_NS_PER_NODE,
        "the scale benchmark regressed: {measured} ns/node exceeds the CI budget of \
         {CI_BUDGET_NS_PER_NODE} ns/node"
    );

    let register = read_repo_file(REGISTER);
    let table = gate_table(&register, PERF_HEADING, 4);
    assert!(
        !table.is_empty(),
        "the register carries no performance table under `{PERF_HEADING}`"
    );

    let mut trees: BTreeSet<String> = BTreeSet::new();
    for cells in &table {
        let tree = cells[1].to_lowercase();
        let figure = cells[2].replace('`', "");
        let ns: u64 = figure
            .split_whitespace()
            .next()
            .and_then(|n| n.replace(',', "").parse().ok())
            .unwrap_or_else(|| panic!("`{figure}` is not a `<n> ns/node` figure"));
        assert!(
            ns < SPEC_CEILING_NS_PER_NODE,
            "the recorded figure {ns} ns/node for `{tree}` is over the {SPEC_CEILING_NS_PER_NODE} \
             ns/node spec ceiling"
        );
        if tree.contains("pre-m9") {
            trees.insert("pre-m9".into());
        } else if tree.contains("m9 head") || tree.contains("post-m9") {
            trees.insert("post-m9".into());
        }
    }
    assert!(
        trees.contains("pre-m9") && trees.contains("post-m9"),
        "the performance record must carry BOTH the pre-M9 and the post-M9 figures \
         so the T93 profile change is accounted for, not silently absorbed; found {trees:?}"
    );
}

// ===========================================================================
// (7) THE MILESTONE RECORD
// ===========================================================================

/// **The implementation README carries the M9 summary and the ticket total.**
/// The milestone's own index entry is part of the finished state: the summary
/// paragraph naming the milestone's shape and the running total naming M9's
/// ticket span.
#[test]
fn the_implementation_readme_carries_the_m9_summary_and_the_ticket_total() {
    let readme = read_repo_file("docs/implementation/README.md");
    assert!(
        readme.contains("## M9 — Rust best-practice hardening"),
        "the README carries M9's milestone heading"
    );
    let summary = readme
        .lines()
        .find(|l| l.contains("Applies the `.claude/skills/rust-skills` guidance"))
        .expect("the README carries M9's summary paragraph");
    for claim in ["265 rules", "edition 2024", "register", "acceptance gate (T99)"] {
        assert!(
            summary.contains(claim),
            "M9's summary line states `{claim}`"
        );
    }
    let total = readme
        .lines()
        .find(|l| l.trim_start().starts_with("Total: 80 M0"))
        .expect("the README carries the running ticket total");
    assert!(
        total.contains("8 M9 tickets (**T92–T99**, docs 107–114)"),
        "the running total names M9's eight tickets and their doc range:\n{total}"
    );
}
