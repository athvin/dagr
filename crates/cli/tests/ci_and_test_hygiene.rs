//! **CI coverage and test-isolation hygiene** (T98). Written first, TDD.
//!
//! Two families of gap, both about checks that *exist* but are not *wired*, and
//! both pinned here so they cannot silently come undone again.
//!
//! * **CI does not cover the feature matrix.** The workspace's features are the
//!   mechanism behind three architectural guarantees — `dagr-core` stays
//!   zero-runtime-dependency under `--no-default-features`, `metastore` is
//!   default-off, `dag` confines `inventory` to `dagr-cli` — yet no job built the
//!   matrix. The tests below require a feature-matrix job that builds both ends of
//!   it and asserts the **resolved dependency graph** on the no-default leg, not
//!   merely that the build passed.
//! * **Dormant checkers.** Sixteen of the twenty-six `scripts/check-*.sh` invariant
//!   checkers never ran in CI: they were authored as ticket-time gates and then left
//!   behind. A checker nothing runs is a checker that rots — three of them had
//!   already drifted into permanent failure. The scan below requires every one of
//!   them to be named in the workflow.
//! * **Test isolation.** Much of `crates/cli/tests/` hardcoded a shared literal
//!   base path (`/tmp/dagr-test` across fifteen tests in one file). Collisions were
//!   avoided only because the run store namespaces by `<base>/<pipeline>/<run-id>`
//!   — an *implicit* invariant holding only while no two tests pick the same
//!   pipeline name. The scan below makes isolation structural: every base path is a
//!   [`TempBase`] carrying the pid and a per-call unique component.
//! * **Cfg typo insurance.** `unexpected_cfgs` + `check-cfg` are written out at
//!   `deny`, and the guard is verified to actually bite on a deliberate typo rather
//!   than assumed to.
//! * **Miri.** The verdict — that miri cannot usefully run here — is recorded in the
//!   rust-skills register *and* pinned by an inventory of the `unsafe` surface, so a
//!   third `unsafe` site cannot land without forcing the verdict to be re-taken.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use dagr_core::test_kit::TempBase;

// ===========================================================================
// Shared scanning scaffolding
// ===========================================================================

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("crates/cli has a workspace root two levels up")
        .to_path_buf()
}

fn read(rel: &str) -> String {
    let path = repo_root().join(rel);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{rel} is readable: {e}"))
}

/// Every `*.rs` integration-test file directly under `crates/<crate>/tests/`.
fn test_files(krate: &str) -> Vec<PathBuf> {
    let dir = repo_root().join("crates").join(krate).join("tests");
    let mut out: Vec<PathBuf> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("crates/{krate}/tests is readable: {e}"))
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.is_file() && p.extension().is_some_and(|x| x == "rs"))
        .collect();
    out.sort();
    out
}

fn rel(path: &Path) -> String {
    path.strip_prefix(repo_root())
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

// ===========================================================================
// Dormant checkers: every scripts/check-*.sh runs in CI
// ===========================================================================

/// Every invariant checker in `scripts/`, by file name, sorted.
fn check_scripts() -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(repo_root().join("scripts"))
        .expect("scripts/ is readable")
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|e| e == "sh"))
        .filter_map(|p| p.file_name().and_then(|n| n.to_str()).map(str::to_owned))
        .filter(|n| n.starts_with("check-"))
        .collect();
    names.sort();
    names
}

/// **Test-plan scenario: dormant checkers.** Every `scripts/check-*.sh` is named in
/// the workflow. A checker that nothing runs is not a guard, it is a comment — and
/// three of the sixteen dormant ones had already drifted into permanent failure
/// before anyone noticed, which is exactly the rot this closes.
///
/// The scan is proven non-vacuous by requiring a plausible number of checkers to
/// have been discovered: a glob that silently matched nothing would otherwise pass.
#[test]
fn every_check_script_is_wired_into_ci() {
    let checkers = check_scripts();

    assert!(
        checkers.len() >= 20,
        "the scan found only {} check-*.sh scripts — it is not looking where it \
         should be",
        checkers.len()
    );

    let workflow = read(".github/workflows/ci.yml");
    let dormant: Vec<&String> = checkers
        .iter()
        .filter(|name| !workflow.contains(format!("scripts/{name}").as_str()))
        .collect();

    assert!(
        dormant.is_empty(),
        "these invariant checkers exist but nothing in .github/workflows/ci.yml \
         runs them — wire them or delete them deliberately: {dormant:?}"
    );
}

/// The wiring above must be *execution*, not a passing mention in a comment. Every
/// checker is invoked through a `bash scripts/<name>` run line.
#[test]
fn every_check_script_is_actually_invoked_not_merely_mentioned() {
    let workflow = read(".github/workflows/ci.yml");
    let invoked: BTreeSet<String> = workflow
        .lines()
        .map(str::trim)
        // A run line, not a comment: comments in this workflow start with `#`.
        .filter(|line| !line.starts_with('#'))
        .filter_map(|line| line.split("bash scripts/").nth(1))
        .map(|rest| {
            rest.split_whitespace()
                .next()
                .unwrap_or_default()
                .to_owned()
        })
        .collect();

    let checkers: BTreeSet<String> = check_scripts().into_iter().collect();
    let not_invoked: Vec<&String> = checkers.difference(&invoked).collect();
    assert!(
        not_invoked.is_empty(),
        "these checkers are named in ci.yml but never invoked with `bash \
         scripts/<name>`: {not_invoked:?}"
    );
}

// ===========================================================================
// The feature matrix
// ===========================================================================

/// **Test-plan scenario: feature matrix.** A job builds both ends of the matrix and
/// runs the whole suite under `--all-features` — the `metastore` +
/// `schema-validation` + `dag` combination no job ran before.
#[test]
fn ci_builds_both_ends_of_the_feature_matrix() {
    let workflow = read(".github/workflows/ci.yml");
    for command in [
        "cargo build --workspace --no-default-features",
        "cargo build --workspace --all-features",
        "cargo test --workspace --all-features",
    ] {
        assert!(
            workflow.contains(command),
            "no CI job runs `{command}` — the feature matrix is the mechanism \
             behind three architectural guarantees, so it has to be built"
        );
    }
}

/// The no-default leg is the standing proof of `dagr-core`'s zero-runtime-dependency
/// guarantee, and a build succeeding proves only that it compiled. The **resolved
/// graph** is what carries the claim, so a dedicated checker asserts it and CI runs
/// that checker.
#[test]
fn the_no_default_leg_asserts_the_resolved_dependency_graph() {
    let script = repo_root().join("scripts/check-feature-matrix.sh");
    assert!(
        script.is_file(),
        "scripts/check-feature-matrix.sh must exist: the no-default-features leg \
         has to assert the resolved dependency graph (no dagr-macros, inventory, \
         libsql, or dagr-metastore edge onto dagr-core), not just that the build \
         passed"
    );
    let text = std::fs::read_to_string(&script).expect("the checker is readable");
    for forbidden in ["dagr-macros", "inventory", "libsql", "dagr-metastore"] {
        assert!(
            text.contains(forbidden),
            "check-feature-matrix.sh must assert that the no-default-features \
             resolution of dagr-core carries no {forbidden} edge"
        );
    }
    assert!(
        read(".github/workflows/ci.yml").contains("bash scripts/check-feature-matrix.sh"),
        "the feature-matrix graph assertion has to run in CI"
    );
}

// ===========================================================================
// Test isolation: one shared unique-temp-base helper, no literal paths
// ===========================================================================

/// A literal path in a test file may be exempted, but only in the open: a comment
/// within the three lines above it says `TEMP-BASE-EXEMPT:` and states why. The same
/// idiom `scripts/check-lint-parity.sh` uses for its `EXPECT-EXEMPT:` suppressions.
const EXEMPT_MARKER: &str = "TEMP-BASE-EXEMPT:";

/// How far above a literal the exemption marker may sit. Three lines is enough for a
/// two-line reason plus the marker itself, and short enough that the exemption is
/// still visibly attached to what it exempts.
const EXEMPT_LOOKBACK: usize = 3;

/// This file is the scanner. Its own source names the patterns it looks for — in the
/// scan itself and in the failure messages — so scanning it would report the guard as
/// its own first violation.
const SCANNER: &str = "ci_and_test_hygiene.rs";

/// Every `*.rs` test file the isolation scans cover: `crates/{cli,core}/tests/`, minus
/// this file.
fn scanned_test_files() -> Vec<PathBuf> {
    ["cli", "core"]
        .into_iter()
        .flat_map(test_files)
        .filter(|p| p.file_name().is_none_or(|n| n != SCANNER))
        .collect()
}

/// **Test-plan scenario: no two tests share a filesystem path — by construction.**
/// No test file names a literal `/tmp/...` base. Isolation cannot rest on every test
/// happening to pick a distinct pipeline name; it rests on the base path itself
/// being unique.
#[test]
fn no_cli_test_hardcodes_a_shared_temp_path() {
    let mut offenders: Vec<String> = Vec::new();
    let mut scanned = 0usize;
    for path in scanned_test_files() {
        scanned += 1;
        let text = std::fs::read_to_string(&path).expect("test file is readable");
        let lines: Vec<&str> = text.lines().collect();
        for (i, line) in lines.iter().enumerate() {
            if !line.contains("\"/tmp/") {
                continue;
            }
            let exempt = lines[i.saturating_sub(EXEMPT_LOOKBACK)..i]
                .iter()
                .any(|l| l.contains(EXEMPT_MARKER));
            if !exempt {
                offenders.push(format!("{}:{}: {}", rel(&path), i + 1, line.trim()));
            }
        }
    }
    assert!(
        scanned > 50,
        "the scan covered only {scanned} test files — it is not looking where it \
         should be"
    );
    assert!(
        offenders.is_empty(),
        "these tests hardcode a shared literal temp path instead of using the \
         shared `TempBase` helper — collisions are then avoided only by every test \
         picking a distinct pipeline name, the implicit invariant this ticket \
         removes:\n{}",
        offenders.join("\n")
    );
}

/// The helper is *shared*, not copy-pasted. Two shapes of local copy had grown, one
/// per file: a `struct TempBase` RAII guard (seven files) and a
/// `fn temp_base(…) -> PathBuf` factory (fifteen), the latter unique by construction
/// but leaking every directory it made. Both are gone; this keeps a third from
/// starting.
///
/// `crates/metastore/tests/` is deliberately outside the scan and keeps its own
/// copies: `dagr-metastore` has **no** dependency edge onto `dagr-core` by design
/// (ADR 097 §5 — the same C24-style boundary `render` holds), so it structurally
/// cannot reach the promoted helper, and adding the edge to share a test utility
/// would trade an architectural guarantee for a de-duplication.
#[test]
fn no_test_file_redefines_the_temp_base_helper() {
    // Assembled at runtime so this file's own source does not match its own scan.
    let struct_copy = concat!("struct ", "TempBase");
    let fn_copy = concat!("fn ", "temp_base(");
    let mut offenders: Vec<String> = Vec::new();
    for path in scanned_test_files() {
        let text = std::fs::read_to_string(&path).expect("test file is readable");
        if text.contains(struct_copy) {
            offenders.push(format!("{}: local `struct TempBase`", rel(&path)));
        }
        if text.contains(fn_copy) {
            offenders.push(format!("{}: local `fn temp_base(…)` factory", rel(&path)));
        }
    }
    assert!(
        offenders.is_empty(),
        "these test files define their own temp-base helper instead of using the \
         shared `dagr_core::test_kit::TempBase`:\n{}",
        offenders.join("\n")
    );
}

/// **Test-plan scenario: prove isolation by construction.** Every base carries the
/// process id and a per-call unique component, and concurrent callers never collide.
/// This is the property the scan above is enforcing adoption of.
#[test]
fn temp_bases_are_unique_by_construction_across_threads() {
    const THREADS: usize = 8;
    const PER_THREAD: usize = 16;

    let pid = std::process::id().to_string();
    let handles: Vec<_> = (0..THREADS)
        .map(|t| {
            std::thread::spawn(move || {
                (0..PER_THREAD)
                    .map(|i| {
                        let base = TempBase::new(&format!("uniqueness-{t}-{i}"));
                        let path = base.path().to_path_buf();
                        assert!(path.is_dir(), "TempBase creates its directory eagerly");
                        path
                    })
                    .collect::<Vec<_>>()
            })
        })
        .collect();

    let mut seen: BTreeSet<PathBuf> = BTreeSet::new();
    let mut total = 0usize;
    for handle in handles {
        for path in handle.join().expect("uniqueness worker did not panic") {
            total += 1;
            assert!(
                path.to_string_lossy().contains(&pid),
                "every base path carries the pid so two concurrent processes \
                 cannot collide either: {}",
                path.display()
            );
            assert!(
                seen.insert(path.clone()),
                "two TempBase values produced the same path: {}",
                path.display()
            );
        }
    }
    assert_eq!(
        total,
        THREADS * PER_THREAD,
        "every worker produced its full batch"
    );
}

/// **Test-plan scenario: repeated local runs do not accumulate stale directories.**
/// The helper is RAII: the subtree — including anything a run wrote beneath it — is
/// gone when the guard drops.
#[test]
fn a_temp_base_removes_its_whole_subtree_on_drop() {
    let path = {
        let base = TempBase::new("raii");
        let nested = base.path().join("pipeline").join("run-id");
        std::fs::create_dir_all(&nested).expect("a run writes beneath the base");
        std::fs::write(nested.join("events.jsonl"), b"{}\n").expect("and writes a file");
        base.path().to_path_buf()
    };
    assert!(
        !path.exists(),
        "dropping a TempBase must remove its whole subtree, so a repeated local \
         run does not accumulate stale directories: {} survived",
        path.display()
    );
}

// ===========================================================================
// Cfg typo insurance: unexpected_cfgs + check-cfg
// ===========================================================================

/// **Definition of done: `unexpected_cfgs` + `check-cfg` are configured.** Written
/// out at `deny` in both halves of the lint policy — the same ratchet shape T96 used
/// for `missing_docs` and T97 for `await_holding_lock`. `warnings = "deny"` already
/// promotes it today; writing it out makes weakening it a visible edit here rather
/// than an invisible consequence of an upstream regrouping, and the explicit
/// `check-cfg` states dagr's custom-cfg surface (empty) at the setting.
#[test]
fn unexpected_cfgs_is_written_out_with_check_cfg_in_both_halves() {
    for file in ["lints.toml", "Cargo.toml"] {
        let text = read(file);
        let line = text
            .lines()
            .find(|l| l.trim_start().starts_with("unexpected_cfgs"))
            .unwrap_or_else(|| {
                panic!(
                    "{file} must declare `unexpected_cfgs` — it is the insurance \
                     against a future `#[cfg(feature = \"metastor\")]` typo \
                     compiling silently into dead code"
                )
            });
        assert!(
            line.contains(r#"level = "deny""#),
            "{file} must set unexpected_cfgs to deny, got: {line}"
        );
        assert!(
            line.contains("check-cfg"),
            "{file} must state dagr's custom-cfg surface with check-cfg, got: {line}"
        );
    }
}

/// **Test-plan scenario: verify the guard actually bites.** A deliberately misspelled
/// `#[cfg(feature = "metastor")]` compiled under the repository's own declared
/// `unexpected_cfgs` level must be rejected — and the correctly-spelled one must not
/// be, so the check is not simply "everything fails".
#[test]
fn the_unexpected_cfgs_guard_bites_on_a_deliberate_typo() {
    // The level is read from the repository's policy, so this proves the *declared*
    // configuration bites, not a level invented by the test.
    let policy = read("lints.toml");
    let line = policy
        .lines()
        .find(|l| l.trim_start().starts_with("unexpected_cfgs"))
        .expect("lints.toml declares unexpected_cfgs");
    assert!(
        line.contains(r#"level = "deny""#),
        "the declared level must be deny for the guard to bite: {line}"
    );

    let dir = TempBase::new("cfg-guard");
    let compile = |body: &str, name: &str| -> std::process::Output {
        let src = dir.path().join(format!("{name}.rs"));
        std::fs::write(&src, body).expect("fixture source is writable");
        std::process::Command::new("rustc")
            .arg("--edition=2024")
            .arg("--crate-type=lib")
            .arg("--emit=metadata")
            .arg("--out-dir")
            .arg(dir.path())
            .arg("--check-cfg")
            .arg(r#"cfg(feature, values("metastore"))"#)
            .arg("-D")
            .arg("unexpected_cfgs")
            .arg(&src)
            .output()
            .expect("rustc runs")
    };

    let typo = compile(
        "#[cfg(feature = \"metastor\")]\npub fn dead() {}\n",
        "typo_fixture",
    );
    let stderr = String::from_utf8_lossy(&typo.stderr);
    assert!(
        !typo.status.success(),
        "a misspelled feature cfg compiled clean — the guard does not bite:\n{stderr}"
    );
    assert!(
        stderr.contains("unexpected_cfgs") || stderr.contains("unexpected `cfg`"),
        "the rejection must name the cfg lint, got:\n{stderr}"
    );

    // Negative control: the correct spelling is accepted, so the guard is
    // discriminating rather than merely hostile.
    let correct = compile(
        "#[cfg(feature = \"metastore\")]\npub fn alive() {}\n",
        "correct_fixture",
    );
    assert!(
        correct.status.success(),
        "the correctly-spelled cfg must compile — otherwise the check above proves \
         nothing:\n{}",
        String::from_utf8_lossy(&correct.stderr)
    );
}

// ===========================================================================
// Miri: the verdict is recorded, and pinned to the unsafe surface it rests on
// ===========================================================================

/// Every `unsafe` construct in production source, with the reason miri cannot reach
/// it. The verdict recorded in the register rests on this list being complete; the
/// test below makes a third site impossible to land silently.
const UNSAFE_INVENTORY: &[(&str, &str)] = &[
    (
        "crates/core/src/metrics.rs",
        "the attributing `GlobalAlloc`. Miri supplies its own allocator and ignores \
         `#[global_allocator]` entirely, so the one production `unsafe impl` is \
         precisely the code miri cannot execute",
    ),
    (
        "crates/cli/src/config.rs",
        "`std::env::set_var`/`remove_var`, unsafe in edition 2024 because concurrent \
         environment mutation races at the libc level — a hazard outside miri's UB \
         model, and confined here to the test-support env helper",
    ),
];

/// **Definition of done: the register records why miri cannot usefully run here.**
/// A recorded verdict rots the moment the facts under it change, so it is pinned to
/// the `unsafe` surface it was taken against: a third `unsafe` site fails this test
/// and forces the verdict to be re-taken rather than inherited.
#[test]
fn the_unsafe_surface_is_exactly_what_the_miri_verdict_was_taken_against() {
    let root = repo_root();
    let mut found: BTreeMap<String, usize> = BTreeMap::new();
    for entry in std::fs::read_dir(root.join("crates"))
        .expect("the crates directory is readable")
        .flatten()
    {
        let mut sources = Vec::new();
        collect_rust_sources(&entry.path().join("src"), &mut sources);
        for path in sources {
            let text = std::fs::read_to_string(&path).expect("source is readable");
            let count = text
                .lines()
                .filter(|l| !l.trim_start().starts_with("//"))
                .filter(|l| {
                    l.contains("unsafe {") || l.contains("unsafe impl") || l.contains("unsafe fn")
                })
                .count();
            if count > 0 {
                found.insert(rel(&path), count);
            }
        }
    }

    let expected: BTreeSet<&str> = UNSAFE_INVENTORY.iter().map(|(f, _)| *f).collect();
    let actual: BTreeSet<&str> = found.keys().map(String::as_str).collect();
    assert_eq!(
        actual, expected,
        "the production `unsafe` surface changed. The recorded miri verdict \
         (docs/rust-skills-register.md, `unsafe-miri-ci`) was taken against exactly \
         these files — re-take it, then update UNSAFE_INVENTORY"
    );
}

/// The verdict itself is in the register, in words, with its reason — not implied by
/// the absence of a job.
#[test]
fn the_register_records_the_miri_verdict_with_its_reason() {
    let register = read("docs/rust-skills-register.md");
    let row = register
        .lines()
        .find(|l| l.contains("| unsafe-miri-ci |"))
        .expect("the register dispositions unsafe-miri-ci");
    for needle in ["global_allocator", "nightly"] {
        assert!(
            row.contains(needle),
            "the unsafe-miri-ci verdict must say why miri cannot help here \
             (mentioning {needle:?}); a bare disposition is the rot this ticket \
             exists to stop. Got:\n{row}"
        );
    }
}

fn collect_rust_sources(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_rust_sources(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

// ===========================================================================
// Housekeeping: dangling references, the run-output tree, fixture headers
// ===========================================================================

/// **Definition of done: the `docs/quality-gates.md` / `scripts/run_gate.sh`
/// references are resolved.** Both were cited as repository-relative paths that do
/// not exist — a merge-gate document pointing at a phantom gate is worse than one
/// that does not mention it. They live in the ticket-loop skill, so the citations
/// must either name that location or be gone.
#[test]
fn no_document_cites_a_quality_gate_that_does_not_exist() {
    let mut offenders: Vec<String> = Vec::new();
    let candidates = [
        ".github/workflows/ci.yml",
        "docs/coverage-matrix.md",
        "CONTRIBUTING.md",
        "README.md",
    ];
    for file in candidates {
        for (i, line) in read(file).lines().enumerate() {
            for needle in ["quality-gates.md", "run_gate.sh"] {
                if !line.contains(needle) {
                    continue;
                }
                // A citation is resolved when it names where the file actually is.
                if line.contains(".claude/skills/shipping-dagr-tickets") {
                    continue;
                }
                offenders.push(format!("{file}:{}: {}", i + 1, line.trim()));
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "these lines cite a quality-gate artifact by a path that does not exist in \
         this repository — resolve the citation or drop it:\n{}",
        offenders.join("\n")
    );
}

/// **Definition of done: `layer-c-runs/` is deleted.** A leftover run-output tree
/// checked into the repository root; this keeps it from coming back.
#[test]
fn no_run_output_tree_is_checked_in_at_the_repository_root() {
    let stray = repo_root().join("layer-c-runs");
    assert!(
        !stray.exists(),
        "layer-c-runs/ is a leftover run-output tree, not source — runs belong \
         under a run store, not the repository root"
    );
}

/// **Definition of done: `crates/core/tests/ui/*.rs` use `//!` headers.** The
/// otherwise-identical fixture directories under `crates/cli/tests/ui/` and
/// `crates/macros/tests/expand/` already do; thirty fixtures in core used `//`, so
/// the convention was two-thirds of a convention.
#[test]
fn every_ui_fixture_opens_with_an_inner_doc_header() {
    let mut offenders: Vec<String> = Vec::new();
    let mut checked = 0usize;
    for dir in [
        "crates/core/tests/ui",
        "crates/cli/tests/ui",
        "crates/macros/tests/expand",
    ] {
        let mut sources = Vec::new();
        collect_rust_sources(&repo_root().join(dir), &mut sources);
        for path in sources {
            checked += 1;
            let text = std::fs::read_to_string(&path).expect("fixture is readable");
            let first = text.lines().next().unwrap_or_default();
            if !first.starts_with("//!") {
                offenders.push(format!("{}: {first}", rel(&path)));
            }
        }
    }
    assert!(
        checked >= 40,
        "the scan found only {checked} UI fixtures — it is not looking where it \
         should be"
    );
    assert!(
        offenders.is_empty(),
        "every compile-fail/compile-pass fixture opens with a `//!` header saying \
         what it proves; these do not:\n{}",
        offenders.join("\n")
    );
}
