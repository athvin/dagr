//! **The `dagr.toml` loader, named profiles, and the `file` precedence tier**
//! (ADR 128, T113/T115).
//!
//! ADR 128 permits exactly one kind of configuration file: **runtime knobs**,
//! organised as named profiles, read at **bootstrap** and resolved as the
//! fourth precedence tier — `flag > env > file(profile) > default`. The file
//! configures how one invocation runs and nothing else: it cannot select which
//! flow runs, declare nodes or edges, or reach node policy or placement (the
//! graph stays code), and it is **never** read during assembly, which keeps
//! assembly pure and C20's "empty environment with no configuration present"
//! criterion intact. `dagr-core` never reads it, exactly as it never reads the
//! environment.
//!
//! # Discovery, selection, layering
//!
//! - **Discovery** (`flag > convention > none`): an explicit `--dagr.config
//!   <path>` wins, and a *missing* explicit path is a **hard error** — never a
//!   silent fallback. Otherwise `./dagr.toml` in the invocation's working
//!   directory is used when present. Otherwise there is no file, which is
//!   **not** an error: the default path stays zero-configuration. There is no
//!   user-level fallback (`$XDG_CONFIG_HOME/…`) and no parent-directory walk —
//!   both would make the effective configuration depend on state outside the
//!   invocation, the exact "works on my machine" surface this tier avoids.
//! - **Selection** (`flag > env > default`): `--dagr.profile <name>` wins
//!   outright (the environment is never read), else [`DAGR_PROFILE`], else the
//!   [`default`](DEFAULT_PROFILE) table alone. An **unknown profile name is a
//!   loud failure listing the profiles the file defines** — never a silent
//!   fallback to `default` — and selecting a profile when *no file was found*
//!   is equally loud: the operator asked for a named set of values and did not
//!   get them.
//! - **Layering**: a named profile **layers over `default` key by key**, so a
//!   profile states only its differences. Each effective value records which
//!   profile actually supplied it ([`FileValue::provenance`]).
//!
//! # The closed key set
//!
//! [`file_keys`] is the complete set of key paths a profile may set — the
//! knobs the `dagr run` bootstrap resolves. Every key mirrors an existing
//! reserved flag and (where one exists) `DAGR_*` variable: **no file-only knob
//! exists** (ADR 128 §6), and there is deliberately no key for anything that
//! describes the graph. An unknown key is a loud bootstrap failure naming the
//! file, the profile, and the key — a misspelled knob must never be silently
//! inert. The canonical knob↔key mapping table and its totality assertion are
//! T117's.
//!
//! # Failure posture
//!
//! Every load failure is an [`EnvParseError`] so the established exit-code
//! split applies (a malformed file, unknown key, wrong-typed value, or unknown
//! profile → [`InvalidUsage`](crate::contract::ExitCode::InvalidUsage); an
//! out-of-range *value* caught later at resolution →
//! [`BootstrapFailure`](crate::contract::ExitCode::BootstrapFailure)). Until
//! T116 adds a source discriminator to that type, a file-sourced diagnostic
//! names the file, the profile, and the key in its **detail** text.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::config::EnvParseError;

/// The library-owned flag naming an explicit configuration file
/// (`--dagr.config <path>`). Reserved in
/// [`reserved_flag_names`](crate::contract::reserved_flag_names), so a pipeline
/// parameter can never shadow it. A missing explicit path is a hard error.
pub const CONFIG_FLAG: &str = "--dagr.config";

/// The library-owned flag selecting a profile (`--dagr.profile <name>`).
/// Reserved in [`reserved_flag_names`](crate::contract::reserved_flag_names).
/// A present flag wins outright over [`DAGR_PROFILE`] (the environment is never
/// read).
pub const PROFILE_FLAG: &str = "--dagr.profile";

/// Environment fallback for [`PROFILE_FLAG`] (which profile this invocation
/// uses). With neither flag nor variable, the [`default`](DEFAULT_PROFILE)
/// table alone applies.
pub const DAGR_PROFILE: &str = "DAGR_PROFILE";

/// The conventional file name discovered in the invocation's working directory
/// when no explicit `--dagr.config` is given: `dagr.toml`.
pub const CONFIG_FILE_NAME: &str = "dagr.toml";

/// The profile every other profile layers over: `default`. Present or absent,
/// it is the base layer; a named profile states only its differences.
pub const DEFAULT_PROFILE: &str = "default";

/// The **closed set of file key paths** a profile may set — the runtime knobs
/// the `dagr run` bootstrap resolves, each mirroring its reserved flag (and,
/// where one exists, its `DAGR_*` variable). Dotted paths are nested TOML
/// tables (`pool.memory` is `[default.pool]` `memory = …`).
///
/// Deliberately absent: any key that describes the graph (flow selection,
/// nodes, edges, policy, placement) — permanently excluded by ADR 128 — and
/// the `dagr.blob.*` / `dagr.pod-launch-retries` knobs, whose resolution sites
/// are not on the `dagr run` bootstrap path in this cut; writing one is a loud
/// unknown-key failure rather than a recognized-but-inert key. The canonical
/// mapping table (T117) owns extending this set.
const FILE_KEYS: &[&str] = &[
    "store",
    "grace",
    "teardown-deadline",
    "failure-mode",
    "pool.compute-threads",
    "pool.blocking-threads",
    "pool.memory",
    "pool.headroom-fraction",
    "executor",
    "max-pods",
    "force-roundtrip",
    "metastore",
    "metastore-store",
];

/// The closed set of key paths a profile may set (see the module docs). A key
/// outside this set fails loudly at bootstrap naming the file, the profile,
/// and the key.
#[must_use]
pub fn file_keys() -> &'static [&'static str] {
    FILE_KEYS
}

/// One effective file value: the canonicalized raw text a knob's parser
/// consumes, plus the provenance (file, supplying profile, key) its
/// diagnostics name.
///
/// TOML scalars are canonicalized to their string form (`25` → `"25"`, `true`
/// → `"true"`) and then parsed by the **same** parser the knob's environment
/// variable uses — the file introduces no second value grammar. A value that
/// is not a scalar at all (an array, a date-time, a table where a leaf
/// belongs) is rejected at load time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileValue {
    /// The canonicalized scalar text (what the knob's parser consumes).
    raw: String,
    /// The provenance sentence: ``config file `<path>`, profile `<name>`, key
    /// `<key>` `` — precomputed so every consumer names all three.
    provenance: String,
}

impl FileValue {
    /// The canonicalized scalar text, parsed by the same grammar the knob's
    /// environment variable uses.
    #[must_use]
    pub fn raw(&self) -> &str {
        &self.raw
    }

    /// The provenance sentence naming the file, the supplying profile, and the
    /// key — what a file-sourced diagnostic carries in its detail until T116's
    /// source discriminator lands.
    #[must_use]
    pub fn provenance(&self) -> &str {
        &self.provenance
    }
}

/// The **effective** file tier for one invocation: the selected profile
/// layered over [`default`](DEFAULT_PROFILE), key by key. With no file
/// discovered it is [`empty`](FileTier::empty), and every lookup misses — the
/// resolvers then behave exactly as they did before the tier existed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FileTier {
    /// The layered key → value map (a key appears at most once; the selected
    /// profile's value shadows `default`'s).
    values: BTreeMap<String, FileValue>,
}

impl FileTier {
    /// The tier of an invocation with **no** configuration file: every lookup
    /// misses, so `flag > env > default` behaves exactly as before ADR 128.
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }

    /// The effective value for `key` ([`file_keys`] spelling), or [`None`]
    /// when neither the selected profile nor `default` sets it — the "absent,
    /// so detect / use the compiled default" leg of the tri-state.
    #[must_use]
    pub fn get(&self, key: &str) -> Option<&FileValue> {
        self.values.get(key)
    }
}

/// Parse `--dagr.config` out of a raw invocation (`--dagr.config <path>` /
/// `--dagr.config=<path>`). Absent → [`None`] (fall through to `./dagr.toml`
/// discovery).
///
/// # Errors
/// The diagnostic message when the flag is present with no value — a bare
/// `--dagr.config` silently falling back to discovery would drop an operator's
/// instruction on the floor.
pub fn parse_config_flag(argv: &[std::ffi::OsString]) -> Result<Option<String>, String> {
    crate::config::parse_valued_flag::<String>(argv, CONFIG_FLAG)
}

/// Parse `--dagr.profile` out of a raw invocation, in the same grammars as
/// [`parse_config_flag`]. Absent → [`None`] (fall through to [`DAGR_PROFILE`]
/// / the `default` table).
///
/// # Errors
/// The diagnostic message when the flag is present with no value.
pub fn parse_profile_flag(argv: &[std::ffi::OsString]) -> Result<Option<String>, String> {
    crate::config::parse_valued_flag::<String>(argv, PROFILE_FLAG)
}

/// **Bootstrap entry point**: scan `argv` for `--dagr.config` /
/// `--dagr.profile`, discover the file relative to the invocation's working
/// directory, and load the effective [`FileTier`]. Called on the run path
/// only — never by the assembly verbs, so a `dagr.toml` (present, missing, or
/// malformed) can never reach assembly.
///
/// # Errors
///
/// An [`EnvParseError`] (mapping to
/// [`InvalidUsage`](crate::contract::ExitCode::InvalidUsage)) for a valueless
/// flag, a missing explicit path, an unreadable or malformed file, a
/// wrong-shaped or unknown key, or an unknown/unsatisfiable profile selection
/// — each naming the file, the profile, and the key involved. No file present
/// (with no profile selected) is **not** an error.
pub fn load_file_tier(argv: &[std::ffi::OsString]) -> Result<FileTier, EnvParseError> {
    let explicit =
        parse_config_flag(argv).map_err(|detail| EnvParseError::parse(CONFIG_FLAG, "", detail))?;
    let profile = parse_profile_flag(argv)
        .map_err(|detail| EnvParseError::parse(PROFILE_FLAG, "", detail))?;
    load_file_tier_from(Path::new("."), explicit.as_deref(), profile.as_deref())
}

/// The discovery/load seam behind [`load_file_tier`], with the working
/// directory and the already-parsed flags explicit — so a test can point
/// discovery anywhere without touching the process's working directory, and
/// `RunnableFlow::run_to_store` (which has no argv) can reuse the exact same
/// path with both flags [`None`].
///
/// Discovery: `explicit` when given (a missing or unreadable explicit path is
/// a **hard error** naming it), else `<dir>/dagr.toml` when present, else no
/// file. Selection: `profile_flag` when given (the [`DAGR_PROFILE`]
/// environment variable is **never read** — the flag-wins property), else a
/// non-empty [`DAGR_PROFILE`], else the [`default`](DEFAULT_PROFILE) table
/// alone.
///
/// # Errors
/// As [`load_file_tier`].
pub fn load_file_tier_from(
    dir: &Path,
    explicit: Option<&str>,
    profile_flag: Option<&str>,
) -> Result<FileTier, EnvParseError> {
    let selection = select_profile(profile_flag);

    let Some(path) = discover(dir, explicit)? else {
        // No file. Zero-configuration is the default — but a profile the
        // operator explicitly selected cannot be honoured, and silently
        // running without its values would drop that instruction on the floor.
        if let Some((source, name)) = selection {
            return Err(EnvParseError::parse(
                source,
                &name,
                format!(
                    "profile `{name}` was selected but no configuration file was found \
                     (no --dagr.config was given and no ./{CONFIG_FILE_NAME} exists)"
                ),
            ));
        }
        return Ok(FileTier::empty());
    };
    let shown = path.display().to_string();

    let text = std::fs::read_to_string(&path).map_err(|e| {
        EnvParseError::parse(
            CONFIG_FLAG,
            &shown,
            format!("cannot read config file `{shown}`: {e}"),
        )
    })?;
    let profiles = parse_profiles(&shown, &text)?;

    // Selection: an explicitly selected name must exist (even `default`);
    // implicit selection uses the default table when present, else nothing.
    let selected: Option<&str> = if let Some((source, name)) = &selection {
        if !profiles.contains_key(name.as_str()) {
            let available = profiles
                .keys()
                .map(String::as_str)
                .collect::<Vec<_>>()
                .join(", ");
            return Err(EnvParseError::parse(
                *source,
                name,
                format!(
                    "config file `{shown}` defines no profile `{name}` \
                     (profiles: {available}) — an unknown profile never falls \
                     back to `{DEFAULT_PROFILE}`"
                ),
            ));
        }
        Some(name.as_str())
    } else {
        None
    };

    // Layering: default first, then the selected profile key by key.
    let mut values: BTreeMap<String, FileValue> = BTreeMap::new();
    let mut overlay = |profile_name: &str| {
        if let Some(flat) = profiles.get(profile_name) {
            for (key, raw) in flat {
                values.insert(
                    key.clone(),
                    FileValue {
                        raw: raw.clone(),
                        provenance: format!(
                            "config file `{shown}`, profile `{profile_name}`, key `{key}`"
                        ),
                    },
                );
            }
        }
    };
    overlay(DEFAULT_PROFILE);
    if let Some(name) = selected
        && name != DEFAULT_PROFILE
    {
        overlay(name);
    }

    Ok(FileTier { values })
}

/// Which profile is selected, and by which source (for the diagnostic):
/// `(source, name)` for a flag or a non-empty [`DAGR_PROFILE`], else [`None`]
/// (the implicit `default`). A present flag wins outright — `DAGR_PROFILE` is
/// **never read** on the flag path (the flag-wins property).
fn select_profile(profile_flag: Option<&str>) -> Option<(&'static str, String)> {
    if let Some(name) = profile_flag {
        return Some((PROFILE_FLAG, name.to_string()));
    }
    match std::env::var(DAGR_PROFILE) {
        Ok(name) if !name.is_empty() => Some((DAGR_PROFILE, name)),
        _ => None,
    }
}

/// Discovery: the explicit path when given (missing ⇒ hard error, never a
/// silent fallback), else `<dir>/dagr.toml` when present, else [`None`].
fn discover(dir: &Path, explicit: Option<&str>) -> Result<Option<PathBuf>, EnvParseError> {
    if let Some(p) = explicit {
        let path = PathBuf::from(p);
        if !path.is_file() {
            return Err(EnvParseError::parse(
                CONFIG_FLAG,
                p,
                format!(
                    "the explicitly named configuration file `{p}` does not exist \
                     (an explicit --dagr.config path is never silently skipped)"
                ),
            ));
        }
        return Ok(Some(path));
    }
    let conventional = dir.join(CONFIG_FILE_NAME);
    Ok(conventional.is_file().then_some(conventional))
}

/// Parse the file's text into its profiles: every top-level entry is a profile
/// table; every profile's keys are flattened to dotted paths, checked against
/// the closed key set, and canonicalized to scalar text. The **whole** file is
/// validated — a typo in an unselected profile is still a bootstrap failure,
/// per the never-silent posture (a broken `[prod]` should not wait for the
/// production run to surface).
fn parse_profiles(
    shown: &str,
    text: &str,
) -> Result<BTreeMap<String, BTreeMap<String, String>>, EnvParseError> {
    let table: toml::Table = text.parse().map_err(|e: toml::de::Error| {
        EnvParseError::parse(
            shown,
            "",
            format!("config file `{shown}` is not valid TOML: {e}"),
        )
    })?;
    let mut profiles = BTreeMap::new();
    for (name, value) in &table {
        let toml::Value::Table(profile_table) = value else {
            return Err(EnvParseError::parse(
                shown,
                name,
                format!(
                    "top-level entry `{name}` in config file `{shown}` is not a profile \
                     table; runtime keys live inside a profile (for example under \
                     `[{DEFAULT_PROFILE}]`)"
                ),
            ));
        };
        let mut flat = BTreeMap::new();
        flatten_profile(shown, name, "", profile_table, &mut flat)?;
        profiles.insert(name.clone(), flat);
    }
    Ok(profiles)
}

/// Flatten one profile table into dotted key paths, validating each key
/// against the closed set and each leaf as a scalar. `prefix` is the dotted
/// path accumulated so far (empty at the top of a profile).
fn flatten_profile(
    shown_path: &str,
    profile: &str,
    prefix: &str,
    table: &toml::Table,
    out: &mut BTreeMap<String, String>,
) -> Result<(), EnvParseError> {
    for (name, value) in table {
        let key = if prefix.is_empty() {
            name.clone()
        } else {
            format!("{prefix}.{name}")
        };
        match value {
            // A nested table extends the dotted path ([default.pool] memory →
            // pool.memory).
            toml::Value::Table(nested) => {
                flatten_profile(shown_path, profile, &key, nested, out)?;
            }
            toml::Value::String(s) => {
                insert_known(shown_path, profile, &key, s.clone(), out)?;
            }
            toml::Value::Integer(i) => {
                insert_known(shown_path, profile, &key, i.to_string(), out)?;
            }
            toml::Value::Float(f) => {
                insert_known(shown_path, profile, &key, f.to_string(), out)?;
            }
            toml::Value::Boolean(b) => {
                insert_known(shown_path, profile, &key, b.to_string(), out)?;
            }
            // A date-time or an array is no knob's grammar; rejecting the
            // SHAPE here (rather than failing the parse later) names the kind.
            toml::Value::Datetime(_) | toml::Value::Array(_) => {
                return Err(EnvParseError::parse(
                    shown_path,
                    &key,
                    format!(
                        "key `{key}` in profile `{profile}` of config file \
                         `{shown_path}` must be a scalar (a string, integer, float, \
                         or boolean) — got {}",
                        kind_of(value)
                    ),
                ));
            }
        }
    }
    Ok(())
}

/// Insert a canonicalized scalar under `key`, refusing a key outside the
/// closed set with a diagnostic naming the file, the profile, and the key.
fn insert_known(
    shown_path: &str,
    profile: &str,
    key: &str,
    raw: String,
    out: &mut BTreeMap<String, String>,
) -> Result<(), EnvParseError> {
    if !FILE_KEYS.contains(&key) {
        return Err(EnvParseError::parse(
            shown_path,
            key,
            format!(
                "unknown key `{key}` in profile `{profile}` of config file \
                 `{shown_path}` (known keys: {}) — an unrecognized key is never \
                 silently ignored, and no key can select a flow or describe the graph",
                FILE_KEYS.join(", ")
            ),
        ));
    }
    out.insert(key.to_string(), raw);
    Ok(())
}

/// The human name of a TOML value's kind, for the wrong-shape diagnostic.
fn kind_of(value: &toml::Value) -> &'static str {
    match value {
        toml::Value::String(_) => "a string",
        toml::Value::Integer(_) => "an integer",
        toml::Value::Float(_) => "a float",
        toml::Value::Boolean(_) => "a boolean",
        toml::Value::Datetime(_) => "a date-time",
        toml::Value::Array(_) => "an array",
        toml::Value::Table(_) => "a table",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Write `content` under a fresh temp dir and return (dir, file path).
    fn temp_config(tag: &str, content: &str) -> (PathBuf, PathBuf) {
        let mut dir = std::env::temp_dir();
        dir.push(format!(
            "dagr-t115-cfg-{tag}-{}-{:?}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_nanos())
        ));
        std::fs::create_dir_all(&dir).expect("create temp config dir");
        let path = dir.join("cfg.toml");
        std::fs::write(&path, content).expect("write temp config");
        (dir, path)
    }

    #[test]
    fn no_file_yields_the_empty_tier() {
        let (dir, path) = temp_config("empty-dir", "");
        std::fs::remove_file(&path).expect("leave the dir empty");
        let tier = load_file_tier_from(&dir, None, None).expect("no file is not an error");
        let _ = std::fs::remove_dir_all(&dir);
        assert_eq!(tier, FileTier::empty(), "no file → every lookup misses");
    }

    #[test]
    fn default_and_named_profile_layer_key_by_key() {
        let (dir, path) = temp_config(
            "layering",
            "[default]\ngrace = \"25s\"\nmax-pods = 10\n\n[prod]\nmax-pods = 50\n",
        );
        let tier = load_file_tier_from(&dir, path.to_str(), Some("prod"))
            .expect("a valid layered file loads");
        let _ = std::fs::remove_dir_all(&dir);
        assert_eq!(
            tier.get("max-pods").map(FileValue::raw),
            Some("50"),
            "prod overrides max-pods"
        );
        assert_eq!(
            tier.get("grace").map(FileValue::raw),
            Some("25s"),
            "default's grace layers up"
        );
        assert!(
            tier.get("pool.memory").is_none(),
            "an unmentioned key stays absent (the tri-state)"
        );
    }

    #[test]
    fn unknown_key_names_file_profile_and_key() {
        let (dir, path) = temp_config("unknown-key", "[default]\ngrace-period = \"10s\"\n");
        let err = load_file_tier_from(&dir, path.to_str(), None)
            .expect_err("an unknown key fails loudly");
        let _ = std::fs::remove_dir_all(&dir);
        let text = err.to_string();
        assert!(
            text.contains("grace-period") && text.contains("default") && text.contains("cfg.toml"),
            "the diagnostic names the key, the profile, and the file: {text}"
        );
        assert_eq!(
            err.exit_code(),
            crate::contract::ExitCode::InvalidUsage,
            "an unknown key is invalid usage"
        );
    }

    #[test]
    fn a_top_level_scalar_is_refused_with_guidance() {
        let (dir, path) = temp_config("top-level", "grace = \"10s\"\n");
        let err = load_file_tier_from(&dir, path.to_str(), None)
            .expect_err("a bare top-level key fails loudly");
        let _ = std::fs::remove_dir_all(&dir);
        assert!(
            err.to_string().contains("[default]"),
            "the diagnostic points at the profile shape: {err}"
        );
    }

    #[test]
    fn explicit_selection_of_a_missing_default_is_loud() {
        let (dir, path) = temp_config("explicit-default", "[prod]\nmax-pods = 50\n");
        let err = load_file_tier_from(&dir, path.to_str(), Some(DEFAULT_PROFILE))
            .expect_err("explicitly selecting an undefined profile fails, even `default`");
        let _ = std::fs::remove_dir_all(&dir);
        assert!(
            err.to_string().contains("prod"),
            "the diagnostic lists the profiles the file defines: {err}"
        );
    }

    #[test]
    fn implicitly_absent_default_is_fine() {
        let (dir, path) = temp_config("implicit-default", "[prod]\nmax-pods = 50\n");
        let tier = load_file_tier_from(&dir, path.to_str(), None)
            .expect("a file with no [default] and no selection applies nothing");
        let _ = std::fs::remove_dir_all(&dir);
        assert_eq!(
            tier,
            FileTier::empty(),
            "nothing selected → nothing applies"
        );
    }
}
