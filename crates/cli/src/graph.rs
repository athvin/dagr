//! C20 · **Graph artifact emission** — the pipeline's structure, obtained without
//! executing it (arch.md `### C20 · Graph artifact`; the T0.7 ADR,
//! `docs/implementation/013-T0.7-stable-name-and-fingerprint-adr.md`; ticket
//! T40).
//!
//! This module serializes an assembled [`Pipeline`] into a **schema-valid graph
//! artifact** — the on-demand output that opens M3, "it explains itself." It is
//! the one place the live pipeline (`dagr-core`) and the artifact serialization
//! stack (`serde_json`, the T4-sanctioned runtime writer) meet; `dagr-artifact`
//! cannot see `dagr-core` (the C24 boundary), so the bridge lives here in
//! `dagr-cli`, behind the **graph verb** of the CLI contract (C26).
//!
//! # What it emits, and when
//!
//! Given an assembled pipeline it serializes, for **every node**: the identity
//! name, group label, the author-declared **stable task name**, stable **input**
//! and **output** type names, effective **execution class**, the **complete
//! effective policy** with every C5 field written out (defaults included),
//! declared **resource requirements** (the per-pool cost vector), and the
//! **dependency list**. For **every edge**: its **kind** (data vs ordering) and,
//! for a data edge, the **stable name of the carried type**. The versioned
//! **header** carries the schema version, tool version, generation time, pipeline
//! identity, the **computed C21 fingerprints** — the structural fingerprint, the
//! policy hash (each a version-prefixed string, [`format_fingerprint_structural`]),
//! and the algorithm version (T41) — and **build provenance** (tool version, git
//! commit, lockfile hash) embedded at build time.
//!
//! Emission runs from **pure assembly** — no credentials, no network, no database,
//! no run store, no parameters (C7 / C20). That empty-environment guarantee is why
//! the artifact is trustworthy and why it runs in CI on every pull request.
//!
//! # Author-declared stable names, never `type_name` as identity (C20 / C21)
//!
//! Every identity or type field records the **author-declared** stable name (the
//! [`StableName`](dagr_core::StableName) constant captured at registration), never
//! [`std::any::type_name`], whose output is unstable across compilers. `type_name`
//! is permitted **only** in the node's informational `type_name` debug field, and
//! this emitter never populates that field with anything load-bearing — it is
//! reserved (T0.7 §1). A node lacking captured stable names (registered through a
//! type-erased registrar) is **not emittable** to this contract and produces a
//! clear [`GraphEmitError`].
//!
//! # Byte-identity (C20)
//!
//! Two emissions from one binary are **byte-identical** outside the generation-time
//! field: the artifact is serialized through the shared T4 §6 canonicalizer
//! ([`dagr_artifact::canonical`]), nodes and edges are emitted in a deterministic,
//! registration-order-independent order (node name; edge `(from, to, kind)`), and
//! every header field but generation time is fixed per binary. The
//! [generation-time field](GENERATED_AT_FIELD) is the **only** field allowed to
//! vary; [`mask_generated_at`] blanks it for a byte-identity comparison.
//!
//! # Fingerprints (C21 / T41)
//!
//! The header's two hashes are the **computed** C21 fingerprints, obtained from
//! `dagr-core`'s public reuse surface ([`Pipeline::fingerprint`](dagr_core::Pipeline::fingerprint))
//! — this emitter never re-derives the composition. Each is written as a
//! self-describing, version-prefixed string
//! (`fnv1a-64:v<version>:<hex>`), and the [algorithm
//! version](dagr_core::FINGERPRINT_ALGORITHM_VERSION) is also carried as its own
//! header integer. Because every hashed input is author-declared, the two hashes
//! are **identical across machines and toolchains** for unchanged source and are
//! **unaffected** by the generation time or the build provenance (T0.7 §5) — the
//! byte-identity guarantee above therefore extends to the fingerprint fields.
//!
//! # Scope (T40 / T41)
//!
//! This module **emits** the C20 artifact and populates its C21 fingerprint slot
//! (T41). It folds **no** event stream (T42) and renders **no** diagram (T46). It
//! serializes whatever **ordering** edges (C4 / T50) the assembled graph carries —
//! tagged `ordering` with no carried type, distinct from `data` edges — and the
//! ordering-edge *authoring* surface itself lives in `dagr-core` (T50). None of
//! its output is a runtime outcome — it describes **structure only**.

use std::fmt;

use dagr_core::binding::EdgeKind;
use dagr_core::flow::{Pipeline, PipelineNode};
use dagr_core::task::ExecutionClass;
use dagr_core::{
    is_well_formed, EffectivePolicy, FingerprintSlot, TriggerRule, FINGERPRINT_ALGORITHM_VERSION,
    UNIT_STABLE_NAME,
};
use serde_json::{json, Map, Value};

/// The self-identifying schema version this emitter targets (T4 §3; matches
/// `schemas/graph/v1.schema.json`).
pub const GRAPH_SCHEMA_VERSION: &str = "dagr.graph@1";

/// The published graph-schema **major version** the emitted artifact validates
/// against (`schemas/graph/v1.schema.json`).
pub const GRAPH_SCHEMA_MAJOR: u32 = 1;

/// The header field carrying the artifact's **generation time** — the **only**
/// field excluded from byte-identity comparisons (C20). Everything else is fixed
/// per binary. Kept as a named constant so the emitter, [`mask_generated_at`],
/// and the tests all name the same field.
pub const GENERATED_AT_FIELD: &str = "generated_at";

/// The prefix marking a computed C21 fingerprint string in the graph header: the
/// hash-family tag `fnv1a-64` (matching the [build-provenance lockfile-hash
/// convention](BuildProvenance) and the [`NodeId`](dagr_core::NodeId) digest
/// family), so the string is self-describing and a future algorithm change is
/// visible in the value. The full form is
/// `fnv1a-64:v<algorithm_version>:<16-hex-digits>` (see
/// [`format_fingerprint_structural`]).
pub const FINGERPRINT_HASH_FAMILY: &str = "fnv1a-64";

/// **Build provenance** embedded into the pipeline binary at build time (arch.md
/// C20; "Stability · Supply chain"): tool version, git commit SHA, and lockfile
/// hash — all fixed per binary and identical across every emission from it. The
/// values are resolved by the crate's `build.rs` and read through `env!`, so they
/// are compiled in rather than probed at emit time (which keeps emission
/// environment-free, C20).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildProvenance {
    tool_version: String,
    git_commit: String,
    lockfile_hash: String,
}

impl BuildProvenance {
    /// The provenance **embedded at build time** for this binary (the values the
    /// `build.rs` resolved into `env!`). Fixed per binary; every call returns the
    /// same values, so repeated emissions carry identical provenance.
    #[must_use]
    pub fn embedded() -> Self {
        Self {
            tool_version: env!("DAGR_BUILD_TOOL_VERSION").to_string(),
            git_commit: env!("DAGR_BUILD_GIT_COMMIT").to_string(),
            lockfile_hash: env!("DAGR_BUILD_LOCKFILE_HASH").to_string(),
        }
    }

    /// Construct explicit provenance (for tests that assert on fixed values). The
    /// production path is [`embedded`](BuildProvenance::embedded).
    #[must_use]
    pub fn new(
        tool_version: impl Into<String>,
        git_commit: impl Into<String>,
        lockfile_hash: impl Into<String>,
    ) -> Self {
        Self {
            tool_version: tool_version.into(),
            git_commit: git_commit.into(),
            lockfile_hash: lockfile_hash.into(),
        }
    }

    /// The embedded tool version.
    #[must_use]
    pub fn tool_version(&self) -> &str {
        &self.tool_version
    }

    /// The embedded git commit SHA (or the `unknown` sentinel when git was
    /// unavailable at build time).
    #[must_use]
    pub fn git_commit(&self) -> &str {
        &self.git_commit
    }

    /// The embedded lockfile hash (the resolved dependency set's content hash).
    #[must_use]
    pub fn lockfile_hash(&self) -> &str {
        &self.lockfile_hash
    }
}

/// A failure to emit the graph artifact (arch.md C20). Emission fails only for a
/// **structural** reason the assembled pipeline cannot satisfy — never for a
/// missing environment resource (there is none to miss).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GraphEmitError {
    /// A node carries no author-declared [stable names](dagr_core::StableTypeNames)
    /// (it was registered through a type-erased registrar). The C20 artifact
    /// requires the stable task/input/output names as identity, so such a node
    /// cannot be emitted. Names the offending node.
    MissingStableNames {
        /// The identity name of the node lacking stable names.
        node: String,
    },
    /// A recorded stable name is **malformed** (fails [`is_well_formed`]) — the
    /// whole-pipeline validity rule the artifact enforces (T0.7 §1). Names the
    /// node and the offending value.
    MalformedStableName {
        /// The identity name of the node carrying the malformed stable name.
        node: String,
        /// The malformed stable-name value.
        value: String,
    },
}

impl fmt::Display for GraphEmitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingStableNames { node } => write!(
                f,
                "node `{node}` carries no author-declared stable names; register it through a \
                 stable-name-aware registrar (register_named / register_source_named) so the C20 \
                 graph artifact can record its stable task/input/output type names"
            ),
            Self::MalformedStableName { node, value } => write!(
                f,
                "node `{node}` declares a malformed stable name `{value}`; a stable name must be \
                 non-empty and use only ASCII letters, digits, and `_ - . :`"
            ),
        }
    }
}

impl std::error::Error for GraphEmitError {}

/// Emit the **schema-valid graph artifact** for `pipeline` as a canonical JSON
/// string (arch.md C20), stamped with `pipeline_name` as its identity, the
/// `generated_at` generation time (the only byte-varying field), and the
/// build-time `provenance`.
///
/// The output is deterministic and byte-identical across repeated emissions from
/// one binary outside the generation-time field: nodes and edges are ordered
/// canonically (node name; edge `(from, to, kind)`), the full effective policy is
/// written out with defaults, and every header field but `generated_at` is fixed.
/// It validates against `schemas/graph/v1.schema.json` (T39).
///
/// `generated_at` is injected by the caller (the CLI verb reads the clock once),
/// which keeps this function itself clock-free and lets tests supply a controlled
/// instant.
///
/// # Errors
///
/// Returns [`GraphEmitError`] if any node lacks author-declared stable names, or
/// carries a malformed stable name — the only structural reasons emission can
/// fail (no environment resource is ever required, C20).
pub fn emit_graph(
    pipeline: &Pipeline,
    pipeline_name: &str,
    generated_at: &str,
    provenance: &BuildProvenance,
) -> Result<String, GraphEmitError> {
    let artifact = build_artifact(pipeline, pipeline_name, generated_at, provenance)?;
    Ok(dagr_artifact::canonical::to_canonical_string(&artifact))
}

/// The **graph verb** of the CLI contract (arch.md `### C26 · Command-line
/// contract`: *"emit the graph"*): emit the assembled pipeline's schema-valid
/// graph artifact to `sink`, requiring **no run store, no parameters, and no
/// network** (C20 / C7).
///
/// This is the library entry point the CLI verb dispatcher (the argument parsing
/// and exit-code table are T55 / C26) invokes. It reads the wall clock **once**
/// for the generation-time field, uses the build-time embedded
/// [`BuildProvenance`], and writes the canonical artifact followed by a trailing
/// newline. It opens no run store and reads no parameters — assembly is pure (C7),
/// and this verb runs it with no store at all (arch.md "The shape of a run": *"the
/// inspection verbs run assembly with no store"*).
///
/// `pipeline_name` is the pipeline's identity (the stable pipeline name, T0.6);
/// `now_rfc3339` is the caller-supplied generation timestamp so the clock read is
/// injected at the single call site (keeping the emitter itself testable and
/// clock-free).
///
/// # Errors
///
/// Returns [`GraphEmitError`] if the pipeline cannot be emitted to the C20
/// contract (a node without stable names, or a malformed stable name); returns an
/// [`std::io::Error`] if the sink write fails. Neither is an environment-resource
/// failure — there is none to fail on.
pub fn graph_verb<W: std::io::Write>(
    pipeline: &Pipeline,
    pipeline_name: &str,
    now_rfc3339: &str,
    sink: &mut W,
) -> Result<(), GraphVerbError> {
    let provenance = BuildProvenance::embedded();
    let artifact = emit_graph(pipeline, pipeline_name, now_rfc3339, &provenance)?;
    sink.write_all(artifact.as_bytes())?;
    sink.write_all(b"\n")?;
    Ok(())
}

/// The outcome of the [`graph_verb`]: either a structural emit failure
/// ([`GraphEmitError`]) or a sink write failure ([`std::io::Error`]).
#[derive(Debug)]
pub enum GraphVerbError {
    /// The pipeline could not be emitted to the C20 contract.
    Emit(GraphEmitError),
    /// Writing the artifact to the sink failed.
    Io(std::io::Error),
}

impl fmt::Display for GraphVerbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Emit(e) => write!(f, "graph emission failed: {e}"),
            Self::Io(e) => write!(f, "writing the graph artifact failed: {e}"),
        }
    }
}

impl std::error::Error for GraphVerbError {}

impl From<GraphEmitError> for GraphVerbError {
    fn from(e: GraphEmitError) -> Self {
        Self::Emit(e)
    }
}

impl From<std::io::Error> for GraphVerbError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

/// Build the graph-artifact JSON [`Value`] (the structure [`emit_graph`]
/// serializes). Separated so tests can inspect the parsed structure directly and
/// validate it against the published schema.
///
/// # Errors
///
/// See [`emit_graph`].
pub fn build_artifact(
    pipeline: &Pipeline,
    pipeline_name: &str,
    generated_at: &str,
    provenance: &BuildProvenance,
) -> Result<Value, GraphEmitError> {
    // The C21 fingerprint (T41) is computed once from the assembled pipeline
    // through dagr-core's public reuse surface — this emitter never re-derives the
    // composition, and the same digests reach the run artifact (C22) and resume
    // (C27) from the same place.
    let fingerprint = pipeline.fingerprint();
    let header = build_header(pipeline_name, generated_at, provenance, &fingerprint);

    // Nodes in deterministic, registration-order-independent order. `Pipeline`
    // already iterates by identity name (a total, stable key), which is exactly
    // the canonical node ordering (T0.7 §6).
    let mut nodes = Vec::new();
    for node in pipeline.nodes() {
        nodes.push(build_node(pipeline, node)?);
    }

    // Edges in canonical `(from, to, kind)` order (T0.7 §6), independent of
    // registration order.
    let edges = build_edges(pipeline)?;

    Ok(json!({
        "header": header,
        "nodes": Value::Array(nodes),
        "edges": Value::Array(edges),
    }))
}

/// Blank the [generation-time field](GENERATED_AT_FIELD) in a parsed artifact so
/// two artifacts can be compared for byte-identity **outside** that field (C20:
/// generation time is the only field allowed to vary). Returns the value with
/// `header.generated_at` set to the empty string; every other byte is untouched.
#[must_use]
pub fn mask_generated_at(mut artifact: Value) -> Value {
    if let Some(header) = artifact.get_mut("header").and_then(Value::as_object_mut) {
        header.insert(GENERATED_AT_FIELD.into(), Value::from(""));
    }
    artifact
}

/// Build the versioned header (C20 / C21). Everything but `generated_at` is fixed
/// per binary; the two fingerprints depend only on author-declared inputs, so
/// they too are identical across emissions from any machine or toolchain (T0.7
/// §5).
fn build_header(
    pipeline_name: &str,
    generated_at: &str,
    provenance: &BuildProvenance,
    fingerprint: &FingerprintSlot,
) -> Value {
    json!({
        "schema_version": GRAPH_SCHEMA_VERSION,
        "tool_version": provenance.tool_version(),
        GENERATED_AT_FIELD: generated_at,
        "pipeline": pipeline_name,
        "build_provenance": {
            "tool_version": provenance.tool_version(),
            "git_commit": provenance.git_commit(),
            "lockfile_hash": provenance.lockfile_hash(),
        },
        // The COMPUTED C21 fingerprints (T41): the structural fingerprint, the
        // policy hash (each a self-describing, version-prefixed string), and the
        // algorithm version. Every input is author-declared, so these exclude
        // generation time, provenance, and everything else environmental (T0.7 §5).
        "fingerprint_structural": format_fingerprint_structural(fingerprint),
        "fingerprint_policy": format_fingerprint_policy(fingerprint),
        "fingerprint_algorithm_version": fingerprint.algorithm_version(),
    })
}

/// Format the **structural fingerprint** header string from a computed
/// [`FingerprintSlot`] — `fnv1a-64:v<algorithm_version>:<16-hex-digits>` (C21 /
/// T41). The `v<version>` segment carries the [algorithm
/// version](dagr_core::FINGERPRINT_ALGORITHM_VERSION) inside the value as well as
/// in the dedicated header integer, so a version mismatch is legible from the
/// string alone. Exposed so a consumer (a test, the run artifact C22, resume C27)
/// can reproduce the exact header value from a slot without re-deriving the
/// composition.
#[must_use]
pub fn format_fingerprint_structural(slot: &FingerprintSlot) -> String {
    format_digest(slot.algorithm_version(), slot.structural())
}

/// Format the **policy-hash** header string from a computed [`FingerprintSlot`]
/// (same shape as [`format_fingerprint_structural`]).
#[must_use]
pub fn format_fingerprint_policy(slot: &FingerprintSlot) -> String {
    format_digest(slot.algorithm_version(), slot.policy())
}

/// The self-describing, version-prefixed digest string
/// `fnv1a-64:v<version>:<16-hex-digits>`. Lower-hex, zero-padded to the full
/// 64-bit width so the string is fixed-length and byte-stable.
fn format_digest(algorithm_version: u64, digest: u64) -> String {
    format!("{FINGERPRINT_HASH_FAMILY}:v{algorithm_version}:{digest:016x}")
}

/// Build one node's artifact object (C20): identity name, group, stable
/// task/input/output names, effective execution class, complete effective policy
/// (defaults written out), declared resources, and dependency list.
fn build_node(pipeline: &Pipeline, node: &PipelineNode) -> Result<Value, GraphEmitError> {
    let names = node
        .stable_names()
        .ok_or_else(|| GraphEmitError::MissingStableNames {
            node: node.name().to_string(),
        })?;

    // Enforce the whole-pipeline stable-name well-formedness rule (T0.7 §1): every
    // recorded task/input/output stable name must be well-formed (or the reserved
    // unit sentinel for the output). A malformed name is a hard emit error.
    validate_stable_name(node.name(), names.task())?;
    for input in names.inputs() {
        validate_stable_name(node.name(), input)?;
    }
    if names.output() != UNIT_STABLE_NAME {
        validate_stable_name(node.name(), names.output())?;
    }

    let policy = node.effective_policy();
    let dependencies = dependency_names(pipeline, node);

    Ok(json!({
        "name": node.name(),
        // The group label is presentation metadata (C6) — recorded, but in neither
        // fingerprint. Absent → the empty string (the schema types `group` as a
        // plain string).
        "group": node.group().unwrap_or(""),
        "task_name": names.task(),
        "input_type_names": Value::Array(
            names.inputs().iter().map(|n| Value::from(*n)).collect(),
        ),
        "output_type_name": names.output(),
        "execution_class": execution_class_name(policy.execution_class()),
        "policy": build_policy(&policy),
        "resources": build_resources(&policy),
        "dependencies": Value::Array(
            dependencies.into_iter().map(Value::from).collect(),
        ),
        // `type_name` (the informational debug field, T0.7 §1) is deliberately
        // NOT populated: this emitter records only author-declared stable names as
        // identity, and never emits a `type_name` value that could be mistaken for
        // one. The field stays reserved (optional in the schema).
    }))
}

/// Validate one recorded stable name against the whole-pipeline well-formedness
/// rule (T0.7 §1).
fn validate_stable_name(node: &str, value: &str) -> Result<(), GraphEmitError> {
    if is_well_formed(value) {
        Ok(())
    } else {
        Err(GraphEmitError::MalformedStableName {
            node: node.to_string(),
            value: value.to_string(),
        })
    }
}

/// Build the **complete effective policy** object (C5 / C20): every C5 field
/// written out with its resolved value, defaults included. A no-policy node and an
/// all-defaults node produce byte-identical policy blocks (C5), because both
/// resolve to the same [`EffectivePolicy`].
fn build_policy(policy: &EffectivePolicy) -> Value {
    let cost = policy.cost();
    let backoff = policy.backoff();
    json!({
        "retries": policy.retry_count(),
        "backoff": {
            "base_ms": duration_ms(backoff.base()),
            // The growth factor is a config f64; emit its raw IEEE-754 bits as an
            // integer so the value is deterministic and integer-only (T4 §6 — no
            // float formatting), matching how the fingerprint encoding treats it.
            "factor_bits": backoff.factor().to_bits(),
            "cap_ms": duration_ms(backoff.cap()),
        },
        // The no-timeout default is written out as an explicit `null` timeout so an
        // all-default node's policy block equals the every-default-written-out form.
        "timeout_ms": policy.timeout().map_or(Value::Null, |d| Value::from(duration_ms(d))),
        "cost": {
            "working_memory_bytes": cost.working_memory(),
            "output_residency_bytes": cost.output_residency(),
            "blocking_threads": cost.blocking_threads(),
            "compute_threads": cost.compute_threads(),
        },
        "execution_class": execution_class_name(policy.execution_class()),
        "trigger_rule": trigger_rule_name(policy.trigger_rule()),
        "retained": policy.is_retained(),
        "durable": policy.is_durable(),
    })
}

/// Build the **declared resource requirements** object (C5/C9/C20): the per-pool
/// cost vector in each pool's native unit — bytes for the memory pool (split into
/// working memory and output residency) and a thread count for each thread pool.
/// So bootstrap and the run artifact can juxtapose declared against measured cost.
fn build_resources(policy: &EffectivePolicy) -> Value {
    let cost = policy.cost();
    json!({
        "working_memory_bytes": cost.working_memory(),
        "output_residency_bytes": cost.output_residency(),
        "blocking_threads": cost.blocking_threads(),
        "compute_threads": cost.compute_threads(),
    })
}

/// The dependency names of `node` — the identity names of its upstream nodes, in
/// deterministic (sorted, deduplicated) order. Covers **both** data and ordering
/// upstreams (C4 / T50): a node ordered after another depends on it (it runs after
/// it), so the ordering upstream belongs in the dependency list, even though no
/// value flows.
fn dependency_names(pipeline: &Pipeline, node: &PipelineNode) -> Vec<String> {
    let mut deps: Vec<String> = node
        .data_edges()
        .iter()
        .filter_map(|edge| pipeline.node(edge.upstream()).map(|n| n.name().to_string()))
        .chain(
            node.ordering_edges()
                .iter()
                .filter_map(|edge| pipeline.node(edge.upstream()).map(|n| n.name().to_string())),
        )
        .collect();
    deps.sort();
    deps.dedup();
    deps
}

/// Build the edge array (C20): one edge per dependency, **data** edges tagged
/// `data` and carrying the stable name of the type they carry; **ordering** edges
/// (C4 / T50) tagged `ordering` and carrying **no** type. Emitted in canonical
/// `(from, to, kind)` order, independent of registration order (T0.7 §6).
///
/// The two kinds are recorded distinctly: a data edge's entry has a `type_name`
/// field (the producer's stable output type), an ordering edge's entry has none.
fn build_edges(pipeline: &Pipeline) -> Result<Vec<Value>, GraphEmitError> {
    // Collect (from, to, kind, carried-type) tuples, then sort by (from, to, kind)
    // for a total, registration-order-independent order.
    let mut edges: Vec<(String, String, &'static str, Option<String>)> = Vec::new();
    for node in pipeline.nodes() {
        for edge in node.data_edges() {
            let Some(producer) = pipeline.node(edge.upstream()) else {
                continue;
            };
            // A data edge carries the stable name of the value type it carries —
            // which is the producer's declared stable OUTPUT type name (the value
            // flowing along the edge). A producer therefore must carry stable names
            // for its outgoing data edges to be emittable.
            let carried = producer
                .stable_names()
                .ok_or_else(|| GraphEmitError::MissingStableNames {
                    node: producer.name().to_string(),
                })?
                .output();
            validate_stable_name(producer.name(), carried)?;
            debug_assert_eq!(edge.kind(), EdgeKind::Data);
            edges.push((
                producer.name().to_string(),
                node.name().to_string(),
                "data",
                Some(carried.to_string()),
            ));
        }
        // Ordering edges (C4 / T50): sequence-only, no value, so no carried type.
        // The upstream needs NO stable names — nothing flows along the edge, so an
        // ordering upstream that lacks stable output names is still emittable (only
        // a DATA edge requires its producer's stable output type).
        for edge in node.ordering_edges() {
            let Some(producer) = pipeline.node(edge.upstream()) else {
                continue;
            };
            debug_assert_eq!(edge.kind(), EdgeKind::Ordering);
            edges.push((
                producer.name().to_string(),
                node.name().to_string(),
                "ordering",
                None,
            ));
        }
    }
    edges.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)).then(a.2.cmp(b.2)));

    Ok(edges
        .into_iter()
        .map(|(from, to, kind, carried)| {
            let mut obj = Map::new();
            obj.insert("from".into(), Value::from(from));
            obj.insert("to".into(), Value::from(to));
            obj.insert("kind".into(), Value::from(kind));
            if let Some(carried) = carried {
                // A DATA edge records the stable name of the type carried; an
                // ORDERING edge would carry none (schema: `type_name` required only
                // for `kind == "data"`).
                obj.insert("type_name".into(), Value::from(carried));
            }
            Value::Object(obj)
        })
        .collect())
}

/// The stable string name of an [`ExecutionClass`] recorded in the artifact
/// (arch.md C13). Fixed, author-independent, byte-stable.
fn execution_class_name(class: ExecutionClass) -> &'static str {
    match class {
        ExecutionClass::AwaitBound => "await-bound",
        ExecutionClass::Blocking => "blocking",
        ExecutionClass::Compute => "compute",
    }
}

/// The normative string name of a [`TriggerRule`] (arch.md Vocabulary; matches the
/// schema enum `all-succeeded | all-terminal | any-failed`).
fn trigger_rule_name(rule: TriggerRule) -> &'static str {
    match rule {
        TriggerRule::AllSucceeded => "all-succeeded",
        TriggerRule::AllTerminal => "all-terminal",
        TriggerRule::AnyFailed => "any-failed",
    }
}

/// A [`std::time::Duration`] as whole milliseconds, saturating — a total,
/// deterministic, integer scalar for the artifact (T4 §6; no float formatting).
/// `Duration::MAX` (the effectively-uncapped backoff cap) saturates to
/// [`u64::MAX`], a fixed sentinel that encodes identically every time.
fn duration_ms(d: std::time::Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

// A compile-time cross-check that the computed algorithm version is non-zero (the
// schema requires `fingerprint_algorithm_version >= 1`). A future accidental zero
// is then a build error, not a validation failure at emit time.
const _: () = assert!(FINGERPRINT_ALGORITHM_VERSION >= 1);
