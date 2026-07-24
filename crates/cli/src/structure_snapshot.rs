//! C28 · **Structure-snapshot testing** — the middle level of the C28 testing
//! surface (arch.md `### C28 · Testing surface`; ticket T61).
//!
//! This is a **shipped** library API: a pipeline's structure is captured as a
//! canonical, human-readable [`StructureSnapshot`] and asserted against a
//! checked-in **golden fixture** with [`assert_structure`], so an unintended
//! structural change fails review rather than production. The fixture is
//! regenerated deliberately through a single documented **bless** command
//! ([`bless_structure`]). No pipeline writes its own comparison or serialization
//! code — the entire mechanism ships here.
//!
//! # What the snapshot captures — and deliberately excludes
//!
//! The snapshot is a **semantic** comparison surface built directly on the T40
//! C20 graph artifact ([`crate::graph::build_artifact`]): for **every node** its
//! stable identity name, **group** label (C6), stable task/input/output type
//! names (T0.7), effective **execution class**, the **complete effective policy**
//! (C5, every field written out — defaulted values compare identically to
//! written-out defaults), declared resource requirements, and its dependency
//! list; for **every edge** its **kind** (data vs ordering) and, for a data edge,
//! the stable **carried-type name**. Nodes and edges are emitted in the canonical,
//! registration-order-independent order T40 fixes (node name; edge
//! `(from, to, kind)`), so the fixture is byte-stable across builds, machines, and
//! toolchains.
//!
//! It **excludes** the artifact's volatile header entirely — the generation time,
//! the build provenance (tool version, git commit, lockfile hash), and the schema/
//! tool version — so a rebuild or a toolchain bump produces the identical snapshot
//! (arch.md C28: *"does not fail on a rebuild or a toolchain bump"*). The C21
//! fingerprints are **not** part of the compared bytes; they are exposed
//! separately ([`StructureSnapshot::structural_fingerprint`] /
//! [`StructureSnapshot::policy_fingerprint`]) as a companion check, which is what
//! lets a test prove a **group rename** is review-visible in the snapshot diff yet
//! moves neither fingerprint (C6 / C21) — the one place the C6/C28 distinction is
//! shown together.
//!
//! # The failure output is a **structural** diff
//!
//! On a mismatch [`assert_structure`] returns a [`StructureDiff`] — a
//! node-and-edge-oriented report naming exactly what was **added**, **removed**,
//! **renamed**, **rewired**, **regrouped**, or **repolicied** — never a raw text
//! or byte diff.
//!
//! # A limitation, stated at the point of use (C21)
//!
//! **A structure snapshot does not detect a change to a task's *internal logic*
//! that leaves its interface unchanged.** The snapshot (like the C21 fingerprint
//! it rests on) is composed from author-declared names, edges, trigger rules, and
//! policy — never from a task's function body — so a task whose stable name,
//! input/output types, edges, trigger rule, and policy are unchanged snapshots
//! identically even if its body was rewritten. This is a real limitation with no
//! cheap fix in a compiled language. Where node-level change detection is genuinely
//! needed, the honest answer is a **hand-maintained version marker** on the task
//! (a visible, reviewable, obviously-manual constant that *is* part of the task's
//! declared interface and therefore *does* move the structure and the fingerprint)
//! — never an automatic content hash that silently under-detects (arch.md C21;
//! T0.7 §9).
//!
//! # Example
//!
//! A pipeline's entire structure test is two library calls — no bespoke harness:
//!
//! ```no_run
//! use dagr_cli::structure_snapshot::{assert_structure, bless_structure};
//! # fn assembled() -> dagr_core::Pipeline { unimplemented!() }
//! let pipeline = assembled();
//! // One-time (or after a deliberate structural change): bless the fixture.
//! bless_structure(&pipeline, "my-pipeline", "tests/fixtures/my-pipeline.snapshot.json").unwrap();
//! // In the test: assert the current structure against the checked-in golden.
//! assert_structure(&pipeline, "my-pipeline", "tests/fixtures/my-pipeline.snapshot.json")
//!     .expect("pipeline structure matches its golden fixture");
//! ```

use std::collections::BTreeMap;
use std::fmt;
use std::path::Path;

use dagr_core::Pipeline;
use serde_json::Value;

use crate::graph::{build_artifact, BuildProvenance, GraphEmitError};

/// A fixed, never-varying instant used when capturing a snapshot: the snapshot
/// **excludes** the generation-time field, so the value is immaterial — a constant
/// keeps capture clock-free and the snapshot deterministic.
const SNAPSHOT_INSTANT: &str = "1970-01-01T00:00:00Z";

/// A canonical, deterministic, human-readable capture of a pipeline's **structure**
/// (arch.md C28 · structure level), built on the T40 graph artifact.
///
/// Capture one with [`from_pipeline`](Self::from_pipeline); serialize the golden
/// fixture bytes with [`to_canonical_string`](Self::to_canonical_string); compare
/// against a checked-in fixture with the free [`assert_structure`] function. The
/// captured surface — node set (stable names), edge set (carried types + edge
/// kinds), and effective policies, **with** group labels but **without** the
/// volatile header — is described on the [module docs](self).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StructureSnapshot {
    /// The semantic comparison surface: the artifact's `nodes` and `edges`,
    /// header excluded. This is exactly what the fixture serializes and what the
    /// diff is computed over.
    body: Value,
    /// The C21 structural fingerprint, carried for the **companion** check only —
    /// it is not part of the compared body (see the module docs).
    structural_fingerprint: u64,
    /// The C21 policy hash, carried for the companion check only.
    policy_fingerprint: u64,
}

impl StructureSnapshot {
    /// Capture the structure of an assembled `pipeline`, stamped with its
    /// `pipeline_name` identity.
    ///
    /// Clock-free and environment-free: capture reads only the assembled pipeline
    /// (assembly is pure, C7) and excludes the volatile header, so two captures of
    /// the same structure are byte-identical across runs, machines, and toolchains.
    ///
    /// # Errors
    ///
    /// Returns [`GraphEmitError`] if the pipeline is not emittable to the C20
    /// contract — a node lacking author-declared stable names, or a malformed
    /// stable name (the same emittability rule the graph artifact enforces, T40).
    pub fn from_pipeline(pipeline: &Pipeline, pipeline_name: &str) -> Result<Self, GraphEmitError> {
        Self::from_pipeline_with(
            pipeline,
            pipeline_name,
            SNAPSHOT_INSTANT,
            &fixed_provenance(),
        )
    }

    /// Capture the structure with an explicit generation time and build provenance.
    ///
    /// Both are **excluded** from the snapshot — this overload exists only so a
    /// test can prove that a "rebuild" (a different provenance and instant)
    /// produces the identical snapshot. Prefer [`from_pipeline`](Self::from_pipeline)
    /// in normal use.
    ///
    /// # Errors
    ///
    /// See [`from_pipeline`](Self::from_pipeline).
    pub fn from_pipeline_with(
        pipeline: &Pipeline,
        pipeline_name: &str,
        generated_at: &str,
        provenance: &BuildProvenance,
    ) -> Result<Self, GraphEmitError> {
        let artifact = build_artifact(pipeline, pipeline_name, generated_at, provenance)?;
        let fingerprint = pipeline.fingerprint();
        let body = strip_header(artifact);
        Ok(Self {
            body,
            structural_fingerprint: fingerprint.structural(),
            policy_fingerprint: fingerprint.policy(),
        })
    }

    /// The **canonical** golden-fixture bytes: the node/edge structure serialized
    /// through the shared T4 §6 canonicalizer (sorted keys, integer-only scalars,
    /// no locale), with a trailing newline. Byte-stable across builds and machines
    /// and independent of registration order — this is exactly what
    /// [`bless_structure`] writes and what [`assert_structure`] compares.
    #[must_use]
    pub fn to_canonical_string(&self) -> String {
        let mut s = dagr_artifact::canonical::to_canonical_string(&self.body);
        s.push('\n');
        s
    }

    /// The C21 **structural fingerprint** (node set + carried types + edge kinds +
    /// trigger rules) of the captured pipeline. Exposed as a **companion** check —
    /// it is not part of the compared snapshot — so a test can prove a group rename
    /// is review-visible in the diff yet leaves this hash unchanged (C6 / C21).
    #[must_use]
    pub fn structural_fingerprint(&self) -> u64 {
        self.structural_fingerprint
    }

    /// The C21 **policy hash** of the captured pipeline (the companion counterpart
    /// of [`structural_fingerprint`](Self::structural_fingerprint)).
    #[must_use]
    pub fn policy_fingerprint(&self) -> u64 {
        self.policy_fingerprint
    }

    /// Compute the [`StructureDiff`] of this snapshot **against** a `golden`
    /// snapshot (the checked-in fixture). An empty diff ([`StructureDiff::is_empty`])
    /// means the structures match.
    #[must_use]
    pub fn diff(&self, golden: &Self) -> StructureDiff {
        StructureDiff::compute(golden, self)
    }
}

/// The node-and-edge-oriented **structural diff** returned when a snapshot does not
/// match its golden fixture (arch.md C28: the failure output is a structural diff,
/// not a raw text/byte diff).
///
/// It names exactly what changed — nodes **added**/**removed**, edges
/// **added**/**removed** (a rewire or a carried-type change appears as an edge
/// removed and an edge added), and per-node **changed** facets (a rename, a
/// regroup, a policy field, a type name). Render it with [`Display`](fmt::Display)
/// (or [`to_string`](ToString::to_string)) for the human-readable report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StructureDiff {
    /// Node identity names present in the current structure but not the golden.
    added_nodes: Vec<String>,
    /// Node identity names present in the golden but not the current structure.
    removed_nodes: Vec<String>,
    /// Per-node facet changes: `(node, facet, golden_value, current_value)`.
    changed_nodes: Vec<NodeChange>,
    /// Edges present now but not in the golden — rendered `from -kind-> to [type]`.
    added_edges: Vec<String>,
    /// Edges present in the golden but not now.
    removed_edges: Vec<String>,
}

/// One per-node facet that differs between the golden and current structure — a
/// [`StructureDiff`] carries these alongside whole-node add/remove and edge changes.
#[derive(Debug, Clone, PartialEq, Eq)]
struct NodeChange {
    node: String,
    facet: String,
    golden: String,
    current: String,
}

impl StructureDiff {
    /// Whether the two structures match — an empty diff.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.added_nodes.is_empty()
            && self.removed_nodes.is_empty()
            && self.changed_nodes.is_empty()
            && self.added_edges.is_empty()
            && self.removed_edges.is_empty()
    }

    /// Compute the diff of `current` against `golden`, deterministically (every
    /// list is built in the canonical node/edge order the snapshot already carries).
    fn compute(golden: &StructureSnapshot, current: &StructureSnapshot) -> Self {
        let g_nodes = node_map(&golden.body);
        let c_nodes = node_map(&current.body);

        let mut added_nodes = Vec::new();
        let mut removed_nodes = Vec::new();
        let mut changed_nodes = Vec::new();

        for name in c_nodes.keys() {
            if !g_nodes.contains_key(name) {
                added_nodes.push(name.clone());
            }
        }
        for (name, g_node) in &g_nodes {
            match c_nodes.get(name) {
                None => removed_nodes.push(name.clone()),
                Some(c_node) => diff_node(name, g_node, c_node, &mut changed_nodes),
            }
        }

        let g_edges = edge_set(&golden.body);
        let c_edges = edge_set(&current.body);
        let added_edges = c_edges
            .iter()
            .filter(|e| !g_edges.contains(*e))
            .cloned()
            .collect();
        let removed_edges = g_edges
            .iter()
            .filter(|e| !c_edges.contains(*e))
            .cloned()
            .collect();

        Self {
            added_nodes,
            removed_nodes,
            changed_nodes,
            added_edges,
            removed_edges,
        }
    }
}

impl fmt::Display for StructureDiff {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_empty() {
            return write!(f, "structure matches the golden fixture (no diff)");
        }
        writeln!(f, "structure differs from the golden fixture:")?;
        for n in &self.added_nodes {
            writeln!(f, "  + added node `{n}`")?;
        }
        for n in &self.removed_nodes {
            writeln!(f, "  - removed node `{n}`")?;
        }
        for c in &self.changed_nodes {
            writeln!(
                f,
                "  ~ node `{}` {} changed: `{}` -> `{}`",
                c.node, c.facet, c.golden, c.current
            )?;
        }
        for e in &self.added_edges {
            writeln!(f, "  + added edge {e}")?;
        }
        for e in &self.removed_edges {
            writeln!(f, "  - removed edge {e}")?;
        }
        Ok(())
    }
}

/// The failure of a [`assert_structure`] call: either the golden fixture could not
/// be read/written ([`Io`](Self::Io)) or the structure did not match it
/// ([`Mismatch`](Self::Mismatch), carrying the structural diff).
#[derive(Debug)]
pub enum StructureAssertError {
    /// The current structure does not match the golden fixture — carries the
    /// node-and-edge-oriented [`StructureDiff`]. Re-bless deliberately with
    /// [`bless_structure`] if the change is intended.
    Mismatch(StructureDiff),
    /// The pipeline is not emittable to the C20 contract (a node without stable
    /// names, or a malformed stable name — T40).
    Emit(GraphEmitError),
    /// The golden fixture could not be read (for example it does not exist yet —
    /// bless it first). Names the underlying I/O failure.
    Io(std::io::Error),
}

impl fmt::Display for StructureAssertError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Mismatch(diff) => write!(f, "{diff}"),
            Self::Emit(e) => write!(f, "pipeline is not snapshottable: {e}"),
            Self::Io(e) => write!(
                f,
                "could not read the golden structure fixture ({e}); bless it first with \
                 `bless_structure` (or set the update flag)"
            ),
        }
    }
}

impl std::error::Error for StructureAssertError {}

impl From<GraphEmitError> for StructureAssertError {
    fn from(e: GraphEmitError) -> Self {
        Self::Emit(e)
    }
}

impl From<std::io::Error> for StructureAssertError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

/// Assert an assembled `pipeline`'s structure against the golden fixture at
/// `fixture_path` (arch.md C28 · structure level). The whole structure test is
/// this one call — no pipeline writes its own comparison or serialization code.
///
/// Passes with no output when the structure matches; otherwise returns
/// [`StructureAssertError::Mismatch`] carrying a [`StructureDiff`] that names
/// exactly what was added, removed, renamed, rewired, regrouped, or repolicied.
///
/// The comparison is **semantic**: it ignores the volatile header (generation
/// time, build provenance, tool version), so a rebuild or a toolchain bump never
/// fails it, while a group rename **is** review-visible here (C6) even though it
/// never moves the C21 fingerprint. When the structure has legitimately changed,
/// regenerate the fixture with [`bless_structure`].
///
/// **Limitation (C21):** this does not detect a change to a task's *internal
/// logic* that leaves its interface unchanged — see the [module docs](self) for the
/// hand-maintained-version-marker answer.
///
/// # Errors
///
/// - [`StructureAssertError::Mismatch`] — the structure differs from the fixture.
/// - [`StructureAssertError::Emit`] — the pipeline is not snapshottable (T40).
/// - [`StructureAssertError::Io`] — the fixture could not be read (bless it first).
pub fn assert_structure(
    pipeline: &Pipeline,
    pipeline_name: &str,
    fixture_path: impl AsRef<Path>,
) -> Result<(), StructureAssertError> {
    let current = StructureSnapshot::from_pipeline(pipeline, pipeline_name)?;
    let golden_bytes = std::fs::read_to_string(fixture_path.as_ref())?;
    let golden = parse_fixture(&golden_bytes)?;
    let diff = current.diff(&golden);
    if diff.is_empty() {
        Ok(())
    } else {
        Err(StructureAssertError::Mismatch(diff))
    }
}

/// The **bless / update** flow (arch.md C28: *"a single documented command … that
/// rewrites the canonical, stably-ordered fixture for review"*): deliberately
/// (re)generate the golden fixture for `pipeline` at `fixture_path` from the
/// current structure.
///
/// This is the one command that regenerates a fixture. It is **idempotent**:
/// running it twice against an unchanged structure writes byte-identical output.
/// The written bytes are the canonical, stably-ordered serialization
/// ([`StructureSnapshot::to_canonical_string`]), so the fixture is byte-stable
/// across builds, machines, and toolchains and is reviewed in version control like
/// any other checked-in file.
///
/// # Errors
///
/// - [`StructureAssertError::Emit`] — the pipeline is not snapshottable (T40).
/// - [`StructureAssertError::Io`] — the fixture could not be written.
pub fn bless_structure(
    pipeline: &Pipeline,
    pipeline_name: &str,
    fixture_path: impl AsRef<Path>,
) -> Result<(), StructureAssertError> {
    let snapshot = StructureSnapshot::from_pipeline(pipeline, pipeline_name)?;
    std::fs::write(fixture_path.as_ref(), snapshot.to_canonical_string())?;
    Ok(())
}

// === internals =============================================================

/// The fixed provenance used when capturing a snapshot — its values are excluded
/// from the snapshot, so the constants are immaterial; they exist only to satisfy
/// the T40 emitter's signature without probing the build environment.
fn fixed_provenance() -> BuildProvenance {
    BuildProvenance::new("", "", "")
}

/// Strip the volatile header from a graph artifact, leaving exactly the semantic
/// comparison surface `{nodes, edges}` (arch.md C28: volatile header fields —
/// generation time, build provenance, tool/schema version — are excluded).
fn strip_header(mut artifact: Value) -> Value {
    if let Some(obj) = artifact.as_object_mut() {
        obj.remove("header");
    }
    artifact
}

/// Parse a golden fixture's bytes back into a [`StructureSnapshot`] for comparison.
/// The fingerprints are not carried in the fixture (they are a companion check, not
/// part of the compared body), so they are recorded as unknown (`0`) — they are
/// never read on the golden side of a diff.
fn parse_fixture(bytes: &str) -> Result<StructureSnapshot, StructureAssertError> {
    let body: Value = serde_json::from_str(bytes).map_err(|e| {
        StructureAssertError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("golden structure fixture is not valid JSON: {e}"),
        ))
    })?;
    Ok(StructureSnapshot {
        body,
        structural_fingerprint: 0,
        policy_fingerprint: 0,
    })
}

/// The nodes of a snapshot body, keyed by identity name (the canonical key).
fn node_map(body: &Value) -> BTreeMap<String, Value> {
    let mut map = BTreeMap::new();
    if let Some(nodes) = body.get("nodes").and_then(Value::as_array) {
        for node in nodes {
            if let Some(name) = node.get("name").and_then(Value::as_str) {
                map.insert(name.to_string(), node.clone());
            }
        }
    }
    map
}

/// The set of edges of a snapshot body, each rendered as a stable, human-readable
/// key `from -kind-> to [carried-type]`, so a rewire or a carried-type change reads
/// as one edge removed and one edge added.
fn edge_set(body: &Value) -> std::collections::BTreeSet<String> {
    let mut set = std::collections::BTreeSet::new();
    if let Some(edges) = body.get("edges").and_then(Value::as_array) {
        for edge in edges {
            let from = edge.get("from").and_then(Value::as_str).unwrap_or("?");
            let to = edge.get("to").and_then(Value::as_str).unwrap_or("?");
            let kind = edge.get("kind").and_then(Value::as_str).unwrap_or("?");
            let carried = edge
                .get("type_name")
                .and_then(Value::as_str)
                .map(|t| format!(" carrying `{t}`"))
                .unwrap_or_default();
            set.insert(format!("`{from}` -{kind}-> `{to}`{carried}"));
        }
    }
    set
}

/// Diff two node records (identity name equal), appending one [`NodeChange`] per
/// differing facet — group (regroup), stable task/type names, execution class,
/// dependency list, and each effective-policy field. Deterministic: facets are
/// walked in a fixed order.
fn diff_node(name: &str, golden: &Value, current: &Value, out: &mut Vec<NodeChange>) {
    // Scalar/string facets compared directly.
    for facet in ["group", "task_name", "output_type_name", "execution_class"] {
        push_if_differs(name, facet, golden.get(facet), current.get(facet), out);
    }
    // Composite facets rendered canonically so a change is legible.
    for facet in ["input_type_names", "dependencies", "resources", "policy"] {
        push_if_differs(name, facet, golden.get(facet), current.get(facet), out);
    }
    // For a policy change, additionally name the specific changed field(s) so the
    // diff points at exactly the repolicied value (e.g. `retries`), not just
    // "policy".
    if let (Some(gp), Some(cp)) = (
        golden.get("policy").and_then(Value::as_object),
        current.get("policy").and_then(Value::as_object),
    ) {
        // Walk the union of policy field names deterministically, naming each field
        // whose value differs (e.g. `retries`).
        let mut fields: std::collections::BTreeSet<&str> = gp.keys().map(String::as_str).collect();
        fields.extend(cp.keys().map(String::as_str));
        for field in fields {
            let gv = gp.get(field);
            let cv = cp.get(field);
            if gv != cv {
                out.push(NodeChange {
                    node: name.to_string(),
                    facet: format!("policy.{field}"),
                    golden: render(gv),
                    current: render(cv),
                });
            }
        }
    }
}

/// Push a [`NodeChange`] when a facet's golden and current values differ.
fn push_if_differs(
    name: &str,
    facet: &str,
    golden: Option<&Value>,
    current: Option<&Value>,
    out: &mut Vec<NodeChange>,
) {
    if golden != current {
        out.push(NodeChange {
            node: name.to_string(),
            facet: facet.to_string(),
            golden: render(golden),
            current: render(current),
        });
    }
}

/// Render a JSON value canonically for the diff report (absent → `<none>`).
fn render(value: Option<&Value>) -> String {
    match value {
        None => "<none>".to_string(),
        Some(v) => dagr_artifact::canonical::to_canonical_string(v),
    }
}
