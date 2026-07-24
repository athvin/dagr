//! C20 · Graph artifact emission — ticket T40. Written first, TDD.
//!
//! These translate the T40 Test plan into executable tests against the **real**
//! emitter [`dagr_cli::graph`] over a **real** assembled [`Pipeline`] built with
//! the stable-name-aware registrars. Each test maps to one arch.md C20 acceptance
//! criterion (see the per-test doc comment).
//!
//! The **schema round-trip** — the load-bearing interlock with T39 (a real
//! emitted artifact validates against `schemas/graph/v1.schema.json` via the
//! published helper, and a corrupted copy is rejected) — lives in the sibling
//! `graph_artifact_schema_roundtrip.rs`, gated behind the `schema-validation`
//! feature so the CI-/dev-scoped `jsonschema` validator is pulled only by CI's
//! dedicated step (mirroring T39), never by the shipped binary or the bare
//! `cargo test --workspace`.

use dagr_cli::graph::{
    emit_graph, graph_verb, mask_generated_at, BuildProvenance, GraphEmitError,
    GRAPH_SCHEMA_VERSION,
};
use dagr_core::stable_name::StableName;
use dagr_core::task::{ExecutionClass, RunContext, Task};
use dagr_core::{Flow, NodePolicy, Pipeline, TaskError};
use serde_json::Value;

// === Fixture value + task types (author-declared stable names) =============

struct Rows;
impl StableName for Rows {
    const STABLE_NAME: &'static str = "Rows";
}
struct Schema;
impl StableName for Schema {
    const STABLE_NAME: &'static str = "Schema";
}
struct Report;
impl StableName for Report {
    const STABLE_NAME: &'static str = "Report";
}

/// A sourceless task producing `Rows`, in the `ingest` group, with a non-default
/// policy and a declared cost vector — exercises group + non-default policy +
/// resource cost (Node-completeness / declared-resources plan items).
struct LoadRows;
impl StableName for LoadRows {
    // The stable TASK name deliberately DIFFERS from the Rust type identifier, to
    // prove the recorded name is the author-declared one, never `type_name`.
    const STABLE_NAME: &'static str = "load-rows-task";
}
impl Task for LoadRows {
    type Input = ();
    type Output = Rows;
    const EXECUTION_CLASS: ExecutionClass = ExecutionClass::Compute;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Rows, TaskError> {
        Ok(Rows)
    }
}

/// A sourceless task producing `Schema`, all-default policy.
struct LoadSchema;
impl StableName for LoadSchema {
    const STABLE_NAME: &'static str = "LoadSchema";
}
impl Task for LoadSchema {
    type Input = ();
    type Output = Schema;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Schema, TaskError> {
        Ok(Schema)
    }
}

/// A two-input task consuming `(Rows, Schema)` and producing `Report`.
struct BuildReport;
impl StableName for BuildReport {
    const STABLE_NAME: &'static str = "BuildReport";
}
impl Task for BuildReport {
    type Input = (Rows, Schema);
    type Output = Report;
    async fn run(&mut self, _c: &RunContext, _i: (Rows, Schema)) -> Result<Report, TaskError> {
        Ok(Report)
    }
}

// === Fixtures ==============================================================

/// A three-node fixture pipeline: two sources (`load` with a non-default policy
/// and a cost vector, in the `ingest` group; `schema` all-default) feeding a
/// two-input `report` node. Two data edges (Rows and Schema into report).
fn fixture_pipeline() -> Pipeline {
    let mut flow = Flow::new();
    let rows = flow.register_source_named::<LoadRows>(
        "load",
        &LoadRows,
        Some("ingest"),
        NodePolicy::new()
            .retries(2)
            .working_memory(4096)
            .output_residency(1024)
            .compute_threads(3),
    );
    let schema = flow.register_source_named::<LoadSchema>(
        "schema",
        &LoadSchema,
        None::<String>,
        NodePolicy::new(),
    );
    let _report = flow.register_named::<BuildReport, _>(
        "report",
        &BuildReport,
        (rows, schema),
        None::<String>,
        NodePolicy::new(),
    );
    flow.finish()
}

fn test_provenance() -> BuildProvenance {
    BuildProvenance::new(
        "0.0.0",
        "0123456789abcdef0123456789abcdef01234567",
        "fnv1a-64:0011223344556677",
    )
}

const GEN_A: &str = "2026-07-23T00:00:00Z";
const GEN_B: &str = "2026-07-23T12:34:56Z";

fn emit(pipeline: &Pipeline, generated_at: &str) -> String {
    emit_graph(
        pipeline,
        "example-pipeline",
        generated_at,
        &test_provenance(),
    )
    .expect("fixture pipeline emits")
}

fn parse(json: &str) -> Value {
    serde_json::from_str(json).expect("emitted artifact is valid JSON")
}

// === Tests =================================================================

/// **Empty-environment emission (C20).** Emission returns a complete artifact
/// with no env vars, no filesystem fixtures, no network, no credentials, and no
/// parameters — assembly is pure (C7), and this emitter reads nothing beyond the
/// assembled pipeline and the injected generation time.
#[test]
fn emission_succeeds_in_an_empty_environment() {
    // No environment is consulted: the only inputs are the in-memory pipeline, a
    // fixed provenance value, and an injected timestamp. Prove success.
    let pipeline = fixture_pipeline();
    let out = emit_graph(&pipeline, "example-pipeline", GEN_A, &test_provenance())
        .expect("emits with no environment present");
    let artifact = parse(&out);
    assert_eq!(
        artifact["header"]["pipeline"],
        Value::from("example-pipeline")
    );
    assert_eq!(artifact["nodes"].as_array().unwrap().len(), 3);
}

/// **Byte-identical repeat (C20).** Emitting twice from the same binary produces
/// identical bytes after masking only the generation-time field — including
/// header provenance and node/edge ordering.
#[test]
fn emitting_twice_is_byte_identical_outside_generation_time() {
    let pipeline = fixture_pipeline();
    let a = emit(&pipeline, GEN_A);
    let b = emit(&pipeline, GEN_A);
    assert_eq!(a, b, "two emissions with the same clock are byte-identical");

    // With DIFFERENT clocks, mask the generation-time field and the rest matches.
    let a = emit(&pipeline, GEN_A);
    let b = emit(&pipeline, GEN_B);
    assert_ne!(a, b, "different generation times differ somewhere");
    let masked_a = mask_generated_at(parse(&a));
    let masked_b = mask_generated_at(parse(&b));
    assert_eq!(
        masked_a, masked_b,
        "outside the generation-time field, the two artifacts are identical"
    );
}

/// **Generation time is the only variance (C20).** Two emissions with two
/// different instants differ **only** within the generation-time field; every
/// other field (including provenance and ordering) is byte-for-byte identical.
#[test]
fn generation_time_is_the_only_field_that_varies() {
    let pipeline = fixture_pipeline();
    let a = parse(&emit(&pipeline, GEN_A));
    let b = parse(&emit(&pipeline, GEN_B));

    // The generation-time field itself carries each supplied instant.
    assert_eq!(a["header"]["generated_at"], Value::from(GEN_A));
    assert_eq!(b["header"]["generated_at"], Value::from(GEN_B));
    // Everything else is identical.
    assert_eq!(mask_generated_at(a), mask_generated_at(b));
}

/// **Node completeness (C20).** Every assembled node appears exactly once, each
/// carrying name, group, stable task name, stable input/output type names,
/// execution class, and dependency lists; none is missing and none is invented.
#[test]
fn every_node_appears_once_with_its_complete_fields() {
    let pipeline = fixture_pipeline();
    let artifact = parse(&emit(&pipeline, GEN_A));
    let nodes = artifact["nodes"].as_array().unwrap();
    assert_eq!(nodes.len(), 3, "exactly the three assembled nodes");

    let by_name: std::collections::HashMap<&str, &Value> = nodes
        .iter()
        .map(|n| (n["name"].as_str().unwrap(), n))
        .collect();
    assert_eq!(by_name.len(), 3, "no node appears twice");

    let load = by_name["load"];
    assert_eq!(load["group"], Value::from("ingest"));
    assert_eq!(load["task_name"], Value::from("load-rows-task"));
    assert_eq!(load["input_type_names"], serde_json::json!([]));
    assert_eq!(load["output_type_name"], Value::from("Rows"));
    assert_eq!(load["execution_class"], Value::from("compute"));
    assert_eq!(load["dependencies"], serde_json::json!([]));

    let report = by_name["report"];
    assert_eq!(report["task_name"], Value::from("BuildReport"));
    assert_eq!(
        report["input_type_names"],
        serde_json::json!(["Rows", "Schema"]),
        "input type names recorded in declaration order"
    );
    assert_eq!(report["output_type_name"], Value::from("Report"));
    let deps = report["dependencies"].as_array().unwrap();
    let deps: Vec<&str> = deps.iter().map(|d| d.as_str().unwrap()).collect();
    assert!(deps.contains(&"load") && deps.contains(&"schema"));
    assert_eq!(deps.len(), 2);

    // A schema-only node with no group records the empty group label.
    assert_eq!(by_name["schema"]["group"], Value::from(""));
}

/// **Full effective policy including defaults (C5 / C20).** An all-default node's
/// policy block equals the every-default-written-out form; an overridden node
/// shows the overridden values. Neither omits a field.
#[test]
fn full_effective_policy_is_written_out_with_defaults() {
    let pipeline = fixture_pipeline();
    let artifact = parse(&emit(&pipeline, GEN_A));
    let by_name: std::collections::HashMap<&str, &Value> = artifact["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|n| (n["name"].as_str().unwrap(), n))
        .collect();

    // The all-default `schema` node writes out every C5 field at its default.
    let p = &by_name["schema"]["policy"];
    for field in [
        "retries",
        "backoff",
        "timeout_ms",
        "cost",
        "execution_class",
        "trigger_rule",
        "retained",
        "durable",
    ] {
        assert!(
            p.get(field).is_some(),
            "default policy writes out field `{field}`"
        );
    }
    assert_eq!(p["retries"], Value::from(0));
    assert_eq!(
        p["timeout_ms"],
        Value::Null,
        "no-timeout default written out"
    );
    assert_eq!(p["trigger_rule"], Value::from("all-succeeded"));
    assert_eq!(p["retained"], Value::from(false));
    assert_eq!(p["durable"], Value::from(false));
    assert_eq!(p["execution_class"], Value::from("await-bound"));

    // The overridden `load` node shows its overridden values.
    let lp = &by_name["load"]["policy"];
    assert_eq!(lp["retries"], Value::from(2));
    assert_eq!(lp["execution_class"], Value::from("compute"));
}

/// **Declared resource requirements present (C9 / C20).** A node's declared cost
/// vector appears in native units with distinct working-memory, output-residency,
/// and thread entries, matching what was declared.
#[test]
fn declared_resource_requirements_are_present_in_native_units() {
    let pipeline = fixture_pipeline();
    let artifact = parse(&emit(&pipeline, GEN_A));
    let load = artifact["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|n| n["name"] == "load")
        .unwrap();
    let r = &load["resources"];
    assert_eq!(r["working_memory_bytes"], Value::from(4096));
    assert_eq!(r["output_residency_bytes"], Value::from(1024));
    assert_eq!(r["compute_threads"], Value::from(3));
    assert_eq!(r["blocking_threads"], Value::from(0));
}

/// **Edge kinds and carried types (C20).** A data edge is tagged `data` and
/// records the stable declared name of its carried payload type; every data edge
/// the runtime would use is present. (Ordering edges are T50; none exist yet.)
#[test]
fn data_edges_are_tagged_and_carry_the_stable_type_name() {
    let pipeline = fixture_pipeline();
    let artifact = parse(&emit(&pipeline, GEN_A));
    let edges = artifact["edges"].as_array().unwrap();
    assert_eq!(edges.len(), 2, "two data edges into report");

    // The Rows edge (load -> report) carries the stable payload name "Rows".
    let rows_edge = edges.iter().find(|e| e["from"] == "load").unwrap();
    assert_eq!(rows_edge["to"], "report");
    assert_eq!(rows_edge["kind"], "data");
    assert_eq!(rows_edge["type_name"], "Rows");

    let schema_edge = edges.iter().find(|e| e["from"] == "schema").unwrap();
    assert_eq!(schema_edge["kind"], "data");
    assert_eq!(schema_edge["type_name"], "Schema");

    // Every edge is a data edge here (no ordering edges yet — T50).
    assert!(edges.iter().all(|e| e["kind"] == "data"));
}

/// **Stable declared names, never `type_name` as identity (C20 / C21).** Recorded
/// task and type names are the author-declared stable names — `load-rows-task`
/// differs from its Rust type identifier `LoadRows`. No `type_name` value is used
/// as identity, and the informational `type_name` debug field is not populated by
/// the emitter.
#[test]
fn recorded_names_are_stable_declared_names_never_type_name() {
    let pipeline = fixture_pipeline();
    let artifact = parse(&emit(&pipeline, GEN_A));
    let load = artifact["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|n| n["name"] == "load")
        .unwrap();
    // The recorded stable task name is the author-declared one, not the Rust path.
    assert_eq!(load["task_name"], "load-rows-task");
    assert!(
        !load["task_name"].as_str().unwrap().contains("LoadRows"),
        "the stable name is author-declared, not the Rust type identifier"
    );
    // The emitter populates no informational `type_name` field (it is reserved and
    // never used as identity).
    assert!(
        load.get("type_name").is_none(),
        "the emitter emits no load-bearing type_name; it is a reserved debug field"
    );
}

/// **Build-provenance header embedded (C20 / Stability).** The header carries the
/// schema version, tool version, pipeline identity, the reserved fingerprint /
/// algorithm-version slots, and build provenance (tool version, git commit,
/// lockfile hash) fixed for the binary and identical across repeated emissions.
#[test]
fn header_carries_the_versioned_provenance_slots() {
    let pipeline = fixture_pipeline();
    let a = parse(&emit(&pipeline, GEN_A));
    let h = &a["header"];
    assert_eq!(h["schema_version"], Value::from(GRAPH_SCHEMA_VERSION));
    assert_eq!(h["tool_version"], Value::from("0.0.0"));
    assert_eq!(h["pipeline"], Value::from("example-pipeline"));
    // Reserved fingerprint slots present (values are T41's).
    assert!(h.get("fingerprint_structural").is_some());
    assert!(h.get("fingerprint_policy").is_some());
    assert!(h["fingerprint_algorithm_version"].as_u64().unwrap() >= 1);
    // Build provenance embedded.
    let bp = &h["build_provenance"];
    assert_eq!(bp["tool_version"], Value::from("0.0.0"));
    assert_eq!(
        bp["git_commit"],
        Value::from("0123456789abcdef0123456789abcdef01234567")
    );
    assert_eq!(
        bp["lockfile_hash"],
        Value::from("fnv1a-64:0011223344556677")
    );

    // The real embedded provenance is fixed per binary: two calls agree.
    let one = BuildProvenance::embedded();
    let two = BuildProvenance::embedded();
    assert_eq!(one.tool_version(), two.tool_version());
    assert_eq!(one.git_commit(), two.git_commit());
    assert_eq!(one.lockfile_hash(), two.lockfile_hash());
    assert!(!one.tool_version().is_empty());
    assert!(!one.git_commit().is_empty());
    assert!(!one.lockfile_hash().is_empty());
}

/// **Two in-process assemblies agree — interlock with T15 (C7 / C20).** Assembling
/// the same pipeline definition twice in one process and emitting each yields
/// byte-identical artifacts outside the generation-time field — emission adds no
/// nondeterminism on top of assembly, regardless of registration order.
#[test]
fn two_in_process_assemblies_emit_identically() {
    let a = emit(&fixture_pipeline(), GEN_A);
    let b = emit(&fixture_pipeline(), GEN_A);
    assert_eq!(a, b, "two independent assemblies emit byte-identically");

    // Registration order does not change the artifact (the node/edge canonical
    // ordering is by name / (from, to, kind), never by registration order).
    let reordered = {
        let mut flow = Flow::new();
        // Register `schema` before `load` — the opposite order.
        let schema = flow.register_source_named::<LoadSchema>(
            "schema",
            &LoadSchema,
            None::<String>,
            NodePolicy::new(),
        );
        let rows = flow.register_source_named::<LoadRows>(
            "load",
            &LoadRows,
            Some("ingest"),
            NodePolicy::new()
                .retries(2)
                .working_memory(4096)
                .output_residency(1024)
                .compute_threads(3),
        );
        let _report = flow.register_named::<BuildReport, _>(
            "report",
            &BuildReport,
            (rows, schema),
            None::<String>,
            NodePolicy::new(),
        );
        flow.finish()
    };
    assert_eq!(
        emit(&reordered, GEN_A),
        a,
        "registration order does not change the emitted artifact"
    );
}

/// **Graph verb reachable, needs no store or parameters (C26 / C7).** The graph
/// verb writes the artifact to an arbitrary sink, opening no run store and reading
/// no parameters — the inspection-verb guarantee.
#[test]
fn graph_verb_writes_to_a_sink_with_no_store_or_parameters() {
    let pipeline = fixture_pipeline();
    let mut buf: Vec<u8> = Vec::new();
    graph_verb(&pipeline, "example-pipeline", GEN_A, &mut buf).expect("graph verb emits");
    let text = String::from_utf8(buf).unwrap();
    assert!(text.ends_with('\n'), "the verb writes a trailing newline");
    // The verb produced a complete, well-formed artifact (schema validation of the
    // verb's output is exercised in the feature-gated round-trip suite).
    let artifact = parse(text.trim_end());
    assert_eq!(artifact["header"]["schema_version"], GRAPH_SCHEMA_VERSION);
    assert_eq!(artifact["nodes"].as_array().unwrap().len(), 3);

    // The real embedded provenance is fixed per binary, so two verb emissions at
    // the same instant agree byte-for-byte (no store, no parameters, no clock
    // beyond the injected instant).
    let mut buf2: Vec<u8> = Vec::new();
    graph_verb(&pipeline, "example-pipeline", GEN_A, &mut buf2).expect("second verb emit");
    let mut buf3: Vec<u8> = Vec::new();
    graph_verb(&pipeline, "example-pipeline", GEN_A, &mut buf3).expect("third verb emit");
    assert_eq!(
        buf2, buf3,
        "two verb emissions at one instant are byte-identical"
    );
}

/// **A node without stable names is not emittable (C20).** The C20 contract
/// requires author-declared stable names as identity; a node registered through a
/// type-erased registrar (no stable names captured) produces a clear error naming
/// the node — never a silent `type_name` fallback.
#[test]
fn a_node_without_stable_names_fails_emission() {
    struct Plain;
    impl Task for Plain {
        type Input = ();
        type Output = ();
        async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<(), TaskError> {
            Ok(())
        }
    }
    let mut flow = Flow::new();
    // Type-erased registrar: no stable names captured.
    let _h = flow.register_source::<Plain>("plain", &Plain);
    let pipeline = flow.finish();

    let err = emit_graph(&pipeline, "p", GEN_A, &test_provenance())
        .expect_err("a stable-name-less node is not emittable to C20");
    assert_eq!(
        err,
        GraphEmitError::MissingStableNames {
            node: "plain".into()
        },
        "the error names the offending node"
    );
}

/// **A malformed stable name fails emission (C20 / T0.7 §1).** A recorded stable
/// name that violates the well-formedness rule (whitespace, control chars, or
/// out-of-set punctuation) is rejected with an error naming the node and value.
#[test]
fn a_malformed_stable_name_fails_emission() {
    struct BadPayload;
    impl StableName for BadPayload {
        // A space is not in the well-formed set.
        const STABLE_NAME: &'static str = "bad name";
    }
    struct BadTask;
    impl StableName for BadTask {
        const STABLE_NAME: &'static str = "BadTask";
    }
    impl Task for BadTask {
        type Input = ();
        type Output = BadPayload;
        async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<BadPayload, TaskError> {
            Ok(BadPayload)
        }
    }
    let mut flow = Flow::new();
    let _h =
        flow.register_source_named::<BadTask>("bad", &BadTask, None::<String>, NodePolicy::new());
    let pipeline = flow.finish();

    let err = emit_graph(&pipeline, "p", GEN_A, &test_provenance())
        .expect_err("a malformed stable name is rejected");
    assert_eq!(
        err,
        GraphEmitError::MalformedStableName {
            node: "bad".into(),
            value: "bad name".into(),
        },
        "the error names the node and the malformed value"
    );
}

/// **A no-policy node and an all-defaults-written-out node emit identical policy
/// blocks (C5 / C20).** Two nodes — one registered with `NodePolicy::new()`, one
/// with every default explicitly set — produce byte-identical policy JSON.
#[test]
fn no_policy_and_written_out_defaults_emit_identical_policy_blocks() {
    fn build(policy: NodePolicy) -> Value {
        let mut flow = Flow::new();
        let _h = flow.register_source_named::<LoadSchema>("n", &LoadSchema, None::<String>, policy);
        let pipeline = flow.finish();
        let artifact = parse(&emit(&pipeline, GEN_A));
        artifact["nodes"][0]["policy"].clone()
    }
    let defaulted = build(NodePolicy::new());
    let written_out = build(
        NodePolicy::new()
            .retries(0)
            .timeout_off()
            .retained(false)
            .durable(false),
    );
    assert_eq!(
        defaulted, written_out,
        "no-policy and every-default-written-out emit identical policy blocks (C5)"
    );
}

/// **The checked-in T40 corpus fixture is a real emitted artifact (C20 / T0.10).**
/// `tests/fixtures/corpus/graph/v1/t40-three-node.json` must be exactly what this
/// emitter produces for the three-node fixture (generation-time field aside), so
/// the corpus example never drifts from the emitter and the T39/T48 corpus walker
/// validates a genuine emission rather than a hand-written approximation.
#[test]
fn checked_in_corpus_fixture_matches_a_real_emission() {
    let fixture_path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../tests/fixtures/corpus/graph/v1/t40-three-node.json"
    );
    let fixture: Value = serde_json::from_str(
        &std::fs::read_to_string(fixture_path).expect("corpus fixture is checked in"),
    )
    .expect("corpus fixture is valid JSON");

    let emitted = parse(&emit(&fixture_pipeline(), GEN_A));
    assert_eq!(
        mask_generated_at(fixture),
        mask_generated_at(emitted),
        "the corpus fixture equals a real emission outside the generation-time field"
    );
}
