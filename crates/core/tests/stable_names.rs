//! Stable-name trait and stable-name capture — ticket T40 (the first consumer of
//! the T0.7 stable-name contract). Written first, TDD.
//!
//! These exercise the `dagr_core::stable_name` surface (the author-declared
//! [`StableName`] constant, the [`is_well_formed`] well-formedness predicate, and
//! the positional [`StableInputNames`] input-name resolution) and the flow
//! builder's stable-name capture (`register_source_named` / `register_named`
//! populate a node's [`StableTypeNames`]; the type-erased registrars leave it
//! `None`).

use dagr_core::stable_name::{is_well_formed, StableInputNames, StableName, UNIT_STABLE_NAME};
use dagr_core::task::{RunContext, Task};
use dagr_core::{Flow, NodePolicy, TaskError};

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

struct MakeRows;
impl StableName for MakeRows {
    const STABLE_NAME: &'static str = "make-rows";
}
impl Task for MakeRows {
    type Input = ();
    type Output = Rows;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Rows, TaskError> {
        Ok(Rows)
    }
}
struct MakeSchema;
impl StableName for MakeSchema {
    const STABLE_NAME: &'static str = "MakeSchema";
}
impl Task for MakeSchema {
    type Input = ();
    type Output = Schema;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Schema, TaskError> {
        Ok(Schema)
    }
}
struct Build;
impl StableName for Build {
    const STABLE_NAME: &'static str = "Build";
}
impl Task for Build {
    type Input = (Rows, Schema);
    type Output = Report;
    async fn run(&mut self, _c: &RunContext, _i: (Rows, Schema)) -> Result<Report, TaskError> {
        Ok(Report)
    }
}

/// The stable name is the **author-declared** constant, matching the compiler-
/// enforced type — never `std::any::type_name`.
#[test]
fn stable_name_is_the_author_declared_constant() {
    assert_eq!(Rows::STABLE_NAME, "Rows");
    // The task's declared name may deliberately differ from its Rust identifier.
    assert_eq!(MakeRows::STABLE_NAME, "make-rows");
}

/// Well-formedness: non-empty, ASCII letters/digits and `_ - . :`; whitespace and
/// control chars are rejected. The unit sentinel is a reserved exception.
#[test]
fn well_formedness_accepts_the_documented_shape_and_rejects_the_rest() {
    assert!(is_well_formed("Rows"));
    assert!(is_well_formed("crate::Rows"));
    assert!(is_well_formed("row-count_v2.0"));
    assert!(!is_well_formed(""), "empty is malformed");
    assert!(!is_well_formed("bad name"), "whitespace is malformed");
    assert!(!is_well_formed("bad\tname"), "tab is malformed");
    assert!(!is_well_formed("bad\nname"), "newline is malformed");
    assert!(!is_well_formed("bad/name"), "slash is not in the set");
    // The unit sentinel is deliberately NOT well-formed under the general rule;
    // it is handled as a reserved name by the artifact builder.
    assert!(!is_well_formed(UNIT_STABLE_NAME));
    assert_eq!(UNIT_STABLE_NAME, "()");
}

/// `StableInputNames` resolves the ordered stable input type names of a
/// data-dependent node: one entry for a single payload, one per position for a
/// tuple in declaration order. (A source consumes nothing, so its empty input
/// list is recorded directly by the registrar and never routes through here.)
#[test]
fn stable_input_names_resolve_positionally() {
    assert_eq!(
        <Rows as StableInputNames>::stable_input_names(),
        vec!["Rows"]
    );
    assert_eq!(
        <(Rows, Schema) as StableInputNames>::stable_input_names(),
        vec!["Rows", "Schema"]
    );
}

/// The stable-name-aware source registrar captures the task/output stable names
/// (empty input list for a source); the output is the produced payload's name.
#[test]
fn register_source_named_captures_task_and_output_names() {
    let mut flow = Flow::new();
    let h = flow.register_source_named::<MakeRows>(
        "rows",
        &MakeRows,
        None::<String>,
        NodePolicy::new(),
    );
    let pipeline = flow.finish();
    let node = pipeline.resolve(h).expect("registered node");
    let names = node.stable_names().expect("stable names captured");
    assert_eq!(names.task(), "make-rows");
    assert!(names.inputs().is_empty(), "a source consumes nothing");
    assert_eq!(names.output(), "Rows");
}

/// The stable-name-aware data registrar captures the task name, the ordered input
/// type names of the bound inputs, and the output name.
#[test]
fn register_named_captures_input_and_output_names_in_order() {
    let mut flow = Flow::new();
    let rows = flow.register_source_named::<MakeRows>(
        "rows",
        &MakeRows,
        None::<String>,
        NodePolicy::new(),
    );
    let schema = flow.register_source_named::<MakeSchema>(
        "schema",
        &MakeSchema,
        None::<String>,
        NodePolicy::new(),
    );
    let report = flow.register_named::<Build, _>(
        "report",
        &Build,
        (rows, schema),
        None::<String>,
        NodePolicy::new(),
    );
    let pipeline = flow.finish();
    let node = pipeline.resolve(report).expect("registered node");
    let names = node.stable_names().expect("stable names captured");
    assert_eq!(names.task(), "Build");
    assert_eq!(names.inputs(), &["Rows", "Schema"]);
    assert_eq!(names.output(), "Report");
}

/// A node registered through a **type-erased** registrar carries no stable names,
/// so the C20 emitter can distinguish emittable from non-emittable nodes.
#[test]
fn type_erased_registrar_captures_no_stable_names() {
    let mut flow = Flow::new();
    let h = flow.register_source::<MakeRows>("rows", &MakeRows);
    let pipeline = flow.finish();
    let node = pipeline.resolve(h).expect("registered node");
    assert!(
        node.stable_names().is_none(),
        "type-erased registration captures no stable names"
    );
}

/// A `()`-output effect node records the reserved unit sentinel as its output
/// name rather than an absent field.
#[test]
fn unit_output_records_the_reserved_sentinel() {
    struct Effect;
    impl StableName for Effect {
        const STABLE_NAME: &'static str = "Effect";
    }
    impl Task for Effect {
        type Input = ();
        type Output = ();
        async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<(), TaskError> {
            Ok(())
        }
    }
    // `()` implements `StableName` via the reserved sentinel so `T::Output: StableName`
    // is satisfied for an effect node.
    let mut flow = Flow::new();
    let h =
        flow.register_source_named::<Effect>("effect", &Effect, None::<String>, NodePolicy::new());
    let pipeline = flow.finish();
    let names = pipeline.resolve(h).unwrap().stable_names().unwrap();
    assert_eq!(names.output(), UNIT_STABLE_NAME);
}
