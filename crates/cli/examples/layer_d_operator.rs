//! **Layer D — the developer and operator surface** (arch.md "Layer D"; ticket
//! T64).
//!
//! The surfaces that make a pipeline *operable* and *reviewable*. This example
//! shows the C28 **structure snapshot**: a pipeline's structure is captured as a
//! canonical, semantic fingerprint, and a structural change (adding a node,
//! rewiring an edge) is caught as a diff — so an unintended rewiring fails review
//! rather than production (system criterion 6). It uses the public
//! [`StructureSnapshot`](dagr_cli::structure_snapshot::StructureSnapshot) API and
//! needs no run store, network, or database (pure assembly, C7). Run it with
//! `cargo run --example layer_d_operator`.

use dagr_cli::structure_snapshot::StructureSnapshot;
use dagr_core::context::RunContext;
use dagr_core::stable_name::StableName;
use dagr_core::task::Task;
use dagr_core::{Flow, NodePolicy, Pipeline, TaskError};

// Author-declared stable names (C20): recorded in the structure, stable across
// toolchains (unlike `std::any::type_name`).
struct Count;
impl StableName for Count {
    const STABLE_NAME: &'static str = "Count";
}
struct Doubled;
impl StableName for Doubled {
    const STABLE_NAME: &'static str = "Doubled";
}
struct Published;
impl StableName for Published {
    const STABLE_NAME: &'static str = "Published";
}

struct Load;
impl StableName for Load {
    const STABLE_NAME: &'static str = "load";
}
impl Task for Load {
    type Input = ();
    type Output = Count;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Count, TaskError> {
        Ok(Count)
    }
}
struct Transform;
impl StableName for Transform {
    const STABLE_NAME: &'static str = "transform";
}
impl Task for Transform {
    type Input = Count;
    type Output = Doubled;
    async fn run(&mut self, _c: &RunContext, _i: Count) -> Result<Doubled, TaskError> {
        Ok(Doubled)
    }
}
struct Publish;
impl StableName for Publish {
    const STABLE_NAME: &'static str = "publish";
}
impl Task for Publish {
    type Input = Doubled;
    type Output = Published;
    async fn run(&mut self, _c: &RunContext, _i: Doubled) -> Result<Published, TaskError> {
        Ok(Published)
    }
}

/// The baseline pipeline: `load -> transform`.
fn baseline() -> Pipeline {
    let mut flow = Flow::new();
    let loaded = flow.register_source_named("load", &Load, None::<String>, NodePolicy::new());
    let _t = flow.register_named(
        "transform",
        &Transform,
        loaded,
        None::<String>,
        NodePolicy::new(),
    );
    flow.finish()
}

/// The same pipeline with one node added: `load -> transform -> publish`.
fn with_added_node() -> Pipeline {
    let mut flow = Flow::new();
    let loaded = flow.register_source_named("load", &Load, None::<String>, NodePolicy::new());
    let transformed = flow.register_named(
        "transform",
        &Transform,
        loaded,
        None::<String>,
        NodePolicy::new(),
    );
    let _p = flow.register_named(
        "publish",
        &Publish,
        transformed,
        None::<String>,
        NodePolicy::new(),
    );
    flow.finish()
}

fn main() {
    let name = "example-pipeline";

    // Snapshot the baseline structure — a canonical, semantic fingerprint over the
    // node set, edge set, and effective policies (volatile header fields excluded).
    let base_snapshot =
        StructureSnapshot::from_pipeline(&baseline(), name).expect("baseline snapshots");

    // A rebuild of the same pipeline snapshots identically — the structure test
    // does not flap on a rebuild or a toolchain bump.
    let rebuilt = StructureSnapshot::from_pipeline(&baseline(), name).expect("rebuild snapshots");
    assert!(
        base_snapshot.diff(&rebuilt).is_empty(),
        "an unchanged pipeline shows no structural diff"
    );
    println!("baseline vs rebuild: no diff (stable under rebuild)");

    // Adding a node is a structural change — the diff names it, so a reviewer sees
    // exactly what changed (system criterion 6). This is what catches an
    // unintended rewiring in code review.
    let added_snapshot =
        StructureSnapshot::from_pipeline(&with_added_node(), name).expect("added snapshots");
    let diff = base_snapshot.diff(&added_snapshot);
    assert!(
        !diff.is_empty(),
        "adding a node produces a non-empty structural diff"
    );
    println!("baseline vs added-node: DIFF present\n{diff}");

    // The structural change also moves the structural fingerprint; a policy-only
    // change would move only the policy hash (C21).
    assert_ne!(
        base_snapshot.structural_fingerprint(),
        added_snapshot.structural_fingerprint(),
        "adding a node changes the structural fingerprint"
    );
    println!("structural fingerprint changed, as expected");
}
