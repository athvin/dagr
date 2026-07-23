// UI compile-failure fixture — ticket T12 (024), case `wiring_handle_no_lookup`.
//
// PROVES (C2; arch.md `### C2 · Handle`): there is NO API to retrieve a node's
// output handle by name, index, or string key. This is the REAL authoring
// surface (`dagr_core::flow::Flow` / `dagr_core::flow::Pipeline`): neither the
// builder nor the finalized pipeline exposes a `get(name)` / `lookup(index)` /
// `handle(key)` that conjures a `Handle` — the ONLY route to a handle is a
// registration return value. Attempting such a lookup is a "no method" error
// (E0599), so a by-name/index/string lookup cannot be written. This is the
// "No API exists to retrieve a node's output by name, index, or string key"
// acceptance criterion, and it is why a cycle stays inexpressible: without a
// lookup there is no way to name a node that has not yet been registered.
//
// Wired to the T8 UI harness (crates/core/tests/ui.rs); the sibling `.stderr`
// names the substrings the diagnostic must contain, and the harness asserts this
// sample FAILS to compile under the pinned toolchain (C28).

use dagr_core::flow::Flow;

fn main() {
    let mut flow = Flow::new();
    // No by-name lookup on the builder: `Flow` exposes no `get(name)` that hands
    // back a handle. The only way to a handle is a `register` call's return.
    let _by_name = flow.get("some-node");

    let pipeline = flow.finish();
    // Nor on the finalized pipeline: no `handle(name)` / by-index / by-string
    // route to a `Handle` exists. Node outputs are never addressed by key.
    let _by_key = pipeline.handle("some-node");
}
