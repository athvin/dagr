// UI compile-failure fixture — ticket T5 (018), case `typed_handle_data_cycle`.
//
// PROVES (C2): a cycle via DATA edges is INEXPRESSIBLE by CONSTRUCTION —
// structural, never a later or runtime validation pass. Because a handle is
// obtained ONLY by registering a node, and `register` accepts only already-
// existing handles, no expression can name a node that is not yet registered.
// Binding node B's handle as an input to node A, when A is registered BEFORE B,
// cannot be written: B's handle does not exist yet at A's registration point, so
// it is a use of an undeclared binding (E0425). This is the data-edge half of
// C2's "an attempt to express a cycle — through data edges or ordering edges —
// fails to compile" (the ordering-edge half is the T0.9 fixtures).
//
// Wired to the T8 UI harness (crates/core/tests/ui.rs); the sibling `.stderr`
// names the substrings the diagnostic must contain, and the harness asserts
// this sample FAILS to compile under the pinned toolchain (C28).
//
// THROWAWAY, intentionally NON-COMPILING SKETCH — NOT dagr's real authoring API
// (typed handles land in T10, the real binding in T11). It models only the
// settled T5 backward-reference registration discipline shared with C4 ordering
// edges.

use std::marker::PhantomData;

#[derive(Clone, Copy)]
struct NodeId(u32);

struct Handle<T> {
    id: NodeId,
    _t: PhantomData<fn() -> T>,
}
impl<T> Clone for Handle<T> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<T> Copy for Handle<T> {}

trait Deps {
    type Inputs;
}
impl<A> Deps for Handle<A> {
    type Inputs = A;
}
trait Task {
    type Input;
    type Output;
}
struct Flow {
    next: u32,
}
impl Flow {
    // `register` binds a Deps of ALREADY-EXISTING handles and RETURNS the node's
    // handle — the only way to refer to it afterward. The returned handle cannot
    // be referenced inside its own binding expression.
    fn register<T, D>(&mut self, _task: T, _deps: D) -> Handle<T::Output>
    where
        T: Task,
        D: Deps<Inputs = T::Input>,
    {
        let id = NodeId(self.next);
        self.next += 1;
        Handle {
            id,
            _t: PhantomData,
        }
    }
}

struct V;
struct NodeTask;
impl Task for NodeTask {
    type Input = V;
    type Output = V;
}

fn main() {
    let mut flow = Flow { next: 0 };
    // Attempt a cycle: make A depend on B's handle, but B is registered AFTER A.
    // `b` is not yet bound where A's argument is evaluated — a use of an
    // undeclared binding. The cycle cannot be EXPRESSED; no runtime check exists.
    let a = flow.register(NodeTask, b);
    let b = flow.register(NodeTask, a);
    let _ = (a, b);
}
