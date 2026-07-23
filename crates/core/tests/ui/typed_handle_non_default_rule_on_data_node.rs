// UI compile-failure fixture — ticket T5 (018),
// case `typed_handle_non_default_rule_on_data_node`.
//
// PROVES (C3, Vocabulary): a node that carries a DATA dependency cannot be given
// any trigger rule other than `all-succeeded` — the builder TYPESTATE makes it
// INEXPRESSIBLE, a COMPILE error rather than a runtime check. The builder starts
// in a `ConsumesNothing` state where `.trigger_rule(..)` IS offered; binding a
// data dependency transitions the typestate to `ConsumesData`, a state that
// deliberately offers NO `trigger_rule` method. Calling it there is a "no method
// in this state" error (E0599). This is the compile-time enforcement of
// arch.md's "Data-dependent nodes always use `all-succeeded` (C3), and that
// restriction is enforced at compile time."
//
// Wired to the T8 UI harness (crates/core/tests/ui.rs); the sibling `.stderr`
// names the substrings the diagnostic must contain, and the harness asserts
// this sample FAILS to compile under the pinned toolchain (C28).
//
// THROWAWAY, intentionally NON-COMPILING SKETCH — NOT dagr's real authoring API
// (the real flow builder / node policy lands in T11/T13). It models only the
// settled T5 typestate decision. The trigger-rule NAMES are the normative
// Vocabulary set (`all-succeeded` default, `all-terminal`, `any-failed`).

#![allow(dead_code)]

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

// The closed, normative trigger-rule set (arch.md Vocabulary).
enum TriggerRule {
    AllSucceeded,
    AllTerminal,
    AnyFailed,
}

// Typestate markers: whether the node so far consumes a value.
struct ConsumesNothing;
struct ConsumesData;

struct Builder<S> {
    id: NodeId,
    _s: PhantomData<S>,
}

// A non-default trigger rule is settable ONLY on a consume-nothing node.
impl Builder<ConsumesNothing> {
    fn trigger_rule(self, _rule: TriggerRule) -> Self {
        self
    }
    // Binding a data dependency transitions the typestate to `ConsumesData`.
    fn depends_on<T>(self, _upstream: Handle<T>) -> Builder<ConsumesData> {
        Builder {
            id: self.id,
            _s: PhantomData,
        }
    }
}
impl Builder<ConsumesData> {
    // Deliberately NO `trigger_rule` here — the restriction IS the absence of
    // the method in this state.
    fn finish(self) -> Handle<()> {
        Handle {
            id: self.id,
            _t: PhantomData,
        }
    }
}

fn main() {
    // An upstream handle (stands in for one a `register`/`finish` call returned).
    let upstream: Handle<u32> = Handle {
        id: NodeId(0),
        _t: PhantomData,
    };

    // A data-dependent node tries to set a NON-DEFAULT rule. After `.depends_on`
    // the builder is `Builder<ConsumesData>`, which offers no `trigger_rule`
    // method — a compile error, not a runtime check.
    let _node = Builder::<ConsumesNothing> {
        id: NodeId(1),
        _s: PhantomData,
    }
    .depends_on(upstream)
    .trigger_rule(TriggerRule::AllTerminal);
}
