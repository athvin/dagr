//! Positive (compiles == the assertion) fixtures for the T5 (018) typed-handle
//! + dependency-encoding spike.
//!
//! These are the compile-PASS half of the ticket's evidence. They live in a
//! normal integration test (compiled by `cargo test --workspace`), NOT in
//! [`tests/ui/`](./ui), because the T8 UI harness ([`tests/ui.rs`](./ui.rs))
//! asserts every `tests/ui/*.rs` sample FAILS to compile — a positive sample
//! there would break it. Their **compilation is the assertion**: a future
//! regression that made any of these shapes stop compiling would fail the build,
//! exactly as the ticket's definition of done asks (handles freely copyable;
//! single input takes `T` not `(T,)`; one handle fans out to many consumers).
//!
//! THROWAWAY, illustrative types — NOT dagr's real authoring API. The real typed
//! handle lands in T10, the real data-dependency binding in T11, and the real
//! flow builder / node identity in T13. This file only pins the settled T5
//! ENCODING so those tickets adopt it. Names mirror the ADR embedded in
//! `docs/implementation/018-T5-typed-handle-encoding-spike.md`.

use std::marker::PhantomData;
use std::rc::Rc;

/// A node's identity token (mirrors C2 · Handle: identity comes from the node's
/// declared name — T0.7 — not from its value type or registration order).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct NodeId(u32);

/// A typed handle (C2): node identity plus the value's type. `PhantomData<fn()
/// -> T>` keeps the handle `Copy + Send + Sync` REGARDLESS of `T` (the naive
/// `PhantomData<T>` would infect the handle with `T`'s auto-traits), while still
/// carrying the value type at compile time.
struct Handle<T> {
    id: NodeId,
    value: PhantomData<fn() -> T>,
}
// Manual impls so `T: Copy` is NOT required — a derive would over-constrain.
impl<T> Clone for Handle<T> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<T> Copy for Handle<T> {}

/// A type-erased ordering upstream (C4/T0.9): identity only, value type dropped.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct Ordering(NodeId);

impl<T> Handle<T> {
    fn ordering(self) -> Ordering {
        Ordering(self.id)
    }
}

/// Sealed positional binding (C3): a private trait maps a handle tuple to the
/// task's declared input tuple so COUNT, ORDER, and TYPES are all compile-
/// checked. A single input is the bare value `A`, never a one-tuple `(A,)`.
trait Deps {
    type Inputs;
    /// Consumes the dep-set, yielding the upstream node identities.
    fn into_ids(self) -> Vec<NodeId>;
}
impl<A> Deps for Handle<A> {
    type Inputs = A; // single-input ergonomics: `T`, not `(T,)`
    fn into_ids(self) -> Vec<NodeId> {
        vec![self.id]
    }
}
impl<A, B> Deps for (Handle<A>, Handle<B>) {
    type Inputs = (A, B);
    fn into_ids(self) -> Vec<NodeId> {
        vec![self.0.id, self.1.id]
    }
}

trait Task {
    type Input;
    type Output;
}

struct Flow {
    registered: u32,
}
impl Flow {
    fn new() -> Self {
        Self { registered: 0 }
    }
    fn mint<Out>(&mut self) -> Handle<Out> {
        let id = NodeId(self.registered);
        self.registered += 1;
        Handle {
            id,
            value: PhantomData,
        }
    }
    /// Register a data-dependent node: the `deps` value types must EXACTLY match
    /// the task's declared `Input` (the `Inputs = T::Input` bound), and `deps`
    /// is consumed to record the upstream identities.
    fn register<T, D>(&mut self, _task: &T, deps: D) -> Handle<T::Output>
    where
        T: Task,
        D: Deps<Inputs = T::Input>,
    {
        let ids = deps.into_ids();
        assert!(!ids.is_empty());
        self.mint()
    }
    /// A sourceless (consume-nothing) node: no `Deps` argument at all.
    fn source<T: Task<Input = ()>>(&mut self, _task: &T) -> Handle<T::Output> {
        self.mint()
    }
}

// --- Concrete illustrative value + task types ---
struct Gamma;
struct Rows;
struct Alpha;
struct Beta;
struct Bytes;

struct MakeGamma;
impl Task for MakeGamma {
    type Input = ();
    type Output = Gamma;
}
struct MakeAlpha;
impl Task for MakeAlpha {
    type Input = ();
    type Output = Alpha;
}
struct MakeBeta;
impl Task for MakeBeta {
    type Input = ();
    type Output = Beta;
}
/// Consumes a SINGLE `Gamma` (Input = `Gamma`, NOT `(Gamma,)`).
struct ConsumeGamma;
impl Task for ConsumeGamma {
    type Input = Gamma;
    type Output = Rows;
}
/// Consumes exactly two inputs.
struct ConsumeTwo;
impl Task for ConsumeTwo {
    type Input = (Alpha, Beta);
    type Output = Bytes;
}

fn assert_copy<T: Copy>() {}
fn assert_send<T: Send>() {}
fn assert_sync<T: Sync>() {}

fn passthrough<T>(handle: Handle<T>) -> Handle<T> {
    let taken = handle; // a Copy, not a move
    let reused = handle; // original still usable after a copy was taken
                         // Both the copy and the reuse remain live — the point of Copy handles.
    let _ = reused;
    taken
}

/// Handles are freely COPYABLE and passable during construction, even for a
/// value type that is itself `!Send + !Sync + !Copy` — the fn-pointer phantom
/// keeps the handle unconditionally cheap and thread-safe (C2).
#[test]
fn handles_are_freely_copyable() {
    // `Rc<String>` is deliberately NOT Send/Sync/Copy — the adversarial `T`.
    assert_copy::<Handle<Rc<String>>>();
    assert_send::<Handle<Rc<String>>>();
    assert_sync::<Handle<Rc<String>>>();

    let mut flow = Flow::new();
    let handle: Handle<Alpha> = flow.source(&MakeAlpha);
    let copy = handle; // Copy, not move
    let out = passthrough(handle); // pass a copy into and out of a helper
    let still_usable = handle; // original still usable afterward
    assert_eq!(copy.id, still_usable.id);
    assert_eq!(out.id, handle.id);
}

/// A single-input task consumes `T` directly — NO tuple wrapping at the call
/// site (resolves the single-input-ergonomics open question). The companion
/// rejected form `(Gamma,)` is documented in the ADR as unnecessary.
#[test]
fn single_input_takes_t_not_one_tuple() {
    let mut flow = Flow::new();
    let gamma: Handle<Gamma> = flow.source(&MakeGamma);
    // Exactly one handle, bound as the bare value — not `(gamma,)`.
    let rows: Handle<Rows> = flow.register(&ConsumeGamma, gamma);
    assert_eq!(rows.id, NodeId(1));
}

/// One producer handle fans out to any number of downstream consumers whose
/// declared input type matches; the same handle is reused freely (C3).
#[test]
fn fan_out_one_handle_many_consumers() {
    let mut flow = Flow::new();
    let gamma: Handle<Gamma> = flow.source(&MakeGamma);
    let first = flow.register(&ConsumeGamma, gamma);
    let second = flow.register(&ConsumeGamma, gamma);
    let third = flow.register(&ConsumeGamma, gamma);
    // The producer handle is still usable after all three bindings.
    let fourth = flow.register(&ConsumeGamma, gamma);
    assert_eq!(
        [first.id, second.id, third.id, fourth.id],
        [NodeId(1), NodeId(2), NodeId(3), NodeId(4)]
    );
}

/// A correct multi-input binding (exact arity + exact types) compiles, and an
/// ordering edge from a handle of ANY value type is accepted (type-erased).
#[test]
fn multi_input_and_type_erased_ordering() {
    let mut flow = Flow::new();
    let alpha: Handle<Alpha> = flow.source(&MakeAlpha);
    let beta: Handle<Beta> = flow.source(&MakeBeta);
    let gamma: Handle<Gamma> = flow.source(&MakeGamma);
    let bytes: Handle<Bytes> = flow.register(&ConsumeTwo, (alpha, beta));
    assert_eq!(bytes.id, NodeId(3));
    // An ordering upstream drops the value type: a `Handle<Gamma>` and a
    // `Handle<Alpha>` both erase to the same `Ordering` type.
    let erased: [Ordering; 2] = [gamma.ordering(), alpha.ordering()];
    assert_eq!(erased, [Ordering(NodeId(2)), Ordering(NodeId(0))]);
}
