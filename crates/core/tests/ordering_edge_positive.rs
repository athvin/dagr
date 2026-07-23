//! Positive (compiles) ordering-edge fixtures — ticket T0.9 (015).
//!
//! These are the POSITIVE counterparts to the two compile-fail cycle fixtures in
//! [`tests/ui/`](./ui): where those prove a misuse fails to compile, THIS file's
//! very compilation is the assertion. It cannot live in `tests/ui/` — the T8 UI
//! harness ([`tests/ui.rs`](./ui.rs)) asserts every `tests/ui/*.rs` sample FAILS
//! to compile, so a positive sample there would break it. A normal integration
//! test compiled by `cargo test --workspace` is the right home: if a future
//! regression accidentally couples the value type into ordering, or gives an
//! ordering-only node a bound value, this file fails to compile and the change
//! fails review — exactly the guard T0.9's Test plan asks for.
//!
//! THROWAWAY SKETCHES, NOT dagr's real authoring API. Handles, the flow builder,
//! ordering-edge declaration, and the ordering-only "receives no value" shape
//! are IMPLEMENTED by T13 (builder / node identity) and T50 (ordering edges).
//! These sketches model only the settled C4 mechanics this ADR locks:
//!
//!   * `ordering_edge_any_value_type_ok` — an ordering edge is TYPE-ERASED: a
//!     handle of ANY `T` is an acceptable ordering upstream, and mixing ordering
//!     upstreams of DIFFERENT value types is legal (arch.md C4).
//!   * `data_plus_ordering_and_ordering_only` — a node may carry BOTH a data
//!     dependency and additional ordering edges, and a node attached ONLY by
//!     ordering edges RECEIVES NO VALUE (its body input is `()`), distinguishing
//!     it from a data-dependent node whose body sees the bound value (arch.md C4).

use std::marker::PhantomData;

/// A node's identity (mirrors C2 · Handle: identity comes from the node, not its
/// value type). An ordering edge references this identity, never the value type.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct NodeId(u32);

/// A typed claim on a value that does not exist yet (mirrors C2 · Handle). The
/// handle carries the node's IDENTITY (`id`) and the value type `T`; the value
/// type is IGNORED by ordering edges.
#[derive(Clone, Copy)]
struct Handle<T> {
    id: NodeId,
    _value: PhantomData<T>,
}

/// A type-erased ordering upstream. It keeps the node's IDENTITY but drops the
/// value type: an ordering edge constrains SEQUENCE, not DATA (C4). This is what
/// makes mixing ordering upstreams of different value types legal.
#[derive(Clone, Copy)]
struct OrderingHandle(NodeId);

impl OrderingHandle {
    /// The identity of the node this ordering edge references — the value type
    /// is gone, but the identity that determines *sequence* survives.
    fn id(self) -> NodeId {
        self.0
    }
}

impl<T> Handle<T> {
    /// Any typed handle yields a type-erased ordering upstream. The node's
    /// identity survives; the value type `T` is dropped.
    fn ordering(self) -> OrderingHandle {
        OrderingHandle(self.id)
    }
}

/// A throwaway flow that models the registration-time backward-reference
/// discipline: every entry point takes already-existing handles, and returns a
/// new handle (the only way to refer to the node afterward). `registered`
/// counts nodes so registration visibly mutates the flow, as it must.
struct Flow {
    registered: u32,
}

impl Flow {
    fn new() -> Self {
        Self { registered: 0 }
    }

    /// Register a node: mint a fresh identity and return its typed handle (the
    /// only way to refer to the node afterward).
    fn mint<Out>(&mut self) -> Handle<Out> {
        let id = NodeId(self.registered);
        self.registered += 1;
        Handle {
            id,
            _value: PhantomData,
        }
    }

    /// A sourceless node: no upstreams, produces a typed value.
    fn source<Out>(&mut self, _body: impl Fn(()) -> Out) -> Handle<Out> {
        self.mint()
    }

    /// A node registered against type-erased ordering upstreams only. Because
    /// the upstreams are `OrderingHandle`, their originating value types are
    /// irrelevant — any `T` is accepted, mixed types are fine.
    fn register_ordering<Out>(
        &mut self,
        _ordering_upstreams: &[OrderingHandle],
        _body: impl Fn(()) -> Out,
    ) -> Handle<Out> {
        self.mint()
    }

    /// A node with a DATA dependency plus additional ordering edges: its body
    /// sees the bound value `In`; the extra ordering edges are type-erased.
    fn register_data<In: Copy, Out>(
        &mut self,
        _data: Handle<In>,
        _extra_ordering: &[OrderingHandle],
        _body: impl Fn(In) -> Out,
    ) -> Handle<Out> {
        self.mint()
    }

    /// A node attached ONLY by ordering edges: NO typed input; its body input is
    /// `()` — it RECEIVES NO VALUE (C4).
    fn register_ordering_only<Out>(
        &mut self,
        _ordering: &[OrderingHandle],
        _body: impl Fn(()) -> Out,
    ) -> Handle<Out> {
        self.mint()
    }
}

#[derive(Clone, Copy)]
struct Rows;
#[derive(Clone, Copy)]
struct Bytes;
#[derive(Clone, Copy)]
struct Payload;

/// `ordering_edge_any_value_type_ok`: any value type is an acceptable ordering
/// upstream, and mixing distinct value types is legal. The assertion IS that
/// this compiles; the runtime body is trivial.
#[test]
fn ordering_edge_any_value_type_ok() {
    let mut flow = Flow::new();

    // Two upstreams producing DISTINCT value types.
    let rows: Handle<Rows> = flow.source(|()| Rows);
    let bytes: Handle<Bytes> = flow.source(|()| Bytes);

    // Attach ordering edges from BOTH to one downstream node. Both erase to
    // `OrderingHandle`, so the differing value types do not participate — this
    // compiles precisely because ordering is type-erased.
    let ordering_upstreams = [rows.ordering(), bytes.ordering()];
    let _downstream: Handle<()> = flow.register_ordering(&ordering_upstreams, |()| ());

    // Type-erasure keeps the IDENTITY, not the value type: the two ordering
    // upstreams reference the two distinct source nodes.
    assert_eq!(ordering_upstreams[0].id(), NodeId(0));
    assert_eq!(ordering_upstreams[1].id(), NodeId(1));
    assert_eq!(flow.registered, 3);
}

/// `data_plus_ordering_and_ordering_only`: a node may carry BOTH a data
/// dependency and additional ordering edges; a node attached ONLY by ordering
/// edges receives no value (its body input is `()`).
#[test]
fn data_plus_ordering_and_ordering_only() {
    let mut flow = Flow::new();

    let src: Handle<Payload> = flow.source(|()| Payload);
    let gate: Handle<()> = flow.source(|()| ());

    // Data dependency PLUS an extra ordering edge on the same node: the body
    // sees the bound value (`Payload`), the extra ordering edge is type-erased.
    let _consumer: Handle<()> = flow.register_data(src, &[gate.ordering()], |_payload: Payload| ());

    // Ordering-only node: its body input is `()` — it receives NO value.
    let _cleanup: Handle<()> =
        flow.register_ordering_only(&[src.ordering(), gate.ordering()], |_no_value: ()| ());

    assert_eq!(flow.registered, 4);
}
