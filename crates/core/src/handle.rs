//! The C2 typed handle — a typed claim on a value that does not exist yet.
//!
//! A [`Handle<T>`] is the **only** way one node refers to another node's output
//! (arch.md `### C2 · Handle`). It carries the referenced node's identity plus
//! the *type* of the value that node will eventually produce, and it is
//! obtainable **only** by registering a node under an explicit name — there is
//! no public constructor, no `From`/`Default`, and no lookup by name, index, or
//! string key. That single property is what makes a dependency cycle
//! *structurally* inexpressible rather than something a later validation pass
//! catches: a registration can only name handles that already exist, so a
//! forward reference cannot be written (§ *No forgeable path, no lookup* below).
//!
//! # Obtained only by registration
//!
//! The one and only currency for referring to a node's output is the handle its
//! registration returned. This crate exposes **no** function that manufactures a
//! handle from a name, an index, or any string key, and none can be added
//! without a deliberate `pub` change that review will catch. The real flow
//! builder that hands handles back lives in T13 (C7); this module delivers the
//! handle type, its identity payload, and the crate-private registration seam
//! (`Handle::for_registration`) the builder consumes.
//!
//! # Identity comes from the name, never from order
//!
//! A node's identity is derived from the **author-declared registration name**
//! (T0.7), not from registration order. Renaming a node therefore changes its
//! identity (and, downstream, the structural fingerprint — C21), while
//! reordering registrations changes nothing. This module consumes that
//! contract; it does not re-derive identity from order.
//!
//! # Cheap and freely copyable
//!
//! [`Handle<T>`] holds identity, never the value, so it is `Copy` and trivially
//! cheap to pass around during construction — and it stays `Copy + Send + Sync`
//! for **every** value type `T`, even a `T` that is itself `!Send + !Sync +
//! !Copy` (an [`Rc`](std::rc::Rc), say). That is the whole point of the
//! [`PhantomData<fn() -> T>`](std::marker::PhantomData) marker: the handle
//! carries the value type at compile time without inheriting its auto-traits.
//! Because it is `Copy`, one producer handle fans out to any number of
//! downstream consumers by reuse (C3 / T11).
//!
//! # No forgeable path, no lookup
//!
//! [`Handle`]'s fields are private and there is no public constructor, so a
//! handle cannot be fabricated from outside this crate; the checked-in UI
//! fixture [`typed_handle_unforgeable`] asserts a struct-literal forgery fails
//! to compile. Likewise there is no `get(name)` / `get(index)` / `get(key)`
//! anywhere in this crate — the only route to a handle is a registration return
//! value, and the checked-in UI fixture [`typed_handle_no_lookup`] asserts that
//! no such lookup exists. Adding either would require a deliberate `pub` change.
//!
//! [`typed_handle_unforgeable`]: https://github.com/athvin/dagr/blob/main/crates/core/tests/ui/typed_handle_unforgeable.rs
//! [`typed_handle_no_lookup`]: https://github.com/athvin/dagr/blob/main/crates/core/tests/ui/typed_handle_no_lookup.rs
//!
//! # What lives elsewhere
//!
//! - **Data-dependency binding** — exact type matching, tuple arities, fan-out,
//!   the ownership model — is C3 / T11; this module only provides the handle the
//!   binding consumes.
//! - **The flow builder, registration ergonomics, duplicate-name checking, and
//!   the stable-name trait/derive** are C7 / T13 and T0.7; this module consumes
//!   the identity contract and exposes the registration seam.
//! - **The full compile-failure suite** (cycle inexpressibility across data and
//!   ordering edges, wrong-type/arity binding, …) is T12 on the T8 harness; this
//!   module guarantees only the public surface those fixtures assert against.

use std::marker::PhantomData;

/// A node's identity token — an opaque, name-derived key (arch.md `### C2 ·
/// Handle`; T0.7).
///
/// Identity is derived from the node's **author-declared registration name**,
/// never from registration order: the same name always yields the same
/// `NodeId`, and two different names yield different ones (barring a
/// vanishingly-improbable hash collision — see below). This is what makes a
/// rename change identity while a reorder changes nothing.
///
/// It is deliberately **opaque**: the wrapped value is private, there is no
/// public constructor, and it exposes no route back to a name or to a handle.
/// It is `Copy + Eq + Hash` so downstream code (the builder's whole-pipeline
/// uniqueness check, T13) can key on it, and `Debug` so it can appear in
/// diagnostics — but it is *not* a lookup key into any registry (dagr has no
/// runtime node registry; the graph shape is fixed at compile time).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeId(u64);

impl NodeId {
    /// Derive the identity token for a node registered under `name`.
    ///
    /// Identity is minted here, at the single registration seam. It is a pure
    /// function of the name — reorder-stable and rename-sensitive by construction
    /// — so this is *not* a lookup: it **manufactures no handle** and **consults
    /// no registry**; it only hashes the name the caller already holds. The real
    /// builder (T13) supplies the name from the author's declaration and pairs the
    /// resulting id with the node.
    ///
    /// # Not a C2 escape hatch
    ///
    /// This is deliberately **public** because hand-construction of a
    /// [`RunContext`](crate::context::RunContext) — the C8 / T16 test-kit
    /// guarantee (feeds T60) — names the nodes a teardown context covers, and a
    /// teardown developer identifies covered nodes by their author-declared name.
    /// Exposing it does **not** weaken C2: a `NodeId` is an opaque identity token
    /// with no route back to a name or to a [`Handle`], and this function returns
    /// no [`Handle`] and reaches into no runtime node registry (dagr has none).
    /// The unforgeable-[`Handle`]/no-lookup contract is about *handles* and about
    /// a *lookup service*, neither of which this touches.
    #[must_use]
    pub fn from_name(name: &str) -> Self {
        // FxHash-free, dependency-free: FNV-1a over the name bytes. A stable,
        // deterministic hash is all identity needs here; the fingerprint's own
        // hash function is T0.7/T41's concern, not this equality token's.
        const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
        const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
        let mut hash = FNV_OFFSET;
        for byte in name.as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(FNV_PRIME);
        }
        Self(hash)
    }

    /// The opaque inner value, exposed **crate-internally only** as a total,
    /// stable sort key. Assembly (T14) sorts upstream-id lists by this to
    /// deduplicate and count dependencies deterministically; it is not a public
    /// route back to a name or a handle (identity stays opaque — C2).
    #[must_use]
    pub(crate) fn sort_key(self) -> u64 {
        self.0
    }
}

/// A typed claim on a value that does not exist yet (arch.md `### C2 · Handle`).
///
/// A `Handle<T>` is what a node registration hands back, and the **only** way to
/// refer to that node's output afterward. It carries the node's
/// [identity](NodeId) plus the compile-time *type* `T` of the value the node
/// will eventually produce. It is obtained **only** by registering a node (there
/// is no public constructor, no `From`/`Default`, no lookup by name/index/key —
/// see the [module docs](self)), which is precisely what makes a dependency
/// cycle inexpressible: a registration can only name handles that already exist.
///
/// # Freely copyable
///
/// `Handle<T>` is [`Copy`] (and [`Clone`]) for **every** `T` — the manual impls
/// below do not require `T: Copy` — so it can be copied and passed around freely
/// during construction and one producer handle fans out to any number of
/// consumers. It stays `Copy + Send + Sync` even when `T` is `!Send + !Sync +
/// !Copy`, because the [`PhantomData<fn() -> T>`](std::marker::PhantomData)
/// marker carries the type without owning a `T` or inheriting its auto-traits.
///
/// The value type is load-bearing at the binding site: the data-dependency API
/// (C3 / T11) matches a bound `Handle<T>`'s `T` against the consuming task's
/// declared input type, and a mismatch is a compile error.
pub struct Handle<T> {
    /// The referenced node's identity (name-derived — T0.7). Private: a handle
    /// is unforgeable from outside this crate.
    id: NodeId,
    /// The value type the node will produce, carried at compile time. `fn() ->
    /// T` (not bare `T`) keeps the handle `Copy + Send + Sync` for every `T` and
    /// covariant in `T`, while owning no `T`.
    value: PhantomData<fn() -> T>,
}

// Manual `Clone`/`Copy` — NOT `#[derive]`. A derive would emit `impl<T: Copy>`,
// wrongly requiring `T: Copy`; these unconditional impls make the handle `Copy`
// for every `T` (T5 ADR §1).
impl<T> Clone for Handle<T> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<T> Copy for Handle<T> {}

impl<T> Handle<T> {
    /// Mint the handle for a node registered under `name`.
    ///
    /// This is the **only** constructor, and it is crate-private: the sole way
    /// to obtain a `Handle<T>` from outside this crate is as the return value of
    /// node registration, which is exactly what the flow builder (T13) calls
    /// here. Its existence is why a handle is unforgeable — there is no public
    /// path to it — and why identity flows from the name rather than from
    /// registration order.
    // Seam consumed by T13 (flow builder) / T11 (binding); see `NodeId::from_name`.
    #[allow(
        dead_code,
        reason = "crate-private registration seam consumed by T13 (flow builder) / T11 (binding)"
    )]
    pub(crate) fn for_registration(name: &str) -> Self {
        Self {
            id: NodeId::from_name(name),
            value: PhantomData,
        }
    }

    /// The identity of the node this handle refers to (name-derived — T0.7).
    ///
    /// This is the one observation the public surface offers: it lets a caller
    /// compare handle identities and confirm that two handles name the same or
    /// different nodes. It is *not* a lookup — it reads the identity a
    /// registration already stamped on the handle; it consults no registry and
    /// manufactures no handle.
    #[must_use]
    pub fn id(self) -> NodeId {
        self.id
    }
}

/// Test-only construction seam for this crate's own unit tests.
///
/// The registration seam ([`Handle::for_registration`]) is `pub(crate)`, so an
/// external integration test cannot mint a handle — which is the point. This
/// module lets the crate's own tests obtain a handle without depending on the
/// (not-yet-shipped) flow builder. It is compiled out of every non-test build.
#[cfg(test)]
pub(crate) mod test_support {
    use super::Handle;

    /// Mint a handle for a node registered under `name`, as the builder (T13)
    /// will. Test-only.
    pub(crate) fn registered<T>(name: &str) -> Handle<T> {
        Handle::for_registration(name)
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::registered;
    use super::{Handle, NodeId};

    /// Handles are freely COPYABLE (not merely `Clone`) and passable during
    /// construction: copying does not move, the original stays usable after
    /// copies are taken and passed into helpers, and every copy names the same
    /// node. (C2 acceptance: "handles can be copied and passed around freely.")
    #[test]
    fn handles_are_freely_copyable() {
        fn passthrough<T>(handle: Handle<T>) -> Handle<T> {
            let taken = handle; // a Copy, not a move
            let reused = handle; // original still usable after a copy was taken
            let _ = reused;
            taken
        }

        let handle: Handle<u32> = registered::<u32>("counts");

        let copy = handle; // Copy, not move
        let out = passthrough(handle); // pass a copy into and out of a helper
        let still_usable = handle; // original still usable afterward

        // All copies name the same node.
        assert_eq!(copy.id(), still_usable.id());
        assert_eq!(out.id(), handle.id());
    }

    /// A handle carries its node's typed output: two handles for distinctly-typed,
    /// distinctly-named nodes are distinct and each reports its own identity. The
    /// *value type* half is a compile-time fact carried in the handle's type; here
    /// we observe that identity tracks the registration.
    #[test]
    fn a_handle_carries_the_nodes_typed_output() {
        struct A;
        struct B;

        let a: Handle<A> = registered::<A>("makes-a");
        let b: Handle<B> = registered::<B>("makes-b");

        assert_ne!(a.id(), b.id());
        assert_eq!(a.id(), NodeId::from_name("makes-a"));
        assert_eq!(b.id(), NodeId::from_name("makes-b"));
    }

    /// Identity comes from the NAME, not from registration order: the same name
    /// yields a byte-for-byte identical `NodeId` regardless of the order in which
    /// nodes are registered. Modelled by minting the same names in two orders.
    /// (C2 acceptance: "reordering registrations changes nothing.")
    #[test]
    fn identity_comes_from_name_not_registration_order() {
        // Order one: x then y.
        let x1: Handle<u32> = registered::<u32>("x");
        let y1: Handle<u32> = registered::<u32>("y");
        // Order two: y then x.
        let y2: Handle<u32> = registered::<u32>("y");
        let x2: Handle<u32> = registered::<u32>("x");

        assert_eq!(x1.id(), x2.id());
        assert_eq!(y1.id(), y2.id());
    }

    /// Renaming a node changes its identity: two otherwise-identical registrations
    /// under different names produce different identities, and the difference is
    /// solely the name. (C2 acceptance: "renaming a node changes its identity.")
    #[test]
    fn renaming_a_node_changes_its_identity() {
        let under_x: Handle<u32> = registered::<u32>("x");
        let under_y: Handle<u32> = registered::<u32>("y");

        assert_ne!(under_x.id(), under_y.id());
        assert_eq!(under_x.id(), NodeId::from_name("x"));
        assert_eq!(under_y.id(), NodeId::from_name("y"));
    }

    /// The name-derived identity is a pure function of the name — deterministic
    /// across calls — which is what underpins both reorder-stability and the
    /// downstream byte-identical fingerprint (C21).
    #[test]
    fn identity_is_a_pure_function_of_the_name() {
        assert_eq!(
            NodeId::from_name("stage-one"),
            NodeId::from_name("stage-one")
        );
        assert_ne!(
            NodeId::from_name("stage-one"),
            NodeId::from_name("stage-two")
        );
    }
}
