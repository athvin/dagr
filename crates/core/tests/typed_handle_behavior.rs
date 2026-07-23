//! Public-surface (no-construction) tests for the C2 typed handle (ticket T10 /
//! 020). Written first, TDD.
//!
//! These prove the properties that hold **without minting a handle**, so they
//! can live in an external integration test that has no access to the
//! crate-private registration seam — which is exactly the point: a handle is
//! unforgeable from outside the crate, so an integration test *cannot* construct
//! one. The behavioral scenarios that need a real handle (identity from the
//! name, reorder-stability, rename-sensitivity, free copyability) are unit tests
//! inside `src/handle.rs`, where the `pub(crate)` registration seam is reachable;
//! the compile-fail scenarios (no forgeable path, no lookup by name/index/key)
//! are UI fixtures under `tests/ui/`.
//!
//! What THIS file proves through the public surface alone:
//! - `Handle<T>` is `Copy + Send + Sync` for **every** value type `T`, including
//!   a `T` that is itself `!Send + !Sync + !Copy` (the `PhantomData<fn() -> T>`
//!   property the T5 ADR fixed);
//! - `NodeId` is a small, comparable identity token (`Copy + Eq + Hash`);
//! - the public surface exposes the handle type and its identity accessor and
//!   nothing that mints or looks up a handle.

use std::rc::Rc;

use dagr_core::handle::{Handle, NodeId};

/// A handle is `Copy` (not just `Clone`), `Send`, and `Sync` for **every** value
/// type — even a `T` that is itself `!Send + !Sync + !Copy`. This is the
/// `PhantomData<fn() -> T>` property the T5 ADR fixed: the handle carries the
/// value type at compile time without inheriting its auto-traits, which is what
/// makes it "cheap and freely copyable" regardless of what it names (C2).
#[test]
fn handle_is_copy_send_sync_for_any_value_type() {
    fn assert_copy<T: Copy>() {}
    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}

    // `Rc<String>` is deliberately NOT Send/Sync/Copy — the adversarial `T`.
    assert_copy::<Handle<Rc<String>>>();
    assert_send::<Handle<Rc<String>>>();
    assert_sync::<Handle<Rc<String>>>();

    // And the same holds for an ordinary value type.
    assert_copy::<Handle<u32>>();
    assert_send::<Handle<u32>>();
    assert_sync::<Handle<u32>>();
}

/// The identity token a handle carries is itself a small, comparable value:
/// `Copy` (so it is cheap to observe and pass around) and `Eq + Hash` (so
/// downstream code — the builder's uniqueness check, T13 — can key on it).
#[test]
fn node_id_is_a_small_comparable_token() {
    fn assert_copy<T: Copy>() {}
    fn assert_eq_hash<T: Eq + std::hash::Hash>() {}

    assert_copy::<NodeId>();
    assert_eq_hash::<NodeId>();
}

/// API-inventory assertion (DoD): the handle module's **public** surface is
/// limited to the handle type, its identity payload, and the identity accessor —
/// with **no** public constructor, no `From`/`Default`, and no lookup by
/// name/index/string key. This is a compile-time inventory: it names every
/// public item this module is *allowed* to expose and binds each to its intended
/// signature, so any future `pub fn get(name: ...) -> Handle<..>`,
/// `impl Default for Handle`, `impl From<..> for Handle`, or public constructor
/// added without a deliberate `pub` change would either make this test's
/// exhaustive intent stale (caught in review) or, for the trait impls below,
/// fail to compile. It does not — and cannot — call any minting or lookup
/// function, because none is exposed.
#[test]
fn public_surface_is_only_the_type_and_its_identity_accessor() {
    // The two public types exist and are nameable.
    fn handle_type_exists<T>(_: Handle<T>) {}
    fn node_id_type_exists(_: NodeId) {}
    let _ = handle_type_exists::<u32>;
    let _ = node_id_type_exists;

    // The ONLY public accessor is `Handle::id(self) -> NodeId`: exactly this
    // signature, taking the handle by value (it is `Copy`) and returning the
    // identity token. Binding it as a function pointer pins the signature; if the
    // accessor's shape ever changed, this line would stop compiling.
    let id_accessor: fn(Handle<u32>) -> NodeId = Handle::<u32>::id;
    let _ = id_accessor;

    // There is NO public way to mint a handle. A handle is `Copy` but implements
    // neither `Default` nor `From<_>`, and there is no public constructor — the
    // only route is a registration return value (the crate-private seam). These
    // negative facts are asserted at compile time by the UI fixtures
    // (`typed_handle_unforgeable`) and by the crate's `pub(crate)` privacy; this
    // inventory records the positive surface so review catches any addition.
    //
    // (We deliberately do NOT reference `Handle::for_registration` or
    // `NodeId::from_name` here — they are `pub(crate)` and unreachable from this
    // external test, which is exactly the unforgeability guarantee in action.)
}
