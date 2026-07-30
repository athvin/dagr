//! Common-trait tests for `dagr-artifact`'s plain-data error types — ticket 111
//! (T96), written first, TDD.
//!
//! `api-common-traits` asks for the traits a type's fields freely allow. The
//! split in this crate is **structural**, and these tests pin both halves of it:
//!
//! * [`FoldError`] and `SchemaValidationError` carry only plain data (a line
//!   index, two `String`s), so they can be compared, cloned, and hashed — which
//!   is what lets a test assert on an exact fold failure rather than on a
//!   substring of its `Display`.
//! * [`ReadError`] carries a real [`serde_json::Error`] (T95 made it carry the
//!   deserializer's own diagnostic rather than a string copy of it). That type
//!   implements none of `Clone`/`PartialEq`/`Eq`/`Hash`, so `ReadError` cannot
//!   derive them either — the same trade every `io::Error`-carrying type in this
//!   workspace makes. The test below pins what it *does* offer, so a future
//!   change that quietly drops the carried cause to regain a derive is visible.

use dagr_artifact::event_stream::read_records;
use dagr_artifact::fold::FoldError;
use std::collections::hash_map::DefaultHasher;
use std::error::Error;
use std::hash::{Hash, Hasher};

fn hash_of<T: Hash>(v: &T) -> u64 {
    let mut h = DefaultHasher::new();
    v.hash(&mut h);
    h.finish()
}

#[test]
fn fold_error_is_comparable_cloneable_and_hashable() {
    fn assert_clone<T: Clone>() {}
    assert_clone::<FoldError>();

    let a = FoldError::CorruptRecord { line: 7 };
    // `Copy`: reading `a` into `b` leaves `a` usable, with no explicit clone.
    let b = a;
    let other = FoldError::MissingRunStarted;

    assert_eq!(a, b, "a fold failure compares by its data, not its Display");
    assert_ne!(a, other);
    assert_eq!(hash_of(&a), hash_of(&b));
    assert_eq!(a, FoldError::CorruptRecord { line: 7 });
}

#[cfg(feature = "schema-validation")]
#[test]
fn schema_validation_error_is_comparable_cloneable_and_hashable() {
    use dagr_artifact::schema::{ArtifactKind, validate_bytes};

    let err = validate_bytes(ArtifactKind::Graph, 1, b"not json")
        .expect_err("bytes that are not JSON fail validation");
    let clone = err.clone();
    assert_eq!(err, clone, "the error compares by artifact + reason");
    assert_eq!(hash_of(&err), hash_of(&clone));
    assert_eq!(err.artifact(), clone.artifact());
}

#[test]
fn read_error_keeps_the_deserializer_cause_instead_of_a_derive() {
    // A terminated non-final line that does not parse is corruption.
    let err = read_records(b"{\"a\":1}\nnot json\n{\"b\":2}\n")
        .expect_err("a corrupt non-final record is an error");
    assert_eq!(err.line, 1);
    // The cause is a real `serde_json::Error`, reachable through the chain —
    // which is precisely why this type derives no `Clone`/`PartialEq`.
    let cause = err.source().expect("the deserializer error is the cause");
    assert!(
        cause.to_string().contains("expected"),
        "the carried cause is the parser's own diagnostic: {cause}"
    );
}
