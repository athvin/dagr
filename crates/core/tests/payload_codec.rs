//! The `Payload` codec contract — round trip, determinism, and **refusal rather
//! than misinterpretation**.
//!
//! # Why this suite exists
//!
//! Crossing a pod boundary needs bytes, and the codec that produces them lives in
//! `dagr-core`, whose runtime dependency set is empty — so the encoding is dagr's
//! own, with no serde and no codec crate. Nothing writes those bytes anywhere yet
//! (the blob port and the pod-side writer are later tickets), which is exactly why
//! the codec is testable in isolation: a round-trip property needs no cluster.
//!
//! What this file pins:
//!
//! - **Round trip.** Every supported shape — unit struct, tuple struct, named
//!   struct, enum with and without data, nested payloads, `Option`, `Vec`,
//!   `BTreeMap`, tuples, `()` and the primitives — decodes back to the value that
//!   was encoded.
//! - **Determinism and canonical order.** Encoding the same value twice yields the
//!   same bytes, and a `BTreeMap` built by inserting in two different orders
//!   encodes identically — the property content addressing will rest on.
//! - **Refusal, not misinterpretation.** Bytes encoded from one type decoded as
//!   another are a classified *type-identity mismatch* naming both stable names,
//!   never a successfully-decoded wrong value; truncation, trailing garbage, and a
//!   format-version bump are each their own variant; and a `CodecError`'s `Display`
//!   names what was expected and what was found with its source chain intact.
//!
//! Determinism: pure in-memory encode/decode, no clock, no filesystem, no runtime.

use std::collections::BTreeMap;
use std::error::Error;

use dagr_core::payload::{Codec, CodecError, Cursor, FORMAT_VERSION, MAGIC, round_trip};
use dagr_core::{Payload, StableName};

// ===========================================================================
// The shapes under test — one per field shape the derive must cover.
// ===========================================================================

/// A unit struct: no fields, so its body is empty and only the envelope names it.
#[derive(Debug, Clone, PartialEq, Eq, StableName, Payload)]
struct Marker;

/// A tuple struct over two different primitive shapes.
#[derive(Debug, Clone, PartialEq, Eq, StableName, Payload)]
struct Pair(u32, String);

/// A named struct over an integer, a string, and a bool.
#[derive(Debug, Clone, PartialEq, Eq, StableName, Payload)]
struct Named {
    count: u64,
    label: String,
    flag: bool,
}

/// An enum with a data-free variant, a tuple variant, and a struct variant.
#[derive(Debug, Clone, PartialEq, Eq, StableName, Payload)]
enum Shape {
    Empty,
    One(u32),
    Both { left: i64, right: Option<String> },
}

/// A composite over every container the codec ships: a nested payload, a `Vec`, a
/// `BTreeMap`, an `Option`, and a bare tuple.
#[derive(Debug, Clone, PartialEq, Eq, StableName, Payload)]
struct Composite {
    inner: Named,
    items: Vec<u32>,
    index: BTreeMap<String, u64>,
    maybe: Option<Pair>,
    pair: (u8, bool),
}

/// A single-`String` payload, used to craft malformed bytes by hand.
#[derive(Debug, Clone, PartialEq, Eq, StableName, Payload)]
struct Labelled {
    label: String,
}

/// A fixture value exercising every container at once.
fn composite() -> Composite {
    let mut index = BTreeMap::new();
    index.insert("beta".to_string(), 2);
    index.insert("alpha".to_string(), 1);
    Composite {
        inner: Named {
            count: 7,
            label: "seven".to_string(),
            flag: true,
        },
        items: vec![1, 2, 3],
        index,
        maybe: Some(Pair(9, "nine".to_string())),
        pair: (255, false),
    }
}

/// Encode `value` through the envelope into a fresh buffer.
fn encoded<T: Payload>(value: &T) -> Vec<u8> {
    let mut bytes = Vec::new();
    value.encode(&mut bytes);
    bytes
}

// ===========================================================================
// Round trip
// ===========================================================================

/// Every derived shape encodes and decodes back to an equal value.
#[test]
fn every_derived_shape_round_trips() {
    assert_eq!(
        Marker::decode(&encoded(&Marker)).expect("unit struct"),
        Marker
    );

    let pair = Pair(42, "answer".to_string());
    assert_eq!(Pair::decode(&encoded(&pair)).expect("tuple struct"), pair);

    let named = Named {
        count: u64::MAX,
        label: "everything".to_string(),
        flag: false,
    };
    assert_eq!(
        Named::decode(&encoded(&named)).expect("named struct"),
        named
    );

    for variant in [
        Shape::Empty,
        Shape::One(u32::MAX),
        Shape::Both {
            left: -9,
            right: Some("right".to_string()),
        },
        Shape::Both {
            left: 0,
            right: None,
        },
    ] {
        assert_eq!(
            Shape::decode(&encoded(&variant)).expect("enum variant"),
            variant,
            "each enum variant round-trips: {variant:?}"
        );
    }

    let composite = composite();
    assert_eq!(
        Composite::decode(&encoded(&composite)).expect("nested + containers"),
        composite
    );
}

/// The primitives and standard containers a payload realistically uses each round
/// trip through the body codec.
#[test]
fn primitives_and_containers_round_trip() {
    fn body_round_trip<T: Codec + PartialEq + std::fmt::Debug>(value: &T) {
        let mut bytes = Vec::new();
        value.encode_body(&mut bytes);
        let mut cursor = Cursor::new(&bytes);
        let decoded = T::decode_body(&mut cursor).expect("a well-formed body decodes");
        assert_eq!(&decoded, value, "the body codec round-trips");
        assert!(
            cursor.is_empty(),
            "decoding consumes exactly the bytes encoding wrote"
        );
    }

    body_round_trip(&0_u8);
    body_round_trip(&u8::MAX);
    body_round_trip(&u16::MAX);
    body_round_trip(&u32::MAX);
    body_round_trip(&u64::MAX);
    body_round_trip(&u128::MAX);
    body_round_trip(&usize::MAX);
    body_round_trip(&i8::MIN);
    body_round_trip(&i16::MIN);
    body_round_trip(&i32::MIN);
    body_round_trip(&i64::MIN);
    body_round_trip(&i128::MIN);
    body_round_trip(&isize::MIN);
    body_round_trip(&true);
    body_round_trip(&false);
    body_round_trip(&String::new());
    body_round_trip(&"a string with unicode: ✓".to_string());
    body_round_trip(&None::<u32>);
    body_round_trip(&Some("some".to_string()));
    body_round_trip(&Vec::<u32>::new());
    body_round_trip(&vec![1_u64, 2, 3]);
    body_round_trip(&vec![vec!["nested".to_string()]]);
    body_round_trip(&BTreeMap::<String, u64>::new());
    body_round_trip(&BTreeMap::from([("k".to_string(), 1_u64)]));
    body_round_trip(&());
    body_round_trip(&(1_u8,));
    body_round_trip(&(1_u8, "two".to_string()));
    body_round_trip(&(1_u8, 2_u16, 3_u32, 4_u64, true, "six".to_string()));
}

/// `()` — a consume-nothing task's input and an effect-only node's output — is a
/// payload in its own right, carrying the reserved unit stable name.
#[test]
fn the_unit_type_is_a_payload() {
    let bytes = encoded(&());
    assert_eq!(<()>::decode(&bytes).expect("unit round trip"), ());
    assert_eq!(
        <() as StableName>::STABLE_NAME,
        dagr_core::UNIT_STABLE_NAME,
        "the unit payload keeps the reserved sentinel name"
    );
}

/// `Payload` extends `StableName`: a payload-bounded generic can always read the
/// author-declared name, which is what the encoded envelope carries.
#[test]
fn payload_implies_stable_name() {
    fn name_of<T: Payload>() -> &'static str {
        T::STABLE_NAME
    }
    assert_eq!(name_of::<Named>(), "Named");
    assert_eq!(name_of::<Composite>(), "Composite");
}

/// The `round_trip` helper — what the local force-round-trip toggle drives — is an
/// encode immediately followed by a decode.
#[test]
fn the_round_trip_helper_returns_an_equal_value() {
    let value = composite();
    assert_eq!(round_trip(&value).expect("a clean round trip"), value);
}

// ===========================================================================
// Determinism and canonical ordering
// ===========================================================================

/// Encoding the same value twice yields byte-identical output — the property
/// content addressing rests on.
#[test]
fn encoding_the_same_value_twice_is_byte_identical() {
    let value = composite();
    assert_eq!(
        encoded(&value),
        encoded(&value),
        "two encodings of one value must be the same bytes"
    );
}

/// A `BTreeMap` built by inserting in two different orders encodes identically —
/// canonical ordering, matching the artifact layer's posture.
#[test]
fn a_map_encodes_canonically_whatever_the_insertion_order() {
    let mut ascending = BTreeMap::new();
    ascending.insert("alpha".to_string(), 1_u64);
    ascending.insert("beta".to_string(), 2);
    ascending.insert("gamma".to_string(), 3);

    let mut descending = BTreeMap::new();
    descending.insert("gamma".to_string(), 3_u64);
    descending.insert("beta".to_string(), 2);
    descending.insert("alpha".to_string(), 1);

    let mut a = Vec::new();
    ascending.encode_body(&mut a);
    let mut d = Vec::new();
    descending.encode_body(&mut d);
    assert_eq!(a, d, "insertion order must not reach the bytes");
}

/// A nested payload carries **no** second envelope: the type identity is recorded
/// once, at the top level, so a composite's bytes never repeat an inner type's
/// stable name.
#[test]
fn a_nested_payload_carries_no_second_envelope() {
    let bytes = encoded(&composite());
    let needle = Named::STABLE_NAME.as_bytes();
    assert!(
        !bytes.windows(needle.len()).any(|w| w == needle),
        "the nested `Named` must not carry its own envelope inside `Composite`"
    );
}

// ===========================================================================
// Refusal, not misinterpretation
// ===========================================================================

/// Bytes encoded from one type, decoded as another, are a **type-identity
/// mismatch** naming both stable names — never a successfully-decoded wrong value.
#[test]
fn bytes_from_another_type_are_a_type_identity_mismatch() {
    // `Pair(u32, String)` and `Labelled { label: String }` are deliberately
    // *compatible* byte-wise if the name were not checked — the mismatch must come
    // from the identity, not from a lucky parse failure.
    let bytes = encoded(&Pair(1, "x".to_string()));
    let err = Labelled::decode(&bytes).expect_err("a foreign type's bytes are refused");
    match &err {
        CodecError::TypeMismatch { expected, found } => {
            assert_eq!(*expected, Labelled::STABLE_NAME);
            assert_eq!(found, Pair::STABLE_NAME);
        }
        other => panic!("expected a type-identity mismatch, got {other:?}"),
    }
    let rendered = err.to_string();
    assert!(
        rendered.contains(Labelled::STABLE_NAME) && rendered.contains(Pair::STABLE_NAME),
        "the Display names both stable names, got: {rendered}"
    );
}

/// Truncated bytes are their own classified variant, at every truncation point —
/// and never a panic.
#[test]
fn truncated_bytes_are_a_truncation_error() {
    let bytes = encoded(&composite());
    for cut in 0..bytes.len() {
        let err = Composite::decode(&bytes[..cut]).expect_err("truncated bytes are refused");
        assert!(
            matches!(err.root(), CodecError::Truncated { .. }),
            "a truncation at {cut} must be classified as truncation, got {err:?}"
        );
    }
}

/// Bytes remaining after a complete value are trailing garbage — a distinct
/// variant, never silently ignored.
#[test]
fn trailing_bytes_are_trailing_garbage() {
    let mut bytes = encoded(&Named {
        count: 1,
        label: "one".to_string(),
        flag: true,
    });
    let consumed = bytes.len();
    bytes.extend_from_slice(b"and then some");
    let err = Named::decode(&bytes).expect_err("trailing bytes are refused");
    match err {
        CodecError::TrailingGarbage {
            consumed: c,
            total: t,
        } => {
            assert_eq!(c, consumed, "the error reports what was consumed");
            assert_eq!(t, bytes.len(), "and how much there was");
        }
        other => panic!("expected trailing garbage, got {other:?}"),
    }
}

/// A bumped format version is its own refusal — not a malformed-bytes error and
/// never a misinterpretation.
#[test]
fn a_bumped_format_version_is_its_own_error() {
    let mut bytes = encoded(&Marker);
    let bumped = FORMAT_VERSION + 1;
    bytes[MAGIC.len()..MAGIC.len() + 2].copy_from_slice(&bumped.to_le_bytes());
    let err = Marker::decode(&bytes).expect_err("a future format version is refused");
    match err {
        CodecError::UnsupportedVersion { expected, found } => {
            assert_eq!(expected, FORMAT_VERSION);
            assert_eq!(found, bumped);
        }
        other => panic!("expected an unsupported-version error, got {other:?}"),
    }
}

/// Bytes that are not a dagr payload at all are malformed — classified, and never
/// a panic.
#[test]
fn foreign_bytes_are_malformed() {
    let err = Marker::decode(b"not a dagr payload at all").expect_err("foreign bytes are refused");
    assert!(
        matches!(err, CodecError::Malformed { .. }),
        "expected a malformed-bytes error, got {err:?}"
    );
}

/// An enum discriminant outside the declared set is malformed, naming the type.
#[test]
fn an_unknown_enum_variant_is_malformed() {
    let mut bytes = encoded(&Shape::One(1));
    // The variant index is the first four bytes of the body: past the magic, the
    // version, and the length-prefixed stable name.
    let body = MAGIC.len() + 2 + 8 + Shape::STABLE_NAME.len();
    bytes[body..body + 4].copy_from_slice(&99_u32.to_le_bytes());
    let err = Shape::decode(&bytes).expect_err("an unknown variant is refused");
    assert!(
        matches!(err.root(), CodecError::Malformed { .. }),
        "an unknown discriminant is malformed, got {err:?}"
    );
    assert!(
        err.to_string().contains(Shape::STABLE_NAME),
        "the diagnostic names the type, got: {err}"
    );
}

/// A `CodecError` names what was expected and what was found, and its source chain
/// is intact all the way to the underlying cause.
#[test]
fn a_codec_error_displays_the_cause_and_keeps_its_source_chain() {
    // A well-formed `Labelled` whose two string bytes are then replaced with an
    // invalid UTF-8 sequence — the failure is a *field*'s, with a real cause under
    // it.
    let mut bytes = encoded(&Labelled {
        label: "ok".to_string(),
    });
    let len = bytes.len();
    bytes[len - 2..].copy_from_slice(&[0xff, 0xff]);

    let err = Labelled::decode(&bytes).expect_err("invalid UTF-8 is refused");
    let rendered = err.to_string();
    assert!(
        rendered.contains("label") && rendered.contains(Labelled::STABLE_NAME),
        "the Display names the field and its type, got: {rendered}"
    );

    let mut chain: Vec<String> = Vec::new();
    let mut source: Option<&(dyn Error + 'static)> = err.source();
    while let Some(cause) = source {
        chain.push(cause.to_string());
        source = cause.source();
    }
    assert!(
        chain.len() >= 2,
        "the source chain reaches the underlying cause, got: {chain:?}"
    );
    assert!(
        chain.last().is_some_and(|last| last.contains("utf-8")),
        "the deepest cause is the UTF-8 failure, got: {chain:?}"
    );
}

/// A length prefix larger than the remaining input is a truncation, not an
/// allocation the decoder trusts.
#[test]
fn an_oversized_length_prefix_is_a_truncation_not_an_allocation() {
    let mut bytes = encoded(&Labelled {
        label: "ok".to_string(),
    });
    let prefix = MAGIC.len() + 2 + 8 + Labelled::STABLE_NAME.len();
    bytes[prefix..prefix + 8].copy_from_slice(&u64::MAX.to_le_bytes());
    let err = Labelled::decode(&bytes).expect_err("an impossible length is refused");
    assert!(
        matches!(err.root(), CodecError::Truncated { .. }),
        "an oversized length is a truncation, got {err:?}"
    );
}
