//! Compile-pass: `#[derive(Payload)]` over every shape the derive accepts — a unit
//! struct, a tuple struct, a named struct, an enum with and without data, a nested
//! payload, and a generic struct (whose type parameters gain the codec bound). Each
//! value is round-tripped in `main`, so this fixture proves the *generated code
//! runs*, not merely that it compiles.

// `Payload` names both the derive (macro namespace) and the trait (type namespace),
// the standard trait+derive pairing; `StableName` is `Payload`'s supertrait, so a
// payload type derives both.
use dagr_core::{Payload, StableName};
use std::collections::BTreeMap;

/// A unit struct: an empty body under the envelope.
#[derive(Debug, PartialEq, StableName, Payload)]
struct Marker;

/// A tuple struct over two primitive shapes.
#[derive(Debug, PartialEq, StableName, Payload)]
struct Pair(u32, String);

/// A named struct, including a container field.
#[derive(Debug, PartialEq, StableName, Payload)]
struct Named {
    count: u64,
    labels: Vec<String>,
}

/// An enum with a data-free variant, a tuple variant, and a struct variant.
#[derive(Debug, PartialEq, StableName, Payload)]
enum Shape {
    Empty,
    One(i64),
    Both { left: bool, right: Option<String> },
}

/// A composite over a nested payload and the standard containers.
#[derive(Debug, PartialEq, StableName, Payload)]
struct Composite {
    inner: Named,
    shape: Shape,
    index: BTreeMap<String, u64>,
    pair: (u8, bool),
}

/// A generic payload: the derive adds the codec bound to each type parameter.
#[derive(Debug, PartialEq, StableName, Payload)]
struct Wrapper<T> {
    value: T,
}

/// Encode `value`, decode it back, and assert the two are equal.
fn round_trips<T: Payload + PartialEq + std::fmt::Debug>(value: &T) {
    let mut bytes = Vec::new();
    value.encode(&mut bytes);
    let decoded = T::decode(&bytes).expect("a derived payload round-trips");
    assert_eq!(&decoded, value);
}

fn main() {
    round_trips(&Marker);
    round_trips(&Pair(7, "seven".to_string()));
    round_trips(&Named {
        count: 1,
        labels: vec!["a".to_string(), "b".to_string()],
    });
    round_trips(&Shape::Empty);
    round_trips(&Shape::One(-1));
    round_trips(&Shape::Both {
        left: true,
        right: Some("right".to_string()),
    });
    round_trips(&Composite {
        inner: Named {
            count: 2,
            labels: Vec::new(),
        },
        shape: Shape::One(3),
        index: BTreeMap::from([("k".to_string(), 4_u64)]),
        pair: (5, false),
    });
    round_trips(&Wrapper { value: 6_u32 });
}
