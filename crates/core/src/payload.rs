//! The **payload codec** — how a value becomes bytes, and how bytes refuse to
//! become the wrong value.
//!
//! A local handoff moves a typed Rust value through an output slot and never
//! serializes anything. Crossing a process boundary needs bytes, and this module is
//! where dagr owns that translation: a [`Codec`] for the body of a value, a
//! [`Payload`] envelope that records **which type** the bytes came from, and a
//! classified [`CodecError`] that distinguishes a malformed encoding from a
//! type-identity mismatch from a format-version mismatch.
//!
//! # No dependency, on purpose
//!
//! `dagr-core`'s runtime dependency set is **empty**, and additions to it are
//! reviewed as API decisions. So there is no serde and no codec crate here: the
//! encoding is a few hundred lines of length-prefixed, fixed-width,
//! canonically-ordered bytes, and `#[derive(Payload)]` (in the build-time-only
//! `dagr-macros` crate, like `#[task]` and `#[derive(StableName)]`) writes the
//! per-type half.
//!
//! # Two traits, one bound
//!
//! - [`Codec`] is the **body** codec: append this value's bytes, decode them back.
//!   Every primitive, container, and tuple this module ships implements it, and the
//!   derive emits it for a struct or enum whose fields are themselves `Codec`.
//! - [`Payload`] is [`Codec`] **plus** [`StableName`],
//!   and it is the bound the boundary requires. Its [`encode`](Payload::encode) /
//!   [`decode`](Payload::decode) wrap the body in a self-describing envelope
//!   carrying the format version and the type's **author-declared** stable name, so
//!   a shard encoded from a different type after a refactor is a *classified error*
//!   rather than a misinterpreted byte string. It is blanket-implemented, so the
//!   envelope is the framework's and cannot be overridden per type.
//!
//! A **tuple** implements [`Codec`] but not [`Payload`]: it has no author-declared
//! name, and it is never a payload at a boundary either — dagr binds inputs
//! positionally, so an N-input node's edges carry N separately-named values, not one
//! encoded tuple. Inside a derived struct a tuple field is ordinary composite data,
//! which is exactly what [`Codec`] describes. (`()` *is* a payload: it carries the
//! reserved [`UNIT_STABLE_NAME`](crate::stable_name::UNIT_STABLE_NAME) sentinel, so
//! an effect-only node's output type has a name like any other.)
//!
//! # The encoding
//!
//! It is **not** a public wire contract in this milestone: the same binary encodes
//! and decodes, so the format may change while the version tag moves. What *is*
//! contractual is the behaviour the tests pin:
//!
//! - **Deterministic.** Encoding a value twice yields identical bytes.
//! - **Canonical.** A [`BTreeMap`] encodes in ascending
//!   key order whatever the insertion order, and a decoder *rejects* a
//!   non-ascending or duplicate-keyed encoding rather than accepting two byte
//!   strings for one value — which is what makes content addressing sound.
//! - **Self-describing enough to refuse.** The envelope carries the format version
//!   and the stable name; a mismatch in either is its own [`CodecError`] variant.
//!
//! The layout, for the record: a 4-byte [`MAGIC`], the [`FORMAT_VERSION`] as a
//! little-endian `u16`, the stable name as a `u64` length prefix plus its UTF-8
//! bytes, then the body. Integers are fixed-width little-endian; `usize`/`isize`
//! travel as 64-bit so the bytes do not depend on the encoder's word size; lengths
//! and counts are `u64`; `bool` is one byte; a `String`, `Vec`, or map is a `u64`
//! count followed by its elements; an `Option` is a one-byte tag; a nested payload
//! contributes its **body only**, so the identity is recorded once, at the top.
//!
//! **Floating point is deliberately absent.** A determinism claim over `f32`/`f64`
//! would be a lie without a normalization rule for `NaN` payloads and signed zero,
//! and no shipped dagr payload needs one; an author who needs a float today encodes
//! its bits (`f64::to_bits`) in a named payload struct and owns that choice
//! explicitly.
//!
//! # What this module does not do
//!
//! It writes bytes **nowhere**. There is no blob store here, no shard format, and
//! no requirement that anything implement [`Payload`] — local pipelines are
//! untouched. The one local consumer is the force-round-trip toggle, which calls
//! [`round_trip`] on a payload-bounded handoff so a codec bug is catchable without
//! a cluster.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;

use crate::stable_name::StableName;

/// The four bytes every encoded payload starts with, so bytes that are not a dagr
/// payload at all are refused as [malformed](CodecError::Malformed) instead of
/// being parsed as if they were.
pub const MAGIC: [u8; 4] = *b"dgrP";

/// The encoding's format version, carried in the envelope. A decoder refuses any
/// other version with [`CodecError::UnsupportedVersion`] — the format may change
/// while this number moves, because the same binary encodes and decodes.
pub const FORMAT_VERSION: u16 = 1;

// ===========================================================================
// The classified error
// ===========================================================================

/// Why a decode refused.
///
/// The variants are the classification: a **type-identity mismatch** is not the
/// same event as a **version mismatch**, and neither is the same as bytes that ran
/// out or bytes left over. Every one of them is a refusal — the codec never returns
/// a successfully-decoded wrong value.
///
/// A composite's field failure is wrapped in [`Field`](CodecError::Field), which
/// keeps the path (`type` / `field`) *and* the cause; [`root`](CodecError::root)
/// looks through that wrapping when a caller wants the underlying classification,
/// and [`Error::source`] exposes the whole chain.
#[derive(Debug)]
#[non_exhaustive]
pub enum CodecError {
    /// The envelope names a **different type** than the one being decoded into —
    /// the refusal that makes a stale shard a classified error rather than a
    /// misinterpreted byte string. Names both author-declared stable names.
    TypeMismatch {
        /// The stable name of the type the caller asked to decode into.
        expected: &'static str,
        /// The stable name the encoded envelope actually carries.
        found: String,
    },
    /// The envelope carries a format version this build does not implement.
    UnsupportedVersion {
        /// The version this build encodes and decodes ([`FORMAT_VERSION`]).
        expected: u16,
        /// The version the bytes carry.
        found: u16,
    },
    /// The input ended before the value did.
    Truncated {
        /// What was being decoded when the bytes ran out (a type or field name).
        context: &'static str,
        /// How many more bytes that read needed.
        needed: usize,
        /// How many were left.
        available: usize,
    },
    /// The value decoded, and bytes remained — never silently ignored, because
    /// trailing bytes mean the input was not what the caller thought it was.
    TrailingGarbage {
        /// How many bytes the value consumed.
        consumed: usize,
        /// How many bytes there were.
        total: usize,
    },
    /// The bytes are structurally invalid: a wrong magic, a bool that is neither 0
    /// nor 1, invalid UTF-8, an unknown enum discriminant, a non-canonical map, or a
    /// length no platform value could hold.
    Malformed {
        /// What was being decoded (a type, field, or component name).
        context: &'static str,
        /// What was wrong, in words.
        detail: String,
        /// The underlying cause, when there is one (a UTF-8 error, say).
        source: Option<Box<dyn Error + Send + Sync>>,
    },
    /// A composite's field (or enum variant field) failed to decode. Carries the
    /// path and the cause, so the diagnostic names *where* as well as *what*.
    Field {
        /// The composite type's author-declared stable name.
        type_name: &'static str,
        /// The field's name (or its index, for a tuple struct or tuple variant).
        field: &'static str,
        /// What went wrong inside that field.
        source: Box<CodecError>,
    },
}

impl CodecError {
    /// A structurally-invalid encoding, with no underlying cause.
    #[must_use]
    pub fn malformed(context: &'static str, detail: impl Into<String>) -> Self {
        Self::Malformed {
            context,
            detail: detail.into(),
            source: None,
        }
    }

    /// A structurally-invalid encoding whose underlying cause is preserved through
    /// [`Error::source`].
    #[must_use]
    pub fn malformed_from(
        context: &'static str,
        detail: impl Into<String>,
        source: impl Error + Send + Sync + 'static,
    ) -> Self {
        Self::Malformed {
            context,
            detail: detail.into(),
            source: Some(Box::new(source)),
        }
    }

    /// Wrap a failure in the field it happened in — what `#[derive(Payload)]`
    /// emits, so a nested failure names its path without losing its cause.
    #[must_use]
    pub fn in_field(type_name: &'static str, field: &'static str, source: Self) -> Self {
        Self::Field {
            type_name,
            field,
            source: Box::new(source),
        }
    }

    /// The underlying classification, looking through any [`Field`](CodecError::Field)
    /// wrapping.
    ///
    /// A caller that wants to know *what kind* of failure this is (truncation? a
    /// version mismatch?) asks the root; a caller rendering a diagnostic uses the
    /// error itself, which names the path.
    #[must_use]
    pub fn root(&self) -> &Self {
        match self {
            Self::Field { source, .. } => source.root(),
            other => other,
        }
    }
}

impl fmt::Display for CodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TypeMismatch { expected, found } => write!(
                f,
                "payload type mismatch: these bytes were encoded from `{found}`, but a \
                 `{expected}` was expected — the encoded envelope carries the \
                 author-declared stable name, so a value encoded from another type is \
                 refused rather than misread"
            ),
            Self::UnsupportedVersion { expected, found } => write!(
                f,
                "unsupported payload format version: found {found}, expected {expected} \
                 (this build encodes and decodes version {expected})"
            ),
            Self::Truncated {
                context,
                needed,
                available,
            } => write!(
                f,
                "truncated payload while decoding `{context}`: {needed} more byte(s) \
                 needed, {available} available"
            ),
            Self::TrailingGarbage { consumed, total } => write!(
                f,
                "trailing bytes after a complete payload: {consumed} of {total} byte(s) \
                 consumed, {} left over",
                total.saturating_sub(*consumed)
            ),
            Self::Malformed {
                context, detail, ..
            } => write!(f, "malformed payload while decoding `{context}`: {detail}"),
            Self::Field {
                type_name,
                field,
                source,
            } => write!(
                f,
                "while decoding field `{field}` of `{type_name}`: {source}"
            ),
        }
    }
}

impl Error for CodecError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Malformed { source, .. } => source
                .as_ref()
                .map(|boxed| boxed.as_ref() as &(dyn Error + 'static)),
            Self::Field { source, .. } => Some(source.as_ref()),
            Self::TypeMismatch { .. }
            | Self::UnsupportedVersion { .. }
            | Self::Truncated { .. }
            | Self::TrailingGarbage { .. } => None,
        }
    }
}

// ===========================================================================
// The read cursor
// ===========================================================================

/// A position in an encoded payload — what [`Codec::decode_body`] reads through.
///
/// It hands out slices of the input and never allocates, so a decoder cannot be
/// tricked into reserving memory by a length prefix it has not yet validated: every
/// read goes through [`take`](Cursor::take), which refuses to hand out more bytes
/// than remain.
#[derive(Debug)]
pub struct Cursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Cursor<'a> {
    /// A cursor over `bytes`, positioned at the start.
    #[must_use]
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    /// How many bytes have been consumed so far.
    #[must_use]
    pub fn position(&self) -> usize {
        self.position
    }

    /// How many bytes remain unread.
    #[must_use]
    pub fn remaining(&self) -> usize {
        self.bytes.len() - self.position
    }

    /// Whether every byte has been consumed — what the envelope's trailing-garbage
    /// check asks.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.remaining() == 0
    }

    /// Consume exactly `n` bytes.
    ///
    /// # Errors
    ///
    /// [`CodecError::Truncated`] naming `context` if fewer than `n` bytes remain —
    /// the length prefix is never trusted past what the input actually holds.
    pub fn take(&mut self, n: usize, context: &'static str) -> Result<&'a [u8], CodecError> {
        if self.remaining() < n {
            return Err(CodecError::Truncated {
                context,
                needed: n,
                available: self.remaining(),
            });
        }
        let slice = &self.bytes[self.position..self.position + n];
        self.position += n;
        Ok(slice)
    }

    /// Consume exactly one byte.
    ///
    /// # Errors
    ///
    /// [`CodecError::Truncated`] naming `context` if the input is exhausted.
    pub fn take_byte(&mut self, context: &'static str) -> Result<u8, CodecError> {
        Ok(self.take(1, context)?[0])
    }

    /// Consume a `u64` length or count prefix and narrow it to a `usize`.
    ///
    /// # Errors
    ///
    /// [`CodecError::Truncated`] if the prefix itself is not there, and
    /// [`CodecError::Malformed`] if it names more elements than this platform can
    /// address (a corrupt prefix, never an allocation attempt).
    pub fn take_len(&mut self, context: &'static str) -> Result<usize, CodecError> {
        let raw = u64::decode_body_in(self, context)?;
        usize::try_from(raw).map_err(|_| {
            CodecError::malformed(
                context,
                format!("length {raw} exceeds this platform's addressable range"),
            )
        })
    }
}

// ===========================================================================
// The two traits
// ===========================================================================

/// The **body** codec: how a value's bytes are written and read back, with no
/// envelope.
///
/// Implemented here for the primitives, the standard containers, and tuples, and
/// emitted by `#[derive(Payload)]` for a struct or enum whose fields are themselves
/// `Codec`. A type that also carries a
/// [`StableName`] is automatically a [`Payload`].
#[diagnostic::on_unimplemented(
    message = "`{Self}` has no payload codec",
    label = "this type cannot be encoded",
    note = "add `#[derive(Payload)]` to `{Self}` (alongside `#[derive(StableName)]` if it is a \
            payload type in its own right), or use a type that already has a codec: the \
            integers, `bool`, `String`, `Option`, `Vec`, `BTreeMap`, tuples, and `()`"
)]
pub trait Codec: Sized {
    /// Append this value's canonical body bytes to `out`.
    ///
    /// Encoding is infallible by construction (a value that exists can always be
    /// written) and **deterministic**: the same value appends the same bytes every
    /// time, and a `BTreeMap` writes in ascending key order regardless of how it
    /// was built.
    fn encode_body(&self, out: &mut Vec<u8>);

    /// Decode one value from `cursor`, consuming exactly the bytes
    /// [`encode_body`](Codec::encode_body) wrote.
    ///
    /// # Errors
    ///
    /// A [`CodecError`] classifying the refusal: truncation, malformed bytes (an
    /// invalid tag, invalid UTF-8, a non-canonical map), or — for a composite — a
    /// [`Field`](CodecError::Field)-wrapped cause naming where it happened.
    fn decode_body(cursor: &mut Cursor<'_>) -> Result<Self, CodecError>;
}

/// A value that can cross a process boundary as bytes: a [`Codec`] with an
/// author-declared [`StableName`].
///
/// The supertrait is the whole point. `StableName` already gives every payload type
/// an identity the graph artifact records and the fingerprints hash, and it never
/// comes from [`std::any::type_name`]. That name is exactly what a decoder needs to
/// refuse a shard encoded from a different type after a refactor, so
/// [`encode`](Payload::encode) writes it into the envelope and
/// [`decode`](Payload::decode) checks it.
///
/// This trait is **blanket-implemented** for every `Codec + StableName`: the
/// envelope belongs to the framework, so no type can quietly write a different one.
///
/// ```
/// use dagr_core::{Payload, StableName};
///
/// #[derive(Debug, PartialEq, StableName, Payload)]
/// struct RowCount {
///     rows: u64,
/// }
///
/// let mut bytes = Vec::new();
/// RowCount { rows: 7 }.encode(&mut bytes);
/// assert_eq!(RowCount::decode(&bytes).unwrap(), RowCount { rows: 7 });
/// ```
pub trait Payload: Codec + StableName {
    /// Append this value's **self-describing** encoding — magic, format version,
    /// stable name, body — to `out`.
    ///
    /// The buffer is the caller's, so a caller encoding many values reuses one
    /// allocation.
    fn encode(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&MAGIC);
        out.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        encode_str(Self::STABLE_NAME, out);
        self.encode_body(out);
    }

    /// This value's self-describing encoding as a fresh `Vec` — the convenience
    /// form of [`encode`](Payload::encode).
    #[must_use]
    fn encode_to_vec(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.encode(&mut out);
        out
    }

    /// Decode a value of this type from a complete encoded payload.
    ///
    /// # Errors
    ///
    /// - [`CodecError::Malformed`] if `bytes` is not a dagr payload at all;
    /// - [`CodecError::UnsupportedVersion`] if the format version is not
    ///   [`FORMAT_VERSION`];
    /// - [`CodecError::TypeMismatch`], naming both stable names, if the envelope was
    ///   written by a **different** type — the refusal this envelope exists for;
    /// - [`CodecError::Truncated`] if the input ends early, or
    ///   [`CodecError::TrailingGarbage`] if bytes remain after a complete value;
    /// - whatever the body's own decode refused with, wrapped in
    ///   [`CodecError::Field`] when it happened inside a named field.
    fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        let mut cursor = Cursor::new(bytes);
        let magic = cursor.take(MAGIC.len(), Self::STABLE_NAME)?;
        if magic != MAGIC {
            return Err(CodecError::malformed(
                Self::STABLE_NAME,
                "these bytes are not an encoded dagr payload (wrong leading magic)",
            ));
        }
        let version = u16::decode_body_in(&mut cursor, Self::STABLE_NAME)?;
        if version != FORMAT_VERSION {
            return Err(CodecError::UnsupportedVersion {
                expected: FORMAT_VERSION,
                found: version,
            });
        }
        let name = decode_str(&mut cursor, Self::STABLE_NAME)?;
        if name != Self::STABLE_NAME {
            return Err(CodecError::TypeMismatch {
                expected: Self::STABLE_NAME,
                found: name,
            });
        }
        let value = Self::decode_body(&mut cursor)?;
        if !cursor.is_empty() {
            return Err(CodecError::TrailingGarbage {
                consumed: cursor.position(),
                total: bytes.len(),
            });
        }
        Ok(value)
    }
}

impl<T: Codec + StableName> Payload for T {}

/// Encode `value` and decode it straight back — the local **force-round-trip**
/// check, and the property every codec test asserts.
///
/// Nothing on the local fast path calls this unless the operator asks for it
/// (`--dagr.force-roundtrip`); it exists so a codec bug is catchable without a
/// cluster.
///
/// # Errors
///
/// Whatever [`Payload::decode`] refuses with — which, for a value this process just
/// encoded, means a defect in that type's codec.
pub fn round_trip<T: Payload>(value: &T) -> Result<T, CodecError> {
    let mut bytes = Vec::new();
    value.encode(&mut bytes);
    T::decode(&bytes)
}

// ===========================================================================
// Primitive impls
// ===========================================================================

/// A private extension used by the envelope and the container impls: decode a body
/// while naming the *caller's* context in any truncation error.
trait DecodeIn: Codec {
    fn decode_body_in(cursor: &mut Cursor<'_>, context: &'static str) -> Result<Self, CodecError>;
}

/// Implement [`Codec`] for a fixed-width integer as little-endian bytes — canonical
/// by construction (one value, one encoding) and word-size independent.
macro_rules! integer_codec {
    ($($ty:ty),+ $(,)?) => {$(
        impl Codec for $ty {
            fn encode_body(&self, out: &mut Vec<u8>) {
                out.extend_from_slice(&self.to_le_bytes());
            }

            fn decode_body(cursor: &mut Cursor<'_>) -> Result<Self, CodecError> {
                Self::decode_body_in(cursor, stringify!($ty))
            }
        }

        impl DecodeIn for $ty {
            fn decode_body_in(
                cursor: &mut Cursor<'_>,
                context: &'static str,
            ) -> Result<Self, CodecError> {
                const WIDTH: usize = size_of::<$ty>();
                let bytes = cursor.take(WIDTH, context)?;
                let mut buf = [0_u8; WIDTH];
                buf.copy_from_slice(bytes);
                Ok(<$ty>::from_le_bytes(buf))
            }
        }
    )+};
}

integer_codec!(u8, u16, u32, u64, u128, i8, i16, i32, i64, i128);

/// `usize` travels as a 64-bit value, so the bytes do not depend on the encoder's
/// word size; a value too large for the decoding platform is malformed rather than
/// silently truncated.
impl Codec for usize {
    fn encode_body(&self, out: &mut Vec<u8>) {
        (*self as u64).encode_body(out);
    }

    fn decode_body(cursor: &mut Cursor<'_>) -> Result<Self, CodecError> {
        let raw = u64::decode_body_in(cursor, "usize")?;
        Self::try_from(raw).map_err(|_| {
            CodecError::malformed(
                "usize",
                format!("{raw} does not fit this platform's `usize`"),
            )
        })
    }
}

/// `isize` travels as a 64-bit value, for the same reason [`usize`] does.
impl Codec for isize {
    fn encode_body(&self, out: &mut Vec<u8>) {
        (*self as i64).encode_body(out);
    }

    fn decode_body(cursor: &mut Cursor<'_>) -> Result<Self, CodecError> {
        let raw = i64::decode_body_in(cursor, "isize")?;
        Self::try_from(raw).map_err(|_| {
            CodecError::malformed(
                "isize",
                format!("{raw} does not fit this platform's `isize`"),
            )
        })
    }
}

/// `bool` is one byte, `0` or `1`. Any other byte is malformed — a decoder that
/// accepted `2` as "true" would be accepting two encodings of one value.
impl Codec for bool {
    fn encode_body(&self, out: &mut Vec<u8>) {
        out.push(u8::from(*self));
    }

    fn decode_body(cursor: &mut Cursor<'_>) -> Result<Self, CodecError> {
        match cursor.take_byte("bool")? {
            0 => Ok(false),
            1 => Ok(true),
            other => Err(CodecError::malformed(
                "bool",
                format!("expected 0 or 1, found {other}"),
            )),
        }
    }
}

/// The empty payload: no body at all. `()` is a consume-nothing task's input and an
/// effect-only node's output, and it carries the reserved unit stable name — so it
/// is a [`Payload`] like any other rather than an un-encodable special case.
impl Codec for () {
    fn encode_body(&self, _out: &mut Vec<u8>) {}

    fn decode_body(_cursor: &mut Cursor<'_>) -> Result<Self, CodecError> {
        Ok(())
    }
}

/// Append `value`'s UTF-8 bytes behind a `u64` length prefix.
fn encode_str(value: &str, out: &mut Vec<u8>) {
    encode_len(value.len(), out);
    out.extend_from_slice(value.as_bytes());
}

/// Append a length or count as a little-endian `u64`.
fn encode_len(len: usize, out: &mut Vec<u8>) {
    out.extend_from_slice(&(len as u64).to_le_bytes());
}

/// Decode a length-prefixed UTF-8 string, naming `context` in any refusal.
fn decode_str(cursor: &mut Cursor<'_>, context: &'static str) -> Result<String, CodecError> {
    let len = cursor.take_len(context)?;
    let bytes = cursor.take(len, context)?;
    std::str::from_utf8(bytes)
        .map(str::to_owned)
        .map_err(|e| CodecError::malformed_from(context, "the bytes are not valid UTF-8", e))
}

/// A `String` is a `u64` byte-length prefix and its UTF-8 bytes.
impl Codec for String {
    fn encode_body(&self, out: &mut Vec<u8>) {
        encode_str(self, out);
    }

    fn decode_body(cursor: &mut Cursor<'_>) -> Result<Self, CodecError> {
        decode_str(cursor, "String")
    }
}

/// An `Option` is a one-byte tag (`0` absent, `1` present) and, when present, the
/// inner body. Any other tag is malformed.
impl<T: Codec> Codec for Option<T> {
    fn encode_body(&self, out: &mut Vec<u8>) {
        match self {
            None => out.push(0),
            Some(value) => {
                out.push(1);
                value.encode_body(out);
            }
        }
    }

    fn decode_body(cursor: &mut Cursor<'_>) -> Result<Self, CodecError> {
        match cursor.take_byte("Option")? {
            0 => Ok(None),
            1 => Ok(Some(T::decode_body(cursor)?)),
            other => Err(CodecError::malformed(
                "Option",
                format!("expected the tag 0 or 1, found {other}"),
            )),
        }
    }
}

/// A `Vec` is a `u64` element count followed by each element's body, in order.
impl<T: Codec> Codec for Vec<T> {
    fn encode_body(&self, out: &mut Vec<u8>) {
        encode_len(self.len(), out);
        for element in self {
            element.encode_body(out);
        }
    }

    fn decode_body(cursor: &mut Cursor<'_>) -> Result<Self, CodecError> {
        let count = cursor.take_len("Vec")?;
        check_element_count::<T>(count, cursor, "Vec")?;
        let mut values = Self::with_capacity(count.min(cursor.remaining() + 1));
        for _ in 0..count {
            values.push(T::decode_body(cursor)?);
        }
        Ok(values)
    }
}

/// Refuse an element count the remaining input cannot possibly hold, so a corrupt
/// length prefix is a [truncation](CodecError::Truncated) rather than an
/// allocation the decoder makes on the encoder's word.
///
/// A **zero-sized** element type is exempt: its encoding legitimately carries no
/// bytes per element, so a count larger than the remaining input is a valid
/// encoding of that type rather than corruption.
fn check_element_count<T>(
    count: usize,
    cursor: &Cursor<'_>,
    context: &'static str,
) -> Result<(), CodecError> {
    if size_of::<T>() > 0 && count > cursor.remaining() {
        return Err(CodecError::Truncated {
            context,
            needed: count,
            available: cursor.remaining(),
        });
    }
    Ok(())
}

/// A `BTreeMap` is a `u64` entry count followed by `(key, value)` bodies in
/// **ascending key order** — canonical, so two maps holding the same entries encode
/// identically whatever order they were built in.
///
/// Decoding **enforces** that order: a non-ascending or duplicate-keyed encoding is
/// malformed rather than quietly accepted, because accepting it would mean two byte
/// strings for one value.
impl<K: Codec + Ord, V: Codec> Codec for BTreeMap<K, V> {
    fn encode_body(&self, out: &mut Vec<u8>) {
        encode_len(self.len(), out);
        // `BTreeMap`'s iteration order IS ascending key order, so the canonical form
        // costs nothing — insertion order cannot reach the bytes.
        for (key, value) in self {
            key.encode_body(out);
            value.encode_body(out);
        }
    }

    fn decode_body(cursor: &mut Cursor<'_>) -> Result<Self, CodecError> {
        let count = cursor.take_len("BTreeMap")?;
        check_element_count::<K>(count, cursor, "BTreeMap")?;
        let mut map = Self::new();
        for _ in 0..count {
            let key = K::decode_body(cursor)?;
            // The already-decoded entries are exactly the ones with smaller keys, so
            // the map's own last key is the predecessor to compare against — no clone
            // of the key type is needed to enforce the canonical order.
            if map.last_key_value().is_some_and(|(prior, _)| *prior >= key) {
                return Err(CodecError::malformed(
                    "BTreeMap",
                    "the entries are not in strictly ascending key order (a canonical \
                     encoding has exactly one byte string per value)",
                ));
            }
            let value = V::decode_body(cursor)?;
            map.insert(key, value);
        }
        Ok(map)
    }
}

/// Implement [`Codec`] for a tuple of the given arity: each element's body in
/// declaration order, nothing else. A tuple has no author-declared stable name, so
/// it is composite *data* (`Codec`) and never a top-level [`Payload`].
macro_rules! tuple_codec {
    ($($ty:ident => $idx:tt),+) => {
        impl<$($ty: Codec),+> Codec for ($($ty,)+) {
            fn encode_body(&self, out: &mut Vec<u8>) {
                $(self.$idx.encode_body(out);)+
            }

            fn decode_body(cursor: &mut Cursor<'_>) -> Result<Self, CodecError> {
                Ok(($($ty::decode_body(cursor)?,)+))
            }
        }
    };
}

tuple_codec!(T0 => 0);
tuple_codec!(T0 => 0, T1 => 1);
tuple_codec!(T0 => 0, T1 => 1, T2 => 2);
tuple_codec!(T0 => 0, T1 => 1, T2 => 2, T3 => 3);
tuple_codec!(T0 => 0, T1 => 1, T2 => 2, T3 => 3, T4 => 4);
tuple_codec!(T0 => 0, T1 => 1, T2 => 2, T3 => 3, T4 => 4, T5 => 5);
tuple_codec!(T0 => 0, T1 => 1, T2 => 2, T3 => 3, T4 => 4, T5 => 5, T6 => 6);
tuple_codec!(T0 => 0, T1 => 1, T2 => 2, T3 => 3, T4 => 4, T5 => 5, T6 => 6, T7 => 7);
