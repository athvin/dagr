//! The T4 §6 **canonical JSON** serializer — the single byte-form every dagr
//! artifact and record is compared for equality on (arch.md C19/C20/C22; the T4
//! ADR, `docs/implementation/017-T4-...` §6).
//!
//! Canonical form is what makes "two emissions of the same record are
//! byte-identical" (C19 event stream, C20 graph artifact) a *byte* fact rather
//! than a structural one. It is:
//!
//! - **object keys sorted** lexicographically by byte order (`serde_json` does
//!   not sort keys itself — this module does),
//! - **compact** — no insignificant whitespace,
//! - **integers only** — every dagr numeric field is an integer (T4 §6), so no
//!   float-formatting ambiguity arises,
//! - **minimally escaped** — only what JSON requires (`"`, `\`, control chars
//!   `U+0000`–`U+001F`); printable non-ASCII is emitted literally as UTF-8, never
//!   `\u`-escaped.
//!
//! Both the C19 event-stream writer (T19) and the C20 graph-artifact emitter
//! (T40) serialize through [`to_canonical_string`], so their byte-identity
//! guarantees rest on one authoritative canonicalizer rather than two that might
//! drift.

use std::collections::BTreeMap;

use serde_json::Value;

/// Serialize `value` to its **canonical** JSON string (T4 §6): object keys sorted
/// lexicographically, compact, integers only, minimally escaped. Two canonical
/// serializations of equal [`Value`]s are byte-identical.
#[must_use]
pub fn to_canonical_string(value: &Value) -> String {
    let mut out = String::new();
    write_canonical(value, &mut out);
    out
}

/// Write `value` in the T4 canonical form into `out` (see [`to_canonical_string`]).
pub(crate) fn write_canonical(value: &Value, out: &mut String) {
    match value {
        Value::Object(map) => {
            out.push('{');
            // BTreeMap gives lexicographic (byte-order) key ordering.
            let sorted: BTreeMap<&String, &Value> = map.iter().collect();
            for (i, (k, v)) in sorted.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_json_string(k, out);
                out.push(':');
                write_canonical(v, out);
            }
            out.push('}');
        }
        Value::Array(items) => {
            out.push('[');
            for (i, v) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_canonical(v, out);
            }
            out.push(']');
        }
        Value::String(s) => write_json_string(s, out),
        // Booleans, integers, and null render identically to serde_json's compact
        // form; all dagr numeric fields are integers (T4 §6), so no float
        // formatting hazard arises.
        other => out.push_str(&other.to_string()),
    }
}

/// Emit a JSON string with minimal, deterministic escaping (T4 §6): escape only
/// what JSON requires (`"`, `\`, and control chars `U+0000`–`U+001F`); non-ASCII
/// printable characters are emitted literally as UTF-8, never `\u`-escaped.
pub(crate) fn write_json_string(s: &str, out: &mut String) {
    out.push('"');
    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0c}' => out.push_str("\\f"),
            c if (c as u32) < 0x20 => {
                use std::fmt::Write as _;
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
}
