//! The **canonical JSON** serializer — the single byte-form every dagr
//! artifact and record is compared for equality on.
//!
//! Canonical form is what makes "two emissions of the same record are
//! byte-identical" (for the event stream and graph artifact) a *byte* fact
//! rather than a structural one. It is:
//!
//! - **object keys sorted** lexicographically by byte order (`serde_json` does
//!   not sort keys itself — this module does),
//! - **compact** — no insignificant whitespace,
//! - **numbers via `serde_json`'s deterministic formatter** — most dagr numeric
//!   fields are integers, but a node-metric value is a JSON `number`
//!   (`schemas/run/v1.schema.json` types `metrics` as `number`, not
//!   `integer`) and MAY be non-integer. Non-integer numbers reach the output only
//!   via `serde_json::Value::to_string` below, which formats floats with **ryu**
//!   (locale-independent, shortest round-trip, byte-stable) — so the canonical
//!   output stays deterministic and byte-identical for non-integer values too, not
//!   just integers,
//! - **minimally escaped** — only what JSON requires (`"`, `\`, control chars
//!   `U+0000`–`U+001F`); printable non-ASCII is emitted literally as UTF-8, never
//!   `\u`-escaped.
//!
//! Both the event-stream writer and the graph-artifact emitter serialize through
//! [`to_canonical_string`], so their byte-identity guarantees rest on one
//! authoritative canonicalizer rather than two that might drift.

use std::collections::BTreeMap;

use serde_json::Value;

/// Serialize `value` to its **canonical** JSON string: object keys sorted
/// lexicographically, compact, minimally escaped, numbers via `serde_json`'s
/// deterministic (ryu-backed, byte-stable) formatter — so even a non-integer
/// number (e.g. a metric value) serializes deterministically. Two
/// canonical serializations of equal [`Value`]s are byte-identical.
#[must_use]
pub fn to_canonical_string(value: &Value) -> String {
    let mut out = String::new();
    write_canonical(value, &mut out);
    out
}

/// Write `value` in canonical form into `out` (see [`to_canonical_string`]).
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
        // Booleans, numbers, and null render identically to serde_json's compact
        // form. Integers format exactly; a non-integer number — e.g. a metric
        // value, typed `number` (not `integer`) by the run schema — formats via
        // serde_json's ryu-backed float writer (locale-independent, shortest
        // round-trip, byte-stable), so no float-formatting nondeterminism
        // arises for non-integer values either.
        other => out.push_str(&other.to_string()),
    }
}

/// Emit a JSON string with minimal, deterministic escaping: escape only
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
