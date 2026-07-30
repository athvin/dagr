//! `ReadError` carries its real cause. Written first, TDD.
//!
//! `read_records` refuses a corrupt event stream, and until this test the refusal
//! kept only a line index: the `serde_json::Error` that actually explained *what*
//! was wrong — and at which column — was discarded at construction, so no caller
//! could recover it even in principle. dagr's whole pitch is explaining a run
//! after the fact; a truncated chain on the read path is exactly the diagnostic
//! that sends an operator back into the logs.

use std::error::Error;

use dagr_artifact::event_stream::read_records;

/// A stream whose **non-final** line is not valid JSON: the one corruption
/// `read_records` refuses (the trailing partial is tolerated separately).
const CORRUPT: &[u8] =
    b"{\"kind\":\"run-started\"}\n{not json at all}\n{\"kind\":\"run-finished\"}\n";

/// **The chain reaches the deserializer.** Walking `Error::source()` from the
/// refusal must arrive at the `serde_json::Error` that rejected the line, so the
/// parser's own diagnostic (line/column, what it expected) survives the wrap.
#[test]
fn read_error_exposes_the_deserializer_error_through_source() {
    let err = read_records(CORRUPT).expect_err("a corrupt non-final line is refused");

    let source = err
        .source()
        .expect("a ReadError wraps a real deserializer error; source() must expose it");
    assert!(
        source.downcast_ref::<serde_json::Error>().is_some(),
        "the cause must be the serde_json::Error itself, not a stringified copy of it: {source}"
    );
    assert!(
        !source.to_string().is_empty(),
        "the wrapped cause must carry the deserializer's own diagnostic"
    );
}

/// **The line index survives.** Carrying the cause must not cost the information
/// the type already provided: the offending line is still named, and it is the
/// *non-final* one.
#[test]
fn read_error_still_names_the_offending_line() {
    let err = read_records(CORRUPT).expect_err("a corrupt non-final line is refused");
    assert_eq!(err.line, 1, "the zero-based index of the offending line");
    assert!(
        err.to_string().contains('1'),
        "the Display form still names the line: {err}"
    );
}

/// **A tolerated trailing partial is still not an error.** The chain work must not
/// widen what counts as corruption: an unterminated final line is tolerated
/// exactly as before.
#[test]
fn a_trailing_partial_is_still_tolerated() {
    let stream = read_records(b"{\"kind\":\"run-started\"}\n{\"kind\":\"partia")
        .expect("one unterminated tail is tolerated, not a corruption");
    assert_eq!(stream.records.len(), 1);
    assert!(stream.trailing_partial_discarded);
}
