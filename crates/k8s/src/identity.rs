//! **Identity** — labels are lossy selectors, annotations are authoritative.
//!
//! Kubernetes caps a label *value* at 63 characters. dagr's run ids are
//! 36-character `UUIDv7` values **before** any node name is appended, so the
//! ceiling binds immediately and a label simply cannot carry identity. Airflow
//! learned the same thing and reads identity from annotations on every
//! reconciliation path; so does dagr.
//!
//! The split, exactly:
//!
//! - **Labels** ([`AttemptIdentity::labels`]) — selectors *only*: the run id, a
//!   **node fingerprint** (see below), the attempt number, and an owner key. Plus
//!   a completion tombstone key that is written by the mechanism that retires a
//!   pod, not here.
//! - **Annotations** ([`AttemptIdentity::annotations`]) — authoritative and
//!   unbounded: the full node name, the pipeline name, both fingerprints, the
//!   tool version, and the image digest.
//!
//! # The node label is a truncation, not a digest — deliberately
//!
//! The obvious encoding for a "node fingerprint" is a hash, and it is the wrong
//! one here. A digest is *also* irreversible, so it would not let a reader
//! recover the node name either — but it would look unique, and code that looks
//! at a unique-looking label eventually trusts it. What this project needs is the
//! opposite: a label that is visibly lossy, so every path is forced through the
//! annotation. Truncation is that, and it has one more property a digest lacks —
//! a collision is *constructible*, so the test that proves annotations are
//! load-bearing can actually be written.
//!
//! The consequence is stated rather than hidden: two nodes whose names share a
//! long prefix carry the same node label. That is fine, because the label is a
//! selector — the observer narrows by run id and reads identity from the
//! annotation.
//!
//! # The key prefix
//!
//! Every key is prefixed with `dagr.io/`. The prefix is a **namespace**, not a
//! claim of DNS ownership: it keeps dagr's keys from colliding with an operator's
//! own, which is the reason Kubernetes has the form at all. It is one constant
//! here if it ever needs to change.

use std::collections::BTreeMap;
use std::fmt;

/// The key namespace every dagr label and annotation lives under.
pub const KEY_PREFIX: &str = "dagr.io/";

/// Label: the run this pod belongs to. A `UUIDv7` is 36 characters and needs no
/// truncation, which is what makes it the one field the selector can rely on.
pub const LABEL_RUN_ID: &str = "dagr.io/run-id";
/// Label: the **lossy** node fingerprint. See the module docs — this narrows a
/// selector and is never identity.
pub const LABEL_NODE: &str = "dagr.io/node";
/// Label: the attempt number. An integer fits a label value losslessly, so this
/// one *is* authoritative by construction.
pub const LABEL_ATTEMPT: &str = "dagr.io/attempt";
/// Label: the owner key. Adoption after an orchestrator restart is a labels-only
/// patch of this key; this crate ships the key, not the mechanism.
pub const LABEL_OWNER: &str = "dagr.io/owner";
/// Label: the completion tombstone. Present with [`TOMBSTONE_VALUE`] on a pod
/// that has been retired, so an adoption selector can filter it out. This crate
/// ships the key and reads it; the mechanism that writes it does not live here.
pub const LABEL_COMPLETE: &str = "dagr.io/complete";

/// Annotation: the **full** node name, authoritative.
pub const ANNOTATION_NODE: &str = "dagr.io/node-name";
/// Annotation: the pipeline name.
pub const ANNOTATION_PIPELINE: &str = "dagr.io/pipeline";
/// Annotation: the structural fingerprint of the graph that launched this pod. A
/// pod whose value disagrees with the observer's was launched by a different
/// program, and is reported foreign rather than attributed.
pub const ANNOTATION_STRUCTURAL_FINGERPRINT: &str = "dagr.io/structural-fingerprint";
/// Annotation: the policy hash.
pub const ANNOTATION_POLICY_HASH: &str = "dagr.io/policy-hash";
/// Annotation: the tool version that launched this pod.
pub const ANNOTATION_TOOL_VERSION: &str = "dagr.io/tool-version";
/// Annotation: the image digest the attempt ran.
pub const ANNOTATION_IMAGE_DIGEST: &str = "dagr.io/image-digest";

/// The platform's label-value ceiling. Not ours to choose, and the reason this
/// module exists.
pub const LABEL_VALUE_MAX: usize = 63;

/// The value [`LABEL_COMPLETE`] carries when present.
pub const TOMBSTONE_VALUE: &str = "true";

/// The character a disallowed one is replaced with when sanitizing a label value.
const REPLACEMENT: char = '-';

/// What a label value degenerates to when nothing in the input survives
/// sanitization, so a selector is always constructible.
const EMPTY_LABEL: &str = "x";

/// The identity triple an attempt is keyed by: `(run, node, attempt)`.
///
/// The same triple the submission record and the adoption idempotency key use. A
/// run id alone cannot distinguish try 1 from try 3.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AttemptKey {
    /// The run this attempt belongs to.
    pub run_id: String,
    /// The **full** node name, as the annotation carries it.
    pub node: String,
    /// The attempt number, 1-based.
    pub attempt: u32,
}

impl AttemptKey {
    /// Build a key from its three parts.
    #[must_use]
    pub fn new(run_id: impl Into<String>, node: impl Into<String>, attempt: u32) -> Self {
        Self {
            run_id: run_id.into(),
            node: node.into(),
            attempt,
        }
    }
}

impl fmt::Display for AttemptKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}#{}", self.run_id, self.node, self.attempt)
    }
}

/// Everything a pod carries about the attempt it runs.
///
/// [`labels`](Self::labels) and [`annotations`](Self::annotations) split it the
/// way ADR 115 §4 requires; [`identify`] is the inverse, and reads the
/// authoritative half.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttemptIdentity {
    /// The attempt this pod runs.
    pub key: AttemptKey,
    /// The pipeline the node belongs to.
    pub pipeline: String,
    /// The graph's structural fingerprint.
    pub structural_fingerprint: String,
    /// The graph's policy hash.
    pub policy_hash: String,
    /// The tool version that launched the attempt.
    pub tool_version: String,
    /// The image digest the attempt runs.
    pub image_digest: String,
    /// The orchestrator that owns this pod.
    pub owner: String,
}

impl AttemptIdentity {
    /// The **selector** half: four keys, every value inside the platform ceiling.
    ///
    /// The completion tombstone is deliberately absent — a live pod is not
    /// complete, and the key is written by whatever retires the pod.
    #[must_use]
    pub fn labels(&self) -> BTreeMap<String, String> {
        BTreeMap::from([
            (LABEL_RUN_ID.to_string(), label_value(&self.key.run_id)),
            (LABEL_NODE.to_string(), node_label(&self.key.node)),
            (LABEL_ATTEMPT.to_string(), self.key.attempt.to_string()),
            (LABEL_OWNER.to_string(), label_value(&self.owner)),
        ])
    }

    /// The **authoritative** half: the six fields a reconciliation path reads.
    ///
    /// Annotation values are not length-bounded, so nothing here is truncated and
    /// nothing here is lossy.
    #[must_use]
    pub fn annotations(&self) -> BTreeMap<String, String> {
        BTreeMap::from([
            (ANNOTATION_NODE.to_string(), self.key.node.clone()),
            (ANNOTATION_PIPELINE.to_string(), self.pipeline.clone()),
            (
                ANNOTATION_STRUCTURAL_FINGERPRINT.to_string(),
                self.structural_fingerprint.clone(),
            ),
            (ANNOTATION_POLICY_HASH.to_string(), self.policy_hash.clone()),
            (
                ANNOTATION_TOOL_VERSION.to_string(),
                self.tool_version.clone(),
            ),
            (
                ANNOTATION_IMAGE_DIGEST.to_string(),
                self.image_digest.clone(),
            ),
        ])
    }
}

/// The identity read back off a pod — the inverse of [`AttemptIdentity`], plus
/// the one fact only the live object carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedIdentity {
    /// The attempt this pod runs, with the **full** node name from the annotation.
    pub key: AttemptKey,
    /// The pipeline the node belongs to.
    pub pipeline: String,
    /// The graph's structural fingerprint, as this pod claims it.
    pub structural_fingerprint: String,
    /// The graph's policy hash.
    pub policy_hash: String,
    /// The tool version that launched the attempt.
    pub tool_version: String,
    /// The image digest the attempt runs.
    pub image_digest: String,
    /// The orchestrator that owns this pod.
    pub owner: String,
    /// Whether the completion tombstone is present.
    pub complete: bool,
}

/// Why a pod could not be identified.
///
/// Each variant names the key at fault, because the alternative to saying so is
/// attributing somebody else's work to a waiter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdentityError {
    /// A required label is absent.
    MissingLabel {
        /// The label key that was expected.
        key: &'static str,
    },
    /// A required annotation is absent.
    MissingAnnotation {
        /// The annotation key that was expected.
        key: &'static str,
    },
    /// The attempt label is present but is not a number.
    MalformedAttempt {
        /// The offending value, verbatim.
        value: String,
    },
    /// The node label is not the label the annotation derives, so the two halves
    /// disagree. Someone edited one of them, and guessing which is a worse answer
    /// than saying so.
    NodeLabelMismatch {
        /// The label as found on the pod.
        label: String,
        /// The label the annotation's full node name derives.
        derived: String,
    },
}

impl fmt::Display for IdentityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IdentityError::MissingLabel { key } => {
                write!(f, "pod carries no `{key}` label")
            }
            IdentityError::MissingAnnotation { key } => {
                write!(
                    f,
                    "pod carries no `{key}` annotation, and annotations are authoritative"
                )
            }
            IdentityError::MalformedAttempt { value } => {
                write!(
                    f,
                    "`{LABEL_ATTEMPT}` is `{value}`, which is not an attempt number"
                )
            }
            IdentityError::NodeLabelMismatch { label, derived } => {
                write!(
                    f,
                    "`{LABEL_NODE}` is `{label}` but the annotated node name derives `{derived}` \
                     — the two halves of this pod's identity disagree"
                )
            }
        }
    }
}

impl std::error::Error for IdentityError {}

/// Whether `value` is a syntactically valid Kubernetes label value.
///
/// The platform's rule: at most 63 characters; alphanumerics, `-`, `_` and `.`;
/// beginning and ending alphanumeric. (The empty string is also legal, but dagr
/// never emits one — an empty selector value would match by accident.)
#[must_use]
pub fn is_valid_label_value(value: &str) -> bool {
    if value.is_empty() || value.len() > LABEL_VALUE_MAX {
        return false;
    }
    let first = value.chars().next().unwrap_or('\0');
    let last = value.chars().next_back().unwrap_or('\0');
    if !first.is_ascii_alphanumeric() || !last.is_ascii_alphanumeric() {
        return false;
    }
    value
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.')
}

/// Sanitize and truncate `raw` into a legal label value.
///
/// Every character the platform disallows becomes `-`, a run of them collapses to
/// one (so `warehouse::ingest` reads as `warehouse-ingest` rather than
/// `warehouse--ingest`), the result is cut to the 63-character ceiling, and the
/// ends are trimmed back to an alphanumeric. Input that leaves nothing behind
/// degenerates to a single legal character rather than to the empty string.
#[must_use]
pub fn label_value(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len().min(LABEL_VALUE_MAX));
    let mut previous_was_replacement = false;
    for c in raw.chars() {
        if c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' {
            out.push(c);
            previous_was_replacement = false;
        } else {
            if !previous_was_replacement {
                out.push(REPLACEMENT);
            }
            previous_was_replacement = true;
        }
    }

    // Truncate on a character boundary. Everything retained above is ASCII, so
    // byte and character indices agree and a simple truncate is safe.
    out.truncate(LABEL_VALUE_MAX);

    let trimmed = out.trim_matches(|c: char| !c.is_ascii_alphanumeric());
    if trimmed.is_empty() {
        EMPTY_LABEL.to_string()
    } else {
        trimmed.to_string()
    }
}

/// The **lossy** node fingerprint a pod carries as a selector.
///
/// It is [`label_value`] — that is the whole encoding, and the module docs say
/// why a digest was rejected. Two node names sharing a long enough prefix produce
/// the same value, and the annotation is what tells them apart.
#[must_use]
pub fn node_label(node: &str) -> String {
    label_value(node)
}

/// The label selector that narrows a watch to one run's pods.
///
/// One term. Narrowing further would be a false economy: the observer reads
/// identity from annotations anyway, and a selector that named the lossy node
/// label would silently drop a pod whose name sanitized differently.
#[must_use]
pub fn run_selector(run_id: &str) -> String {
    format!("{LABEL_RUN_ID}={}", label_value(run_id))
}

/// Read a pod's identity, authoritatively.
///
/// The full node name comes from the annotation; the attempt number comes from
/// the label, where an integer is lossless. The two halves are cross-checked: a
/// node label that is not what the annotation derives means one of them was
/// edited, and that is reported rather than resolved by preference.
///
/// # Errors
///
/// Returns [`IdentityError`] naming the key at fault when a required label or
/// annotation is absent, when the attempt label is not a number, or when the node
/// label and the node annotation disagree.
pub fn identify(
    labels: &BTreeMap<String, String>,
    annotations: &BTreeMap<String, String>,
) -> Result<ObservedIdentity, IdentityError> {
    let annotation = |key: &'static str| -> Result<String, IdentityError> {
        annotations
            .get(key)
            .cloned()
            .ok_or(IdentityError::MissingAnnotation { key })
    };
    let label = |key: &'static str| -> Result<String, IdentityError> {
        labels
            .get(key)
            .cloned()
            .ok_or(IdentityError::MissingLabel { key })
    };

    let node = annotation(ANNOTATION_NODE)?;
    let run_id = label(LABEL_RUN_ID)?;
    let raw_attempt = label(LABEL_ATTEMPT)?;
    let attempt = raw_attempt
        .parse::<u32>()
        .map_err(|_| IdentityError::MalformedAttempt {
            value: raw_attempt.clone(),
        })?;

    let node_label_value = label(LABEL_NODE)?;
    let derived = node_label(&node);
    if node_label_value != derived {
        return Err(IdentityError::NodeLabelMismatch {
            label: node_label_value,
            derived,
        });
    }

    Ok(ObservedIdentity {
        key: AttemptKey::new(run_id, node, attempt),
        pipeline: annotation(ANNOTATION_PIPELINE)?,
        structural_fingerprint: annotation(ANNOTATION_STRUCTURAL_FINGERPRINT)?,
        policy_hash: annotation(ANNOTATION_POLICY_HASH)?,
        tool_version: annotation(ANNOTATION_TOOL_VERSION)?,
        image_digest: annotation(ANNOTATION_IMAGE_DIGEST)?,
        owner: label(LABEL_OWNER)?,
        complete: labels.get(LABEL_COMPLETE).map(String::as_str) == Some(TOMBSTONE_VALUE),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_key_lives_under_one_namespace() {
        for key in [
            LABEL_RUN_ID,
            LABEL_NODE,
            LABEL_ATTEMPT,
            LABEL_OWNER,
            LABEL_COMPLETE,
            ANNOTATION_NODE,
            ANNOTATION_PIPELINE,
            ANNOTATION_STRUCTURAL_FINGERPRINT,
            ANNOTATION_POLICY_HASH,
            ANNOTATION_TOOL_VERSION,
            ANNOTATION_IMAGE_DIGEST,
        ] {
            assert!(
                key.starts_with(KEY_PREFIX),
                "{key} is outside the namespace"
            );
        }
    }

    #[test]
    fn a_run_selector_is_one_term_over_the_run_id() {
        assert_eq!(
            run_selector("0197f3a8-6b21-7c4e-9d55-1f2a3b4c5d6e"),
            "dagr.io/run-id=0197f3a8-6b21-7c4e-9d55-1f2a3b4c5d6e"
        );
    }

    #[test]
    fn sanitization_collapses_runs_and_trims_the_ends() {
        assert_eq!(label_value("warehouse::ingest"), "warehouse-ingest");
        assert_eq!(label_value("--edges--"), "edges");
        assert_eq!(label_value("  "), EMPTY_LABEL);
        assert!(is_valid_label_value(&label_value("map<String, Vec<u8>>")));
    }

    #[test]
    fn the_empty_string_is_not_a_value_dagr_emits() {
        assert!(!is_valid_label_value(""));
        assert!(!is_valid_label_value(&"a".repeat(LABEL_VALUE_MAX + 1)));
        assert!(!is_valid_label_value("-leading"));
        assert!(!is_valid_label_value("trailing-"));
        assert!(!is_valid_label_value("has space"));
    }
}
