//! **Ownership, adoption, tombstones and revocation** — the pure half (M10,
//! T109, ADR 115 §5).
//!
//! T108's submission is idempotent inside one live orchestrator process. This
//! module is about the process **dying**: it is killed, evicted or redeployed
//! while pods are still running. Those pods keep working — they are separate
//! processes on separate machines, unaware their submitter is gone — and the next
//! invocation has to decide what they are.
//!
//! Both naive answers are expensive in exactly the way dagr exists to prevent.
//! Resubmitting every unfinished node duplicates work that is already running
//! and, for a task with side effects, duplicates the side effects. Ignoring the
//! pods leaks them and double-counts cluster capacity.
//!
//! # The mechanics, and why each one is shaped the way it is
//!
//! **Ownership lives in a mutable label, patched in place.** Adoption is a
//! labels-only patch rewriting [`LABEL_OWNER`](crate::identity::LABEL_OWNER) —
//! *never* a pod recreation, because recreating a pod is running the attempt
//! twice, which is the thing this whole module is here to avoid. So the patch
//! this module produces touches exactly one key ([`adoption_patch`]), and nothing
//! here can express a wider one.
//!
//! **A consumed outcome is tombstoned with the key the discovery selector
//! excludes.** One constant, read by [`adoption_selector`] and written by
//! [`tombstone_patch`] — not two that have to be kept in step. That is what makes
//! "a finished attempt is never adopted twice" hold even when the pod's deletion
//! was deferred or failed.
//!
//! **Revocation is a deliberate two-step patch-then-delete.** Clearing the owner
//! first is what lets a watcher tell an orchestrator-initiated teardown
//! ([`DeletionOrigin::Revoked`]) from an external deletion. The *ordering* is the
//! mechanism: a delete issued first is indistinguishable from somebody else's.
//! Clearing the owner has a second, load-bearing consequence — an unowned pod no
//! longer [identifies](crate::identity::identify) as an attempt's, so a revoked
//! duplicate's disappearance can never retire the waiter of the pod that *was*
//! adopted for the same attempt.
//!
//! # What this module does not do
//!
//! It performs no I/O. It lists nothing, patches nothing, deletes nothing: it
//! decides, and `dagr_cli::adoption` acts, for the same ADR 004 reason the
//! observer is split this way. And it never crosses a run boundary — adoption is
//! scoped to one run id, because the attempt key is scoped to a run and resume
//! mints a *new* run id, so a resumed run adopting the killed run's pods would
//! blur two runs' event streams.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use crate::api::{PodPhase, PodSnapshot};
use crate::identity::{
    AttemptKey, IdentityError, LABEL_COMPLETE, LABEL_OWNER, ObservedIdentity, TOMBSTONE_VALUE,
    identify, label_value, run_selector,
};

/// A **labels-only merge patch**: a key set to `Some(value)`, or removed with
/// `None`.
///
/// `None` is the JSON-merge-patch null that *removes* a key, which is what
/// "clear the owner label" means on the wire. The type is a map of options rather
/// than two separate maps so a patch is one value with one ordering, and a test
/// can assert the whole of what a patch writes by looking at its keys.
pub type LabelPatch = BTreeMap<String, Option<String>>;

/// The label selector a restart discovers this run's reclaimable pods with.
///
/// Two terms: the run, and the **absence** of the completion tombstone. The
/// second is the one that makes discovery safe to run repeatedly — a pod whose
/// outcome has already been consumed is excluded by the very key
/// [`tombstone_patch`] writes, so a finished attempt is never adopted twice even
/// if its deletion never happened.
#[must_use]
pub fn adoption_selector(run_id: &str) -> String {
    format!(
        "{},{LABEL_COMPLETE}!={TOMBSTONE_VALUE}",
        run_selector(run_id)
    )
}

/// The build a discovered pod's annotations must describe for it to be this
/// program's work.
///
/// Three fields, because a pod can disagree with this binary in three independent
/// ways and an operator reading a refusal acts differently on each: a different
/// *graph* (structural fingerprint), a different *tool* (version), and a
/// different *program* (image digest).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildIdentity {
    /// The graph's structural fingerprint.
    pub structural_fingerprint: String,
    /// The tool version — the comparability token.
    pub tool_version: String,
    /// The image digest the orchestrator itself is running.
    pub image_digest: String,
}

impl BuildIdentity {
    /// Build the identity a discovered pod is compared against.
    #[must_use]
    pub fn new(
        structural_fingerprint: impl Into<String>,
        tool_version: impl Into<String>,
        image_digest: impl Into<String>,
    ) -> Self {
        Self {
            structural_fingerprint: structural_fingerprint.into(),
            tool_version: tool_version.into(),
            image_digest: image_digest.into(),
        }
    }
}

/// Why a discovered pod is not adoptable.
///
/// Every variant that compares two values carries **both**, because "this pod is
/// not ours" is not actionable and "this pod claims `sf-abc` where we are
/// `sf-def`" is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdoptionRefusal {
    /// Its outcome has already been consumed — the pod carries the completion
    /// tombstone. Never adopted, and never re-run on its account.
    Tombstoned,
    /// It belongs to a different run. Adoption is scoped to one run id.
    ForeignRun {
        /// The run doing the adopting.
        expected: String,
        /// The run the pod claims.
        found: String,
    },
    /// It was launched by a different graph.
    StructuralFingerprint {
        /// This binary's structural fingerprint.
        expected: String,
        /// The fingerprint the pod's annotation claims.
        found: String,
    },
    /// It was launched by a different tool version.
    ToolVersion {
        /// This binary's tool version.
        expected: String,
        /// The version the pod's annotation claims.
        found: String,
    },
    /// It is running a different image.
    ImageDigest {
        /// This binary's image digest.
        expected: String,
        /// The digest the pod's annotation claims.
        found: String,
    },
    /// It could not be identified at all — guessing whose it is would be worse
    /// than saying so.
    Unidentifiable(IdentityError),
}

impl AdoptionRefusal {
    /// Whether the pod belongs to a **different program** — a build surface
    /// disagreed, rather than the pod being ours and merely finished or
    /// out of scope.
    ///
    /// This is the class the ticket says to report, leave alone, and fail the node
    /// over: a pod on this attempt's object name that dagr did not launch is a
    /// collision it cannot submit through.
    #[must_use]
    pub fn is_foreign_build(&self) -> bool {
        matches!(
            self,
            AdoptionRefusal::StructuralFingerprint { .. }
                | AdoptionRefusal::ToolVersion { .. }
                | AdoptionRefusal::ImageDigest { .. }
        )
    }
}

impl fmt::Display for AdoptionRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AdoptionRefusal::Tombstoned => f.write_str(
                "its outcome has already been consumed (it carries the completion \
                 tombstone), so adopting it would consume the same attempt twice",
            ),
            AdoptionRefusal::ForeignRun { expected, found } => write!(
                f,
                "it belongs to run `{found}`, not `{expected}` — adoption is scoped \
                 to one run id, because the attempt key is"
            ),
            AdoptionRefusal::StructuralFingerprint { expected, found } => write!(
                f,
                "its structural fingerprint is `{found}` and this binary's is \
                 `{expected}` — it was launched by a different graph"
            ),
            AdoptionRefusal::ToolVersion { expected, found } => write!(
                f,
                "its tool version is `{found}` and this binary's is `{expected}` — \
                 it was launched by a different tool"
            ),
            AdoptionRefusal::ImageDigest { expected, found } => write!(
                f,
                "its image digest is `{found}` and this binary's is `{expected}` — \
                 it is running a different program"
            ),
            AdoptionRefusal::Unidentifiable(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for AdoptionRefusal {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            AdoptionRefusal::Unidentifiable(err) => Some(err),
            _ => None,
        }
    }
}

/// What one discovered pod is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdoptionVerdict {
    /// This run's work, from this build: reclaim it by patching its owner.
    ///
    /// Boxed for the same reason [`Delivery::Observed`](crate::observer::Delivery)
    /// is — an authoritative identity is an order of magnitude larger than a
    /// refusal, and an unboxed enum would cost every refusal the same space.
    Adopt(Box<ObservedIdentity>),
    /// Not adoptable, and why.
    Refuse(AdoptionRefusal),
}

/// Whether `pod` is this run's reclaimable work.
///
/// The annotations are what is read — they are the authoritative half of identity
/// (ADR 115 §4) — and all three build surfaces are compared, because a pod can
/// carry this run's id and still belong to a different program.
#[must_use]
pub fn classify(pod: &PodSnapshot, run_id: &str, build: &BuildIdentity) -> AdoptionVerdict {
    match identify(&pod.labels, &pod.annotations) {
        Err(err) => AdoptionVerdict::Refuse(AdoptionRefusal::Unidentifiable(err)),
        Ok(identity) => match refusal_for(&identity, run_id, build) {
            Some(refusal) => AdoptionVerdict::Refuse(refusal),
            None => AdoptionVerdict::Adopt(Box::new(identity)),
        },
    }
}

/// Why an identified pod is not adoptable, or [`None`] when it is.
///
/// Ordered cheapest-and-most-informative first: a pod from another run is not
/// this run's business at all, a tombstoned one is already accounted for, and
/// only then do the three build surfaces get compared.
fn refusal_for(
    identity: &ObservedIdentity,
    run_id: &str,
    build: &BuildIdentity,
) -> Option<AdoptionRefusal> {
    if identity.key.run_id != label_value(run_id) && identity.key.run_id != run_id {
        return Some(AdoptionRefusal::ForeignRun {
            expected: run_id.to_string(),
            found: identity.key.run_id.clone(),
        });
    }
    if identity.complete {
        return Some(AdoptionRefusal::Tombstoned);
    }
    if identity.structural_fingerprint != build.structural_fingerprint {
        return Some(AdoptionRefusal::StructuralFingerprint {
            expected: build.structural_fingerprint.clone(),
            found: identity.structural_fingerprint.clone(),
        });
    }
    if identity.tool_version != build.tool_version {
        return Some(AdoptionRefusal::ToolVersion {
            expected: build.tool_version.clone(),
            found: identity.tool_version.clone(),
        });
    }
    if identity.image_digest != build.image_digest {
        return Some(AdoptionRefusal::ImageDigest {
            expected: build.image_digest.clone(),
            found: identity.image_digest.clone(),
        });
    }
    None
}

// ===========================================================================
// The three patches
// ===========================================================================

/// The **adoption** patch: rewrite the owner key, and nothing else.
///
/// One key is the whole point. Adoption reclaims a *running* pod, so every field
/// beyond ownership belongs to the attempt in flight and must survive untouched —
/// the annotations especially, which are identity and would make the pod
/// unattributable if they moved.
#[must_use]
pub fn adoption_patch(owner: &str) -> LabelPatch {
    LabelPatch::from([(LABEL_OWNER.to_string(), Some(label_value(owner)))])
}

/// The **revocation** patch: clear the owner key.
///
/// `None` removes the label. The delete that follows is then legible as ours
/// ([`deletion_origin`]) — which is the entire reason revocation is two steps
/// rather than one.
#[must_use]
pub fn revocation_patch() -> LabelPatch {
    LabelPatch::from([(LABEL_OWNER.to_string(), None)])
}

/// The **tombstone** patch: mark the outcome consumed with the key
/// [`adoption_selector`] excludes.
#[must_use]
pub fn tombstone_patch() -> LabelPatch {
    LabelPatch::from([(
        LABEL_COMPLETE.to_string(),
        Some(TOMBSTONE_VALUE.to_string()),
    )])
}

/// Whether a pod already carries the completion tombstone.
///
/// Read off the labels rather than through [`identify`], because the caller that
/// needs this — the submission probe, deciding whether the pod squatting on an
/// attempt's object name is adoptable — has a snapshot and no need for the rest.
#[must_use]
pub fn is_tombstoned(labels: &BTreeMap<String, String>) -> bool {
    labels.get(LABEL_COMPLETE).map(String::as_str) == Some(TOMBSTONE_VALUE)
}

// ===========================================================================
// Deletion origin
// ===========================================================================

/// Who deleted a pod, as far as its last-seen labels can say.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeletionOrigin {
    /// dagr cleared the owner and then deleted it — an orchestrator-initiated
    /// teardown.
    Revoked,
    /// It still carried an owner when it disappeared, so something else removed
    /// it: an operator, an eviction, a namespace teardown.
    External,
}

/// Read a disappearance's origin off the labels the pod carried when it went.
///
/// This is what the two-step ordering buys, and the only thing that distinguishes
/// the two events: a delete issued *before* the owner was cleared is
/// indistinguishable from somebody else's.
#[must_use]
pub fn deletion_origin(labels: &BTreeMap<String, String>) -> DeletionOrigin {
    if labels.contains_key(LABEL_OWNER) {
        DeletionOrigin::External
    } else {
        DeletionOrigin::Revoked
    }
}

// ===========================================================================
// The discovery plan
// ===========================================================================

/// A pod this run may reclaim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveredPod {
    /// The object's name.
    pub name: String,
    /// The platform's unique identifier for it — what distinguishes this object
    /// from a recreated one of the same name, and therefore the field that proves
    /// adoption did not recreate anything.
    pub uid: Option<String>,
    /// The host it is running on.
    pub host: Option<String>,
    /// Its phase at discovery. A pod already terminal here is not a special case:
    /// its outcome is consumed through the ordinary await-and-read-the-shard path.
    pub phase: PodPhase,
}

/// A pod that was not adopted, and why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefusedPod {
    /// The object's name.
    pub name: String,
    /// Why.
    pub refusal: AdoptionRefusal,
}

/// What one discovery pass decided about every pod it found.
///
/// Every discovered pod lands in exactly one bucket. A pod that fell through would
/// be one nobody is waiting for and nobody will clean up, which is the leak this
/// ticket exists to close.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DiscoveryPlan {
    /// One pod per attempt key, to be reclaimed by an owner patch.
    pub adopt: BTreeMap<AttemptKey, DiscoveredPod>,
    /// Duplicate pods for an attempt already claimed above, to be revoked. Sorted,
    /// so a plan is a value rather than a transcript of the listing order.
    pub revoke: Vec<String>,
    /// Attempts whose only pod belongs to a different program. The node fails with
    /// a classified error; the pod is left running.
    pub refuse: BTreeMap<AttemptKey, RefusedPod>,
    /// Attempts whose outcome has already been consumed. Neither adopted nor
    /// revoked — the record already has them, and deleting them is a cleanup
    /// decision rather than this pass's.
    pub tombstoned: BTreeMap<AttemptKey, String>,
    /// Attempts of nodes that will not run in this invocation — a
    /// `satisfied-from-prior` node has no runner, so there is nothing to wait on
    /// and nothing to reclaim. Reported, and left alone.
    pub unclaimed: BTreeMap<AttemptKey, String>,
    /// Pods that are not this run's business at all: another run's, unidentifiable,
    /// or a foreign-build duplicate of an attempt that *was* adopted. Reported, and
    /// left alone.
    pub ignored: Vec<RefusedPod>,
}

/// Sort every discovered pod into exactly one outcome.
///
/// `must_run` is the set of node names that have a runner in this invocation —
/// resume's must-run set, or every node for an ordinary run. A pod for a node
/// outside it is `unclaimed`: there is no attempt to reclaim, and deleting a pod
/// dagr never intended to wait for is not this pass's call.
///
/// # Ambiguity
///
/// Several live pods can claim one attempt key — two orchestrators that both
/// submitted before either died, or a pod created under a name this build does not
/// derive. Exactly one is adopted and the rest are revoked, and *which* one is a
/// total order over the object name rather than the order the API happened to list
/// them in: a listing is a set, and a decision that depended on its enumeration
/// would not be reproducible.
///
/// A foreign-build pod claiming an attempt that also has an adoptable pod is
/// **not** a refusal — our own pod is genuinely ours, the object names cannot
/// collide, and the foreign one is reported and left running. A refusal is what
/// happens when the attempt's *only* pod belongs to a different program, because
/// then the object name this build derives is occupied by work dagr did not
/// launch.
#[must_use]
pub fn plan(
    pods: &[PodSnapshot],
    run_id: &str,
    build: &BuildIdentity,
    must_run: &BTreeSet<String>,
) -> DiscoveryPlan {
    let mut out = DiscoveryPlan::default();
    let mut candidates: BTreeMap<AttemptKey, Vec<DiscoveredPod>> = BTreeMap::new();
    let mut deferred: BTreeMap<AttemptKey, Vec<RefusedPod>> = BTreeMap::new();

    for pod in pods {
        let identity = match identify(&pod.labels, &pod.annotations) {
            Ok(identity) => identity,
            Err(err) => {
                out.ignored.push(RefusedPod {
                    name: pod.name.clone(),
                    refusal: AdoptionRefusal::Unidentifiable(err),
                });
                continue;
            }
        };
        let key = identity.key.clone();
        match refusal_for(&identity, run_id, build) {
            None => {
                if must_run.contains(&key.node) {
                    candidates.entry(key).or_default().push(DiscoveredPod {
                        name: pod.name.clone(),
                        uid: pod.uid.clone(),
                        host: pod.host.clone(),
                        phase: pod.phase,
                    });
                } else {
                    out.unclaimed.insert(key, pod.name.clone());
                }
            }
            Some(AdoptionRefusal::Tombstoned) => {
                out.tombstoned.insert(key, pod.name.clone());
            }
            Some(refusal) if refusal.is_foreign_build() && must_run.contains(&key.node) => {
                // Held back: whether this fails the node depends on whether the
                // attempt also has a pod of ours, which the listing may not have
                // reached yet.
                deferred.entry(key).or_default().push(RefusedPod {
                    name: pod.name.clone(),
                    refusal,
                });
            }
            Some(refusal) => out.ignored.push(RefusedPod {
                name: pod.name.clone(),
                refusal,
            }),
        }
    }

    for (key, mut found) in candidates {
        found.sort_by(|a, b| a.name.cmp(&b.name));
        let mut found = found.into_iter();
        // A `None` here cannot happen — a key is only in the map because a pod put
        // it there — but it is expressed as a skip rather than an `expect`, so this
        // function has no panic to document and no panic to hit.
        let Some(adopted) = found.next() else { continue };
        out.revoke.extend(found.map(|pod| pod.name));
        out.adopt.insert(key, adopted);
    }
    out.revoke.sort_unstable();

    for (key, mut refused) in deferred {
        refused.sort_by(|a, b| a.name.cmp(&b.name));
        if out.adopt.contains_key(&key) {
            // The attempt has a pod of ours; the foreign one is somebody else's
            // object under a name we do not use. Report it, leave it running.
            out.ignored.extend(refused);
        } else {
            let mut refused = refused.into_iter();
            let Some(first) = refused.next() else { continue };
            out.refuse.insert(key, first);
            out.ignored.extend(refused);
        }
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_selector_and_the_tombstone_agree_on_one_key() {
        // The property, rather than the spelling: whatever the completion key is,
        // the selector filters on it and the patch writes it.
        let selector = adoption_selector("run-1");
        let patch = tombstone_patch();
        let key = patch.keys().next().expect("one key");
        assert!(selector.contains(key.as_str()));
        assert!(selector.contains(TOMBSTONE_VALUE));
    }

    #[test]
    fn each_patch_writes_exactly_one_key() {
        assert_eq!(adoption_patch("o").len(), 1);
        assert_eq!(revocation_patch().len(), 1);
        assert_eq!(tombstone_patch().len(), 1);
    }

    #[test]
    fn an_owner_is_sanitized_into_a_legal_label_value() {
        let patch = adoption_patch("orchestrator/01 (pod)");
        let value = patch
            .get(LABEL_OWNER)
            .and_then(Option::as_ref)
            .expect("the owner is set");
        assert!(crate::identity::is_valid_label_value(value), "{value}");
    }

    #[test]
    fn the_three_build_surfaces_are_read_from_the_authoritative_half() {
        // A build comparison that read the *labels* would be comparing truncated
        // values, and two builds whose fingerprints share a 63-character prefix
        // would adopt each other's pods. Annotations are unbounded, which is why
        // they are the ones `ObservedIdentity` carries and this module compares.
        use crate::identity::{
            ANNOTATION_IMAGE_DIGEST, ANNOTATION_STRUCTURAL_FINGERPRINT, ANNOTATION_TOOL_VERSION,
            AttemptIdentity, AttemptKey,
        };
        let identity = AttemptIdentity {
            key: AttemptKey::new("run", "node", 1),
            pipeline: "p".to_string(),
            structural_fingerprint: "sf".to_string(),
            policy_hash: "ph".to_string(),
            tool_version: "tv".to_string(),
            image_digest: "id".to_string(),
            owner: "o".to_string(),
        };
        let annotations = identity.annotations();
        for key in [
            ANNOTATION_STRUCTURAL_FINGERPRINT,
            ANNOTATION_TOOL_VERSION,
            ANNOTATION_IMAGE_DIGEST,
        ] {
            assert!(annotations.contains_key(key), "{key} is annotated");
            assert!(!identity.labels().contains_key(key), "{key} is not a label");
        }
    }
}
