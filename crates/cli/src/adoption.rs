//! **Orphan adoption, tombstones and ownership revocation** — the acting half
//! (M10, T109, ADR 115 §5).
//!
//! `dagr_k8s::adoption` decides; this module *does*. It lists the run's pods,
//! patches the owner label on the ones this build may reclaim, revokes the
//! duplicates, and hands the survivors to [`K8sNodeRunner`](crate::k8s_runner)
//! through an [`Adoptions`] registry. The split is ADR 004's, and the same one the
//! observer already has: the decisions are values and live in the crate with no
//! runtime; the I/O lives where the orchestrator process is.
//!
//! # The one thing this exists to prevent
//!
//! An orchestrator that dies leaves pods running. They are separate processes on
//! separate machines and they neither know nor care that their submitter is gone.
//! Resubmitting them duplicates work that is already in flight — and, for a task
//! with side effects, duplicates the side effects. Ignoring them leaks them.
//! Reclaiming them is the only answer that is both correct and cheap, and it costs
//! exactly one label patch per pod.
//!
//! # Adoption is scoped to one run id — and what follows from that
//!
//! The attempt key is scoped to a run, and dagr's resume mints a **new** run id,
//! so a resumed run adopting the killed run's pods would blur two runs' event
//! streams. Adoption therefore requires the *same* run id, which makes the
//! kill-restart guarantee a guarantee about **restarting the same run**
//! (`RunId::from_operator` already supports an operator-supplied id).
//!
//! The consequence is stated rather than hidden: a *resumed* run does not adopt
//! the killed run's pods. It **revokes** them — `prior_run_id`, one pass, at
//! startup — and resubmits, because leaving them running would double both the
//! work and the cluster capacity the resumed run is about to spend. That pass is
//! not a reaper: it is one act of the run that is starting, over the pods of the
//! run it descends from, and nothing here outlives the process.
//!
//! # What discovery does *not* do
//!
//! It never deletes a pod it did not recognize as this build's. A pod annotated
//! with a different structural fingerprint, tool version, or image digest belongs
//! to a different program: it is reported, left running, and the node it collides
//! with fails with a classified error. Deleting somebody else's work is not dagr's
//! call, and guessing which program it belongs to would be worse than saying so.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, PoisonError};

use dagr_k8s::adoption::{
    AdoptionRefusal, BuildIdentity, DiscoveredPod, RefusedPod, adoption_patch, adoption_selector,
    plan, revocation_patch, tombstone_patch,
};
use dagr_k8s::api::{ApiFailure, PodApi, PodLifecycle};
use dagr_k8s::identity::AttemptKey;

/// What one discovery pass was told about this invocation.
#[derive(Debug, Clone)]
pub struct AdoptionConfig {
    /// The run whose pods may be reclaimed. Adoption never crosses this boundary.
    pub run_id: String,
    /// This orchestrator's owner key — the value the adoption patch writes.
    pub owner: String,
    /// The build a discovered pod's annotations must describe.
    pub build: BuildIdentity,
    /// The node names that have a runner in this invocation. A pod for a node
    /// outside it is left alone: a `satisfied-from-prior` node has no runner, so
    /// there is no attempt to reclaim and nothing to wait on.
    pub must_run: BTreeSet<String>,
    /// The run this one resumes, when it resumes one. Its still-live pods are
    /// **revoked**, never adopted — see the module docs.
    pub prior_run_id: Option<String>,
}

/// What discovery reclaimed for one attempt — or why it could not.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Claim {
    /// A live pod this run may wait on instead of submitting a new one.
    Adopted(DiscoveredPod),
    /// A pod on this attempt's object name that belongs to a different program.
    /// The node fails with a classified error naming both values; the pod is left
    /// running.
    Refused(RefusedPod),
}

/// The registry a [`K8sNodeRunner`](crate::k8s_runner::K8sNodeRunner) consults
/// before it submits.
///
/// Claims are **taken**, not read: an attempt reclaims a pod once, and a later
/// retry of the same node is a genuinely new attempt that must submit its own.
/// Shared behind a mutex because one registry serves every node's runner and the
/// driver spawns them concurrently.
#[derive(Debug, Clone, Default)]
pub struct Adoptions {
    claims: Arc<Mutex<BTreeMap<AttemptKey, Claim>>>,
}

impl Adoptions {
    /// A registry that reclaimed nothing — the ordinary, non-restart case, and the
    /// default a runner is built with.
    #[must_use]
    pub fn none() -> Self {
        Self::default()
    }

    /// Build a registry over `claims`.
    #[must_use]
    pub fn new(claims: BTreeMap<AttemptKey, Claim>) -> Self {
        Self {
            claims: Arc::new(Mutex::new(claims)),
        }
    }

    /// Take the claim for `key`, if there is one.
    ///
    /// Poison policy: **recover**. The map behind this lock is a plain
    /// `BTreeMap` of decisions a startup pass already finished making — a panic
    /// on another node's runner cannot leave it half-updated, so there is no
    /// invariant a poisoned flag is warning about. Escalating here would turn one
    /// node's panic into "no other node can find the pod it was told to adopt",
    /// which is the duplicate-execution outcome this module exists to prevent.
    #[must_use]
    pub fn take(&self, key: &AttemptKey) -> Option<Claim> {
        self.claims
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(key)
    }

    /// How many claims are outstanding.
    ///
    /// Poison policy: **recover**, for the reason [`take`](Self::take) states —
    /// and a fortiori here, where the lock guards a read for a report.
    #[must_use]
    pub fn len(&self) -> usize {
        self.claims
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }

    /// Whether nothing was reclaimed.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// What one discovery pass did, in pod names — the operator-facing account.
///
/// Every field is a sorted `Vec<String>` rather than a set of structs because this
/// is a report: it is printed, logged, and asserted against, and a stable order is
/// what makes all three reproducible.
#[derive(Debug, Clone, Default)]
pub struct DiscoveryReport {
    /// The registry the runners consult.
    pub adoptions: Adoptions,
    /// Pods reclaimed by an owner patch.
    pub adopted: Vec<String>,
    /// Duplicate pods revoked (owner cleared, then deleted).
    pub revoked: Vec<String>,
    /// Pods belonging to a different program: reported, left running, and fatal to
    /// the node whose attempt they occupy.
    pub refused: Vec<String>,
    /// Pods whose outcome has already been consumed.
    pub tombstoned: Vec<String>,
    /// Pods for nodes that will not run in this invocation.
    pub unclaimed: Vec<String>,
    /// Pods of the run this one resumes, revoked at startup.
    pub prior_revoked: Vec<String>,
    /// Pods this build would have adopted but could not, because the ownership
    /// patch itself failed. **Not** adopted — the node falls back to T108's
    /// in-process idempotency probe, which finds the same pod under the attempt's
    /// own object name — and named here so the degradation is visible.
    pub unpatched: Vec<String>,
    /// Every refusal's message, keyed by the attempt it occupies.
    refusals: BTreeMap<AttemptKey, String>,
    /// Why each [`unpatched`](Self::unpatched) pod could not be claimed, keyed by
    /// pod name — the platform's own words, kept so a caller can classify them (a
    /// missing `patch` grant reads very differently from a transient `500`, and only
    /// one of them is worth telling an operator to fix).
    unpatched_reasons: BTreeMap<String, String>,
}

impl DiscoveryReport {
    /// The refusal message for `key`, naming the pod and both disagreeing values.
    #[must_use]
    pub fn report_for(&self, key: &AttemptKey) -> Option<String> {
        self.refusals.get(key).cloned()
    }

    /// Why `pod` appears in [`unpatched`](Self::unpatched), in the platform's own
    /// words.
    #[must_use]
    pub fn unpatched_reason(&self, pod: &str) -> Option<&str> {
        self.unpatched_reasons.get(pod).map(String::as_str)
    }

    /// Every refusal message, keyed by **pod name** — the operator-facing view of
    /// [`refused`](Self::refused).
    #[must_use]
    pub fn refusal_messages(&self) -> BTreeMap<String, String> {
        self.refusals
            .values()
            .filter_map(|message| {
                // The message is `refusing to adopt pod \`<name>\`: …`; the pod name
                // is the one field a caller has to key on, and it is already in the
                // sentence rather than duplicated beside it.
                let start = message.find('`')? + 1;
                let end = message[start..].find('`')? + start;
                Some((message[start..end].to_string(), message.clone()))
            })
            .collect()
    }

    /// Whether the pass changed nothing — no pod was reclaimed, revoked or
    /// refused.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.adopted.is_empty()
            && self.revoked.is_empty()
            && self.refused.is_empty()
            && self.prior_revoked.is_empty()
    }
}

/// Discover this run's pods, reclaim what this build may, and revoke the rest.
///
/// One pass, at startup, before any node is submitted. It reads through the
/// **read** port ([`PodApi::list`], the observer's own grant) and writes through
/// the **write** port ([`PodLifecycle`]), which is the same least-privilege split
/// the two ports exist for.
///
/// # Errors
///
/// Returns [`ApiFailure`] when the listing itself failed — a run that cannot see
/// the cluster cannot decide anything, and proceeding would submit duplicates of
/// every pod it could not see. A *per-pod* write failure is not an error: it is
/// recorded in the report and the run continues, because one unpatchable label is
/// not a reason to abandon a run.
pub async fn discover<A: PodApi, L: PodLifecycle>(
    api: &A,
    lifecycle: &L,
    config: &AdoptionConfig,
) -> Result<DiscoveryReport, ApiFailure> {
    let listing = api.list(&adoption_selector(&config.run_id)).await?;
    let decided = plan(
        &listing.pods,
        &config.run_id,
        &config.build,
        &config.must_run,
    );

    let mut report = DiscoveryReport::default();
    let mut claims: BTreeMap<AttemptKey, Claim> = BTreeMap::new();

    for (key, pod) in decided.adopt {
        // The whole of adoption: one labels-only patch, in place. If it fails the
        // pod stays exactly as it was and is not claimed — never deleted, because
        // a label write that did not land says nothing about the work in flight.
        match lifecycle
            .patch_labels(&pod.name, &adoption_patch(&config.owner))
            .await
        {
            Ok(()) => {
                report.adopted.push(pod.name.clone());
                claims.insert(key, Claim::Adopted(pod));
            }
            Err(failure) => {
                tracing::warn!(
                    pod = %pod.name,
                    error = %failure,
                    "could not take ownership of a discovered pod; falling back to \
                     the submission probe"
                );
                report
                    .unpatched_reasons
                    .insert(pod.name.clone(), failure.to_string());
                report.unpatched.push(pod.name);
            }
        }
    }

    for name in decided.revoke {
        revoke(lifecycle, &name).await;
        report.revoked.push(name);
    }

    for (key, refused) in decided.refuse {
        report.refused.push(refused.name.clone());
        report
            .refusals
            .insert(key.clone(), describe(&refused.name, &refused.refusal));
        claims.insert(key, Claim::Refused(refused));
    }

    report.tombstoned = decided.tombstoned.into_values().collect();
    report.unclaimed = decided.unclaimed.into_values().collect();
    for ignored in &decided.ignored {
        tracing::info!(
            pod = %ignored.name,
            reason = %ignored.refusal,
            "a pod matching this run's selector is not this run's to reclaim"
        );
    }

    if let Some(prior) = &config.prior_run_id {
        report.prior_revoked = revoke_run(api, lifecycle, prior).await?;
    }

    report.adopted.sort_unstable();
    report.revoked.sort_unstable();
    report.refused.sort_unstable();
    report.tombstoned.sort_unstable();
    report.unclaimed.sort_unstable();
    report.unpatched.sort_unstable();
    report.adoptions = Adoptions::new(claims);
    Ok(report)
}

/// Revoke every still-live pod of `run_id` — the pods a resumed run's parent left
/// behind.
///
/// # Errors
///
/// Returns [`ApiFailure`] when the listing failed.
async fn revoke_run<A: PodApi, L: PodLifecycle>(
    api: &A,
    lifecycle: &L,
    run_id: &str,
) -> Result<Vec<String>, ApiFailure> {
    let listing = api.list(&adoption_selector(run_id)).await?;
    // The selector is a request to a server; the filter is what makes the pass
    // true. A server that widened it must not be able to make this delete pods of
    // a run nobody named.
    let expected = dagr_k8s::identity::label_value(run_id);
    let mut revoked = Vec::new();
    for pod in &listing.pods {
        let claims_run = pod
            .labels
            .get(dagr_k8s::identity::LABEL_RUN_ID)
            .is_some_and(|value| *value == expected);
        if !claims_run || dagr_k8s::adoption::is_tombstoned(&pod.labels) {
            continue;
        }
        revoke(lifecycle, &pod.name).await;
        revoked.push(pod.name.clone());
    }
    revoked.sort_unstable();
    Ok(revoked)
}

/// **Revoke** a pod: clear the owner label, *then* delete it.
///
/// The ordering is the mechanism, not a detail. A delete issued first is
/// indistinguishable from an external deletion to anything watching; clearing the
/// owner first makes the disappearance legible as ours
/// ([`dagr_k8s::adoption::deletion_origin`]), and — because an unowned pod no
/// longer identifies as an attempt's — stops a revoked duplicate's `Deleted` event
/// from retiring the waiter of the pod that *was* adopted for the same attempt.
///
/// Both calls are best effort. A revocation that fails leaves a pod behind, which
/// is worth a line in the log and is never a reason to fail a node twice.
pub async fn revoke<L: PodLifecycle>(lifecycle: &L, pod: &str) {
    if let Err(failure) = lifecycle.patch_labels(pod, &revocation_patch()).await {
        tracing::warn!(
            pod = %pod,
            error = %failure,
            "could not clear ownership before deleting; the delete will read as an \
             external one"
        );
    }
    if let Err(failure) = lifecycle.delete(pod).await {
        tracing::warn!(pod = %pod, error = %failure, "could not delete a revoked pod");
    }
}

/// **Tombstone** a pod whose outcome has been consumed, with the key
/// [`adoption_selector`] excludes.
///
/// This is what makes "a finished attempt is never adopted twice" hold even when
/// the pod's deletion is deferred or fails — the two protections are independent
/// on purpose, because the delete is the one that can be lost.
///
/// Best effort, and deliberately so: the outcome is already in the run's event
/// stream by the time this is called, so failing the node because a label write
/// failed would discard a result dagr already has.
pub async fn tombstone<L: PodLifecycle>(lifecycle: &L, pod: &str) {
    if let Err(failure) = lifecycle.patch_labels(pod, &tombstone_patch()).await {
        tracing::warn!(
            pod = %pod,
            error = %failure,
            "could not tombstone a consumed pod; a later discovery may re-read it"
        );
    }
}

/// The operator-facing sentence for one refusal.
fn describe(pod: &str, refusal: &AdoptionRefusal) -> String {
    format!("refusing to adopt pod `{pod}`: {refusal}")
}
