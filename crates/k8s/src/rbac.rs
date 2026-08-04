//! **RBAC** — the verbs the orchestrator needs, and what a missing one looks like.
//!
//! A least-privilege grant is only least-privilege if somebody can tell, at a
//! glance, what it is. So the verb set lives here as a closed enum rather than in a
//! YAML file alone, the shipped manifest is asserted against it, and the diagnostic
//! an operator sees when one is missing is generated from the same list.
//!
//! # Why a `403` needs classifying at all
//!
//! Every other failure the API returns is a reason to try again. A quota rejection
//! clears, an admission webhook is redeployed, a `5xx` is a control plane coming
//! back — so the shipped discipline is backoff and retry, which is right for all of
//! them and *wrong* for exactly one. **A permission that is missing does not become
//! present.** Retrying it burns the observer's failure budget and then reports a
//! generic "transient API failure", which tells an operator nothing about the one
//! thing they can fix.
//!
//! This module is the classifier that closes that gap. It is a **pure decision** over
//! an [`ApiFailure`] — no I/O, no client, no runtime — so it is testable on both
//! platforms with no cluster, like everything else in this crate.
//!
//! # The verb, from the platform's own words
//!
//! Kubernetes says which verb it refused:
//!
//! ```text
//! pods is forbidden: User "system:serviceaccount:dagr:dagr-orchestrator"
//!   cannot watch resource "pods" in API group "" in the namespace "dagr"
//! ```
//!
//! So the classification reads the verb out of the message and falls back to the
//! call that was attempted only when the server did not say. Reporting the attempted
//! call unconditionally would be a guess, and a guess about which line to add to a
//! `Role` is worse than no guidance at all.

use std::fmt;

use crate::api::ApiFailure;

/// The repository path of the manifest that grants the whole set.
///
/// Named in every diagnostic, so applying the fix is a copy rather than a
/// reconstruction. It is a repository-relative path rather than a URL because the
/// file is reviewed like code and travels with the build that needs it.
pub const ORCHESTRATOR_RBAC_MANIFEST: &str = "crates/k8s/manifests/dagr-orchestrator-rbac.yaml";

/// The HTTP status a Kubernetes API server returns for a permission it will not
/// grant.
pub const FORBIDDEN_CODE: u16 = 403;

/// The Kubernetes reason string that accompanies it.
const FORBIDDEN_REASON: &str = "Forbidden";

/// A verb the orchestrator exercises on pods.
///
/// A **closed** set, and exactly the set the shipped `Role` grants: three reads the
/// observer needs (`get`, `list`, `watch`) and three writes the executor and the
/// adoption pass need (`create`, `delete`, `patch`). There is no seventh, and there
/// is no second resource.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PodVerb {
    /// Submit one attempt's pod.
    Create,
    /// Remove a pod dagr owns — cancellation, a node timeout, a revocation.
    Delete,
    /// Read one pod: the submission idempotency probe.
    Get,
    /// Read the run's pods: the startup discovery pass and every resync.
    List,
    /// Rewrite `metadata.labels` in place: adoption, revocation, tombstoning.
    Patch,
    /// The one long-lived stream the orchestrator holds.
    Watch,
}

impl PodVerb {
    /// Every verb, sorted, in the order the manifest lists them — so the grant is
    /// written down exactly once and a reviewer can diff it.
    pub const ALL: [PodVerb; 6] = [
        PodVerb::Create,
        PodVerb::Delete,
        PodVerb::Get,
        PodVerb::List,
        PodVerb::Patch,
        PodVerb::Watch,
    ];

    /// The verb as Kubernetes spells it — which is what an operator types into a
    /// `verbs:` list.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            PodVerb::Create => "create",
            PodVerb::Delete => "delete",
            PodVerb::Get => "get",
            PodVerb::List => "list",
            PodVerb::Patch => "patch",
            PodVerb::Watch => "watch",
        }
    }
}

impl fmt::Display for PodVerb {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A grant the orchestrator needs and does not have.
///
/// Distinct from a bare [`ApiFailure`] on purpose: it is the one failure class whose
/// remedy is a file an operator applies, and the type is what lets a call site say
/// so instead of forwarding the platform's sentence and hoping.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissingPermission {
    /// The verb that was refused.
    pub verb: PodVerb,
    /// The namespace it was refused in.
    pub namespace: String,
    /// The platform's own message, verbatim. Kept because a classified failure that
    /// paraphrases the platform is a diagnostic nobody can act on.
    pub detail: String,
}

impl fmt::Display for MissingPermission {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let granted: Vec<&str> = PodVerb::ALL.iter().map(|v| v.as_str()).collect();
        write!(
            f,
            "the identity this orchestrator runs as may not `{}` pods in namespace `{}` \
             — apply `{}`, whose Role grants exactly [{}] on pods in that one namespace \
             (the platform said: {})",
            self.verb,
            self.namespace,
            ORCHESTRATOR_RBAC_MANIFEST,
            granted.join(", "),
            self.detail,
        )
    }
}

impl std::error::Error for MissingPermission {
    /// Classified from **data** — a response the server sent — rather than wrapped
    /// from an underlying error, so there is no cause to expose and fabricating one
    /// would misreport where the refusal came from.
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }
}

/// Classify `failure` as a missing permission, or [`None`] when it is something
/// else.
///
/// `attempted` is the call the caller made; it is the **fallback** for the verb, used
/// only when the server's message does not name one. `namespace` is where the call
/// was made, so the diagnostic can say which `Role` to edit.
#[must_use]
pub fn classify(
    attempted: PodVerb,
    namespace: &str,
    failure: &ApiFailure,
) -> Option<MissingPermission> {
    if !failure.is_forbidden() {
        return None;
    }
    Some(MissingPermission {
        verb: verb_in(&failure.message).unwrap_or(attempted),
        namespace: namespace.to_string(),
        detail: failure.message.clone(),
    })
}

/// The verb a Kubernetes authorization message names, if it names one.
///
/// The message shape is `… cannot <verb> resource "pods" …`. Matching on the
/// `cannot ` prefix rather than on a bare verb word is what stops a namespace called
/// `list` or a user called `patch` from being read as the refusal.
#[must_use]
fn verb_in(message: &str) -> Option<PodVerb> {
    let rest = message.split("cannot ").nth(1)?;
    let word = rest.split_whitespace().next()?;
    PodVerb::ALL.into_iter().find(|v| v.as_str() == word)
}

/// Classify a **rendered** failure — a string that has already passed through one or
/// more `Display` impls — as a missing permission.
///
/// It exists because not every path keeps the `ApiFailure`. A pod that could not be
/// created is charged to the infrastructure budget and reported as a
/// `LaunchExhausted` carrying the platform's words as text, and re-plumbing the
/// typed failure through that budget would change T108's mechanism to serve a
/// diagnostic. So the classification is done over the text, and it is deliberately
/// **strict**: both a forbidden marker *and* a named verb must be present, because a
/// text scan that guessed would misreport a quota rejection as a permission problem
/// and send an operator to edit the wrong file.
#[must_use]
pub fn classify_text(namespace: &str, text: &str) -> Option<MissingPermission> {
    let marked = text.contains(FORBIDDEN_REASON)
        || text.contains("forbidden")
        || text.contains(&format!("({FORBIDDEN_CODE})"));
    if !marked {
        return None;
    }
    let verb = verb_in(text)?;
    Some(MissingPermission {
        verb,
        namespace: namespace.to_string(),
        detail: text.to_string(),
    })
}

/// Whether an observer termination is a missing permission rather than a cluster
/// that went away.
///
/// The observer classifies a `403` as a *transient* API failure and retries it with
/// backoff, which is correct for a server that is coming back. When its bounded
/// budget is spent, this turns the accumulated failure into the actionable sentence:
/// a permission that is missing does not become present, and the operator needs to
/// know which one.
#[must_use]
pub fn classify_termination(
    namespace: &str,
    cause: &crate::observer::TerminationCause,
    last_message: &str,
) -> Option<MissingPermission> {
    let forbidden = matches!(
        cause,
        crate::observer::TerminationCause::TransientApi { code } if *code == FORBIDDEN_CODE
    );
    if !forbidden {
        return None;
    }
    Some(MissingPermission {
        // A terminated observer was either listing or watching; the message says
        // which, and `watch` is the fallback because it is the call it spends its
        // life in.
        verb: verb_in(last_message).unwrap_or(PodVerb::Watch),
        namespace: namespace.to_string(),
        detail: last_message.to_string(),
    })
}

impl ApiFailure {
    /// Whether the server refused this call for lack of permission.
    ///
    /// Either signal is sufficient: a client that surfaced the status without the
    /// reason, and one that surfaced the reason without the status, are both real.
    #[must_use]
    pub fn is_forbidden(&self) -> bool {
        self.code == Some(FORBIDDEN_CODE) || self.reason == FORBIDDEN_REASON
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_verb_parse_ignores_a_verb_shaped_word_outside_the_cannot_clause() {
        let msg = "pods is forbidden: User \"list\" cannot patch resource \"pods\"";
        assert_eq!(verb_in(msg), Some(PodVerb::Patch));
    }

    #[test]
    fn a_message_with_no_cannot_clause_names_no_verb() {
        assert_eq!(verb_in("forbidden"), None);
    }

    #[test]
    fn the_text_classifier_needs_both_a_marker_and_a_verb() {
        // A rendered `LaunchExhausted`, which is how a refused create actually
        // reaches an operator.
        let rendered = "`extract` could not be launched after 3 submission(s): Forbidden \
                        (403): pods is forbidden: User \"x\" cannot create resource \"pods\" \
                        in API group \"\" in the namespace \"dagr\". This is an \
                        infrastructure failure.";
        let missing = classify_text("dagr", rendered).expect("a forbidden create");
        assert_eq!(missing.verb, PodVerb::Create);

        // A quota rejection names no verb and carries no marker: not a permission.
        assert!(
            classify_text(
                "dagr",
                "`extract` could not be launched after 3 submission(s): Forbidden (403): \
                 exceeded quota"
            )
            .is_none(),
            "a marker without a named verb is not enough to send somebody to edit a Role"
        );
        assert!(
            classify_text(
                "dagr",
                "TooManyRequests (429): cannot create resource \"pods\""
            )
            .is_none(),
            "a named verb without a forbidden marker is not a permission problem"
        );
    }
}
