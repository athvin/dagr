//! T112 · **a missing RBAC verb fails informatively.** Written first (TDD).
//!
//! The ticket's requirement is precise: *"the demo works under exactly those
//! permissions and **fails informatively** without them"* — *"the failure names the
//! missing permission rather than hanging or reporting a generic error"*.
//!
//! Before this ticket a `403` was carried verbatim as an `ApiFailure`, which is the
//! platform's own sentence and therefore *technically* informative — but it is
//! indistinguishable, at the call site, from a quota rejection or an admission
//! webhook, and the observer classified it as a *transient* API failure to be
//! retried with backoff. A permission that is missing does not become present, so
//! retrying it is exactly the hang the ticket names.
//!
//! `dagr_k8s::rbac` is the pure classifier that closes both gaps: it recognizes a
//! forbidden response, names the verb (from the platform's own message when it says,
//! from the attempted call when it does not), and points at the manifest that grants
//! it.

use dagr_k8s::api::ApiFailure;
use dagr_k8s::rbac::{FORBIDDEN_CODE, ORCHESTRATOR_RBAC_MANIFEST, PodVerb, classify};

/// The message a real API server sends. Recorded verbatim from the shape
/// `kube` surfaces, so the parse is tested against the platform's own words.
fn forbidden(verb: &str) -> ApiFailure {
    ApiFailure::api(
        FORBIDDEN_CODE,
        "Forbidden",
        format!(
            "pods is forbidden: User \"system:serviceaccount:dagr:dagr-orchestrator\" cannot \
             {verb} resource \"pods\" in API group \"\" in the namespace \"dagr\""
        ),
    )
}

/// **Test plan: given the Role with `watch` removed, the failure names the missing
/// permission rather than hanging or reporting a generic error. Likewise for
/// `create` and `patch`.** All six, because a grant with a hole anywhere is the
/// same class of failure.
#[test]
fn every_missing_verb_is_named_in_the_diagnostic() {
    for verb in PodVerb::ALL {
        let missing = classify(verb, "dagr", &forbidden(verb.as_str()))
            .unwrap_or_else(|| panic!("a 403 on `{verb}` is a missing permission"));

        assert_eq!(missing.verb, verb);
        assert_eq!(missing.namespace, "dagr");

        let rendered = missing.to_string();
        assert!(
            rendered.contains(verb.as_str()),
            "the diagnostic names the missing verb: {rendered}"
        );
        assert!(rendered.contains("pods"), "…and the resource: {rendered}");
        assert!(rendered.contains("dagr"), "…and the namespace: {rendered}");
        assert!(
            rendered.contains(ORCHESTRATOR_RBAC_MANIFEST),
            "…and the manifest that grants it: {rendered}"
        );
    }
}

/// The verb is read from the **platform's** message when it names one, so a call
/// that fails for a verb other than the one attempted (a server that rewrote the
/// request, a proxy) still reports the truth rather than the guess.
#[test]
fn the_verb_comes_from_the_servers_own_message_when_it_names_one() {
    let missing =
        classify(PodVerb::Get, "dagr", &forbidden("watch")).expect("a 403 is a missing permission");
    assert_eq!(
        missing.verb,
        PodVerb::Watch,
        "the server said `watch`; the attempted call is only the fallback"
    );
}

/// …and falls back to the attempted verb when the message does not name one.
#[test]
fn the_attempted_verb_is_the_fallback_when_the_message_names_none() {
    let terse = ApiFailure::api(FORBIDDEN_CODE, "Forbidden", "forbidden");
    let missing = classify(PodVerb::Create, "dagr", &terse).expect("still a missing permission");
    assert_eq!(missing.verb, PodVerb::Create);
}

/// A failure that is **not** a permission problem is not misreported as one. This is
/// the non-vacuity control: a classifier that answered `Some` for everything would
/// pass every assertion above.
#[test]
fn a_non_forbidden_failure_is_not_classified_as_a_missing_permission() {
    for failure in [
        ApiFailure::api(429, "TooManyRequests", "slow down"),
        ApiFailure::api(409, "AlreadyExists", "pods \"dagr-x-0\" already exists"),
        ApiFailure::api(410, "Expired", "too old resource version"),
        ApiFailure::api(500, "InternalError", "the server is unhappy"),
        ApiFailure::transport("connection reset"),
    ] {
        assert!(
            classify(PodVerb::Create, "dagr", &failure).is_none(),
            "{failure} is not a missing permission and must not be reported as one"
        );
    }
}

/// A server that reports the reason without the numeric code is still recognized —
/// the two signals are independent, and only one of them has to be present.
#[test]
fn the_reason_alone_is_enough() {
    let no_code = ApiFailure {
        code: None,
        reason: "Forbidden".to_string(),
        message: "cannot delete resource \"pods\"".to_string(),
    };
    let missing = classify(PodVerb::Delete, "dagr", &no_code).expect("Forbidden by reason");
    assert_eq!(missing.verb, PodVerb::Delete);
}

/// The verb set is closed and its spelling is the platform's, because the
/// diagnostic tells an operator what to put in a `verbs:` list.
#[test]
fn the_verb_set_is_exactly_the_six_the_manifest_grants() {
    let spelled: Vec<&str> = PodVerb::ALL.iter().map(|v| v.as_str()).collect();
    assert_eq!(
        spelled,
        vec!["create", "delete", "get", "list", "patch", "watch"],
        "the six verbs, sorted, spelled as Kubernetes spells them"
    );
}

/// `ApiFailure` answers the question directly too, so a call site that only needs
/// the yes/no does not have to construct a `MissingPermission`.
#[test]
fn api_failure_reports_whether_it_is_forbidden() {
    assert!(forbidden("create").is_forbidden());
    assert!(!ApiFailure::api(410, "Expired", "gone").is_forbidden());
    assert!(!ApiFailure::transport("reset").is_forbidden());
}
