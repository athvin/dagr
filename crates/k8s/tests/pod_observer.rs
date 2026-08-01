//! The load-bearing suite: one watch, its reconnect discipline, and exactly-once
//! delivery to per-attempt waiters — driven against the fake API surface.
//!
//! A Kubernetes watch is not a reliable stream. It ends on `resourceVersion`
//! expiry (410 Gone), on an API-server rollout, on an idle timeout, and on a
//! network partition — and, worse, it can stop delivering *without erroring*,
//! which is indistinguishable from "nothing has changed". Since a remote attempt
//! reports nothing inbound, the watch is the only liveness signal, so a silently
//! stalled watch is a silently hung run.
//!
//! Every scenario the spike found is inducible here: an in-stream 410, a silent
//! stall, a duplicate delivery, and a watch that fails on every reconnect. A fake
//! that could not induce those would not be testing this ticket.

mod support;

use std::time::Duration;

use dagr_k8s::api::{ApiFailure, PodPhase, WatchDelivery};
use dagr_k8s::fake::fake_api;
use dagr_k8s::identity::{ANNOTATION_STRUCTURAL_FINGERPRINT, AttemptKey, LABEL_RUN_ID};
use dagr_k8s::observer::{
    ForeignReason, ObserverLimits, PodObserver, TerminationCause, WaiterEvent,
};
use support::{RUN_ID, bare_pod, identity, pod, pod_with_annotation, selector};

/// Test limits: the same shape as the shipped defaults, with the clock-driven
/// bounds shrunk so a paused-clock test asserts the schedule rather than the
/// wall time. The failure budget is deliberately small so the bounded-retry test
/// is readable.
fn limits() -> ObserverLimits {
    ObserverLimits {
        stall_bound: Duration::from_secs(90),
        backoff_initial: Duration::from_millis(250),
        backoff_max: Duration::from_secs(30),
        max_consecutive_failures: 4,
        failure_window: Duration::from_mins(5),
        watch_timeout_secs: 270,
    }
}

fn key(node: &str, attempt: u32) -> AttemptKey {
    AttemptKey::new(RUN_ID, node, attempt)
}

/// **Test-plan scenario: a watch terminated by `resourceVersion` expiry.**
/// Setup: a watch is running; the pod reaches a terminal phase *during the gap*
/// and the watch is expired with an in-stream 410. Action: the observer
/// reconnects. Expected: it re-**lists** (never resuming from the 410's own
/// resourceVersion, which is the cache's oldest retained bound and not a resume
/// point), and the terminal transition that happened during the gap is delivered
/// exactly once.
#[tokio::test(start_paused = true)]
async fn expiry_relists_and_delivers_a_gap_terminal_exactly_once() {
    let (api, control) = fake_api();
    control.upsert(pod("extract", 1, PodPhase::Pending, "100"));

    let observer = PodObserver::spawn(api, selector(), limits());
    let mut waiter = observer
        .watch_attempt(key("extract", 1))
        .await
        .expect("the observer accepts a waiter");

    control.await_watch().await;
    assert_eq!(control.lists(), 1, "the observer lists before it watches");
    let pending = waiter
        .next()
        .await
        .expect("the initial LIST reports Pending");
    assert!(matches!(
        pending,
        WaiterEvent::Observed(ref o) if o.phase == PodPhase::Pending
    ));

    // The transition happens while the watch is dead: the pod's state moves, and
    // the only event the client ever sees is the expiry itself.
    control.upsert(pod("extract", 1, PodPhase::Succeeded, "140"));
    control
        .deliver(WatchDelivery::ApiError {
            code: 410,
            reason: "Expired".to_string(),
            // Verbatim shape of a real body: the number in the parentheses is the
            // watch cache's OLDEST retained bound, not the head.
            message: "too old resource version: 100 (972)".to_string(),
        })
        .await;

    let event = waiter
        .next()
        .await
        .expect("the terminal transition arrives");
    let WaiterEvent::Observed(observation) = event else {
        panic!("expected an observation, got {event:?}");
    };
    assert_eq!(observation.phase, PodPhase::Succeeded);
    assert!(observation.terminal);
    assert_eq!(observation.key, key("extract", 1));

    // Exactly once: the waiter is retired by its terminal, so the channel closes
    // rather than repeating it.
    assert!(
        tokio::time::timeout(Duration::from_secs(5), waiter.next())
            .await
            .expect("a retired waiter closes rather than blocking")
            .is_none()
    );

    let report = observer
        .shutdown(Duration::from_secs(5))
        .await
        .expect("shutdown completes");
    assert_eq!(report.stats.relists, 2, "the expiry forced a second LIST");
    assert!(
        !control
            .watch_resource_versions()
            .iter()
            .any(|rv| rv == "972"),
        "the resourceVersion inside a 410 is never a resume point: {:?}",
        control.watch_resource_versions()
    );
}

/// **Test-plan scenario: a watch that stops delivering without erroring.**
/// Setup: the watch is open and silent; the pod goes terminal with nobody
/// watching. Action: nothing — the test only lets the clock run. Expected: the
/// observer detects the stall inside its bound and reconnects, which is what
/// finds the transition. This test fails if silence is trusted.
#[tokio::test(start_paused = true)]
async fn a_silent_watch_is_detected_within_the_bound_and_reconnected() {
    let (api, control) = fake_api();
    control.upsert(pod("extract", 1, PodPhase::Running, "100"));

    let observer = PodObserver::spawn(api, selector(), limits());
    let mut waiter = observer
        .watch_attempt(key("extract", 1))
        .await
        .expect("the observer accepts a waiter");

    control.await_watch().await;
    let first = waiter
        .next()
        .await
        .expect("the initial LIST reports Running");
    assert!(matches!(
        first,
        WaiterEvent::Observed(ref o) if o.phase == PodPhase::Running
    ));
    assert_eq!(control.lists(), 1);

    // The pod finishes and the watch says nothing at all — the exact failure mode
    // a blackholed control plane produces.
    control.upsert(pod("extract", 1, PodPhase::Failed, "140"));

    let started = tokio::time::Instant::now();
    let event = tokio::time::timeout(Duration::from_mins(10), waiter.next())
        .await
        .expect("a stalled watch must not hang the run")
        .expect("the reconnect finds the transition");
    let elapsed = started.elapsed();

    let WaiterEvent::Observed(observation) = event else {
        panic!("expected an observation, got {event:?}");
    };
    assert_eq!(observation.phase, PodPhase::Failed);
    assert!(observation.terminal);
    assert_eq!(observation.container_reason.as_deref(), Some("Error"));
    assert_eq!(observation.exit_code, Some(1));

    assert!(
        elapsed >= limits().stall_bound,
        "the observer must not churn before its bound: {elapsed:?}"
    );
    assert!(
        elapsed < limits().stall_bound * 2,
        "the stall must be detected inside a single bound, not a multiple: {elapsed:?}"
    );

    let report = observer
        .shutdown(Duration::from_secs(5))
        .await
        .expect("shutdown completes");
    assert_eq!(report.stats.stalls, 1);
    assert!(report.stats.relists >= 2);
}

/// **Test-plan scenario: a watch terminated repeatedly.** Setup: every attempt to
/// open a watch fails. Action: let the observer retry. Expected: reconnection
/// backs off (each delay strictly larger than the last, up to the cap), is
/// bounded, and the permanent failure surfaces as a *classified* error on the
/// waiter — never an infinite quiet retry.
#[tokio::test(start_paused = true)]
async fn repeated_termination_backs_off_and_fails_with_a_classified_error() {
    let (api, control) = fake_api();
    control.upsert(pod("extract", 1, PodPhase::Pending, "100"));
    for _ in 0..12 {
        control.fail_next_watch(ApiFailure::transport(
            "error reading a body from connection",
        ));
    }

    let observer = PodObserver::spawn(api, selector(), limits());
    let mut waiter = observer
        .watch_attempt(key("extract", 1))
        .await
        .expect("the observer accepts a waiter");

    // The Pending pod from the initial LIST arrives first.
    let first = waiter
        .next()
        .await
        .expect("the initial LIST reports Pending");
    assert!(matches!(
        first,
        WaiterEvent::Observed(ref o) if o.phase == PodPhase::Pending
    ));

    let event = tokio::time::timeout(Duration::from_hours(1), waiter.next())
        .await
        .expect("a permanently broken watch must fail, not hang")
        .expect("the waiter is told");
    let WaiterEvent::ObserverFailed(failure) = event else {
        panic!("expected a classified failure, got {event:?}");
    };
    assert!(matches!(failure.cause, TerminationCause::Transport));
    assert_eq!(
        failure.consecutive_failures,
        limits().max_consecutive_failures
    );
    assert!(
        failure.last_message.contains("reading a body"),
        "the classified error keeps the platform's own words: {}",
        failure.last_message
    );

    // Bounded: it stopped trying rather than retrying forever.
    let opens = control.watch_opens();
    assert_eq!(
        opens,
        limits().max_consecutive_failures,
        "the observer tried exactly its budget, then stopped"
    );

    // And it backed off between tries: the delays are strictly increasing.
    let gaps = control.watch_open_gaps();
    assert!(gaps.len() >= 2, "not enough retries to observe a backoff");
    for pair in gaps.windows(2) {
        assert!(pair[1] > pair[0], "reconnection did not back off: {gaps:?}");
    }

    let report = observer
        .shutdown(Duration::from_secs(5))
        .await
        .expect("shutdown completes");
    let recorded = report.failure.expect("the failure is recorded");
    assert!(matches!(recorded.cause, TerminationCause::Transport));
}

/// **Test-plan scenario: a terminal transition delivered twice by the API.**
/// Expected: the waiter is notified once — delivery is idempotent on the attempt
/// key, not on the event.
#[tokio::test(start_paused = true)]
async fn a_terminal_transition_delivered_twice_notifies_the_waiter_once() {
    let (api, control) = fake_api();
    let observer = PodObserver::spawn(api, selector(), limits());
    let mut waiter = observer
        .watch_attempt(key("extract", 1))
        .await
        .expect("the observer accepts a waiter");
    control.await_watch().await;

    let terminal = pod("extract", 1, PodPhase::Succeeded, "140");
    control
        .deliver(WatchDelivery::Modified(terminal.clone()))
        .await;
    control
        .deliver(WatchDelivery::Modified(terminal.clone()))
        .await;
    control.deliver(WatchDelivery::Added(terminal)).await;

    let event = waiter.next().await.expect("the terminal arrives");
    assert!(matches!(
        event,
        WaiterEvent::Observed(ref o) if o.terminal && o.phase == PodPhase::Succeeded
    ));
    assert!(
        tokio::time::timeout(Duration::from_secs(5), waiter.next())
            .await
            .expect("a retired waiter closes rather than blocking")
            .is_none(),
        "a repeated terminal must not reach the waiter a second time"
    );

    observer
        .shutdown(Duration::from_secs(5))
        .await
        .expect("shutdown completes");
}

/// **Test-plan scenario: two pods for different attempts of the same node.**
/// Expected: each waiter receives only its own.
#[tokio::test(start_paused = true)]
async fn two_attempts_of_one_node_each_receive_only_their_own() {
    let (api, control) = fake_api();
    let observer = PodObserver::spawn(api, selector(), limits());
    let mut first = observer
        .watch_attempt(key("extract", 1))
        .await
        .expect("waiter one");
    let mut second = observer
        .watch_attempt(key("extract", 2))
        .await
        .expect("waiter two");
    control.await_watch().await;

    control
        .deliver(WatchDelivery::Modified(pod(
            "extract",
            2,
            PodPhase::Succeeded,
            "141",
        )))
        .await;
    control
        .deliver(WatchDelivery::Modified(pod(
            "extract",
            1,
            PodPhase::Failed,
            "142",
        )))
        .await;

    let one = first.terminal().await.expect("attempt 1 resolves");
    assert_eq!(one.key.attempt, 1);
    assert_eq!(one.phase, PodPhase::Failed);

    let two = second.terminal().await.expect("attempt 2 resolves");
    assert_eq!(two.key.attempt, 2);
    assert_eq!(two.phase, PodPhase::Succeeded);

    observer
        .shutdown(Duration::from_secs(5))
        .await
        .expect("shutdown completes");
}

/// **Test-plan scenario: a pod whose annotations name a different structural
/// fingerprint.** Expected: reported as foreign, never attributed to a waiter —
/// the pod was launched by a different program.
#[tokio::test(start_paused = true)]
async fn a_foreign_structural_fingerprint_is_reported_not_attributed() {
    let (api, control) = fake_api();
    let observer = PodObserver::spawn(api, selector(), limits());
    let mut waiter = observer
        .watch_attempt(key("extract", 1))
        .await
        .expect("the observer accepts a waiter");
    control.await_watch().await;

    control
        .deliver(WatchDelivery::Modified(pod_with_annotation(
            "extract",
            1,
            PodPhase::Succeeded,
            "140",
            ANNOTATION_STRUCTURAL_FINGERPRINT,
            "sf-someone-elses-build",
        )))
        .await;

    // A pod that cannot be identified at all is also foreign, not a waiter's.
    let id = identity("extract", 1);
    let mut labels = id.labels();
    labels.insert(LABEL_RUN_ID.to_string(), RUN_ID.to_string());
    control
        .deliver(WatchDelivery::Modified(bare_pod(
            "stray",
            "141",
            labels,
            std::collections::BTreeMap::new(),
        )))
        .await;

    // Then a real one, so the test has a synchronisation point that does not
    // depend on the absence of a message.
    control
        .deliver(WatchDelivery::Modified(pod(
            "extract",
            1,
            PodPhase::Running,
            "142",
        )))
        .await;

    let event = waiter.next().await.expect("the genuine pod arrives");
    let WaiterEvent::Observed(observation) = event else {
        panic!("expected an observation, got {event:?}");
    };
    assert_eq!(observation.phase, PodPhase::Running);
    assert!(
        !observation.terminal,
        "the foreign pod's terminal must not have been attributed to this waiter"
    );

    let report = observer
        .shutdown(Duration::from_secs(5))
        .await
        .expect("shutdown completes");
    assert_eq!(report.foreign.len(), 2, "both foreign pods are reported");
    assert!(report.foreign.iter().any(|f| matches!(
        &f.reason,
        ForeignReason::ForeignFingerprint { found, .. } if found == "sf-someone-elses-build"
    )));
    assert!(
        report
            .foreign
            .iter()
            .any(|f| matches!(&f.reason, ForeignReason::Unidentifiable(_)))
    );
}

/// A resync must not *lose* a transition either: a pod deleted while the watch
/// was down is reported once, so a waiter cannot hang on a pod that no longer
/// exists.
#[tokio::test(start_paused = true)]
async fn a_pod_that_vanishes_during_a_gap_is_reported_once() {
    let (api, control) = fake_api();
    control.upsert(pod("extract", 1, PodPhase::Running, "100"));

    let observer = PodObserver::spawn(api, selector(), limits());
    let mut waiter = observer
        .watch_attempt(key("extract", 1))
        .await
        .expect("the observer accepts a waiter");
    control.await_watch().await;

    let first = waiter
        .next()
        .await
        .expect("the initial LIST reports Running");
    assert!(matches!(first, WaiterEvent::Observed(ref o) if !o.vanished));

    control.remove("dagr-extract-1");
    control
        .deliver(WatchDelivery::ApiError {
            code: 410,
            reason: "Expired".to_string(),
            message: "too old resource version: 100 (972)".to_string(),
        })
        .await;

    let event = waiter.next().await.expect("the disappearance is reported");
    let WaiterEvent::Observed(observation) = event else {
        panic!("expected an observation, got {event:?}");
    };
    assert!(observation.vanished);
    assert!(
        tokio::time::timeout(Duration::from_secs(5), waiter.next())
            .await
            .expect("a retired waiter closes rather than blocking")
            .is_none(),
        "reported exactly once"
    );

    observer
        .shutdown(Duration::from_secs(5))
        .await
        .expect("shutdown completes");
}

/// A transport end resumes from the last resourceVersion the observer actually
/// saw — the cheap reconnect point — rather than paying for a full LIST, while a
/// 410 always re-lists. Both halves in one test, because the difference between
/// them is the whole taxonomy.
#[tokio::test(start_paused = true)]
async fn a_transport_end_resumes_from_the_last_seen_version_and_a_410_re_lists() {
    let (api, control) = fake_api();
    control.upsert(pod("extract", 1, PodPhase::Pending, "100"));

    let observer = PodObserver::spawn(api, selector(), limits());
    let _waiter = observer
        .watch_attempt(key("extract", 1))
        .await
        .expect("the observer accepts a waiter");
    control.await_watch().await;

    control
        .deliver(WatchDelivery::Bookmark {
            resource_version: "133".to_string(),
        })
        .await;
    control.end_stream().await;
    control.await_watch().await;

    assert_eq!(control.lists(), 1, "a transport end does not force a LIST");
    assert_eq!(
        control.watch_resource_versions().last().map(String::as_str),
        Some("133"),
        "the bookmarked version is the resume point"
    );

    control
        .deliver(WatchDelivery::ApiError {
            code: 410,
            reason: "Expired".to_string(),
            message: "too old resource version: 133 (972)".to_string(),
        })
        .await;
    control.await_lists(2).await;
    assert_eq!(control.lists(), 2, "a 410 always re-lists");

    observer
        .shutdown(Duration::from_secs(5))
        .await
        .expect("shutdown completes");
}

/// A transient authorization failure during an API-server restart is not fatal:
/// the spike saw two `403`s inside a five-millisecond window while authorization
/// was still loading. An executor that treated one as permanent would abort a run
/// over it.
#[tokio::test(start_paused = true)]
async fn a_transient_403_is_retried_rather_than_treated_as_permanent() {
    let (api, control) = fake_api();
    control.upsert(pod("extract", 1, PodPhase::Pending, "100"));
    control.fail_next_watch(ApiFailure::api(
        403,
        "Forbidden",
        r#"pods is forbidden: User "kubernetes-admin" cannot watch resource "pods""#,
    ));
    control.fail_next_watch(ApiFailure::api(403, "Forbidden", "still loading"));

    let observer = PodObserver::spawn(api, selector(), limits());
    let mut waiter = observer
        .watch_attempt(key("extract", 1))
        .await
        .expect("the observer accepts a waiter");
    let first = waiter
        .next()
        .await
        .expect("the initial LIST reports Pending");
    assert!(matches!(first, WaiterEvent::Observed(_)));

    control.await_watch().await;
    control
        .deliver(WatchDelivery::Modified(pod(
            "extract",
            1,
            PodPhase::Succeeded,
            "140",
        )))
        .await;

    let observation = waiter.terminal().await.expect("the run continues");
    assert_eq!(observation.phase, PodPhase::Succeeded);

    let report = observer
        .shutdown(Duration::from_secs(5))
        .await
        .expect("shutdown completes");
    assert!(
        report.failure.is_none(),
        "a transient 403 is not a run failure"
    );
}
